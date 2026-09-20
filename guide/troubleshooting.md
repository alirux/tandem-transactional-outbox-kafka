# Troubleshooting

---

## 0. Who this page is for

Something is not working and you want the shortest path from **what you see** to **what to do**. The
sections below are organized by symptom. Each cause says how to recognize it, so you can rule it in or
out before changing anything.

!!! question "The question this page answers"
    **Events are not arriving, the application will not start, or the data looks wrong: what is the
    cause, and how do I fix it?**

The tools it uses are the ones from the other chapters: the [CLI](cli.md) (or the
[Admin API](admin-api.md)), the relay's [logs](observability.md#2-logs), and its
[metrics](observability.md#3-metrics).

---

## 1. The first two minutes

Before reading a specific section, take three readings. They separate most problems from each other.

```bash
tandem-cli relay status            # is a relay alive? paused?
tandem-cli outbox summary          # what is waiting, what failed, how old
tandem-cli outbox search --status FAILED --limit 5
```

| What you see | Go to |
|---|---|
| `state: DOWN`, or no relay log line since start | [§2.1](#21-the-relay-is-not-running) |
| `state: PAUSED` | [§2.2](#22-the-relay-is-paused) |
| `FAILED` above zero, or `blocked` above zero | [§3](#3-a-row-is-failed-or-an-aggregate-is-stuck) |
| `PENDING` growing with a healthy `RUNNING` relay | [§2.3](#23-the-relay-runs-but-rows-stay-pending) |
| The application did not start | [§4](#4-the-application-does-not-start) |
| Writing an event throws, or seems to do nothing | [§5](#5-writing-events) |
| The outbox is fine but a consumer disagrees | [§6](#6-what-consumers-see) |

---

## 2. Events are not reaching Kafka

Rows sit in `PENDING` and nothing is published.

### 2.1 The relay is not running

**Recognize it.** `relay status` reports `DOWN`, and the application log has no `Starting relay` line.

| Cause | Fix |
|---|---|
| The relay role was never deployed, or the process is down. | Start it. In a split deployment the relay is its own application. |
| `tandem.relay.enabled` is `false` in the application that should host it. | Set it to `true`, or run the relay elsewhere. |
| **Spring did not find exactly one `DataSource`.** Tandem's write and relay configuration only load when the application has a single `DataSource` (or a primary one). With two and no primary, it silently does nothing and there is no error. | Mark the one Tandem should use as `@Primary`. |
| `tandem-spring-relay` is on the classpath but `tandem-kafka` is not. | Add `tandem-kafka`; the startup message names it (see §4). |

### 2.2 The relay is paused

**Recognize it.** `relay status` reports `PAUSED`, or `relay buckets` shows `paused: true` for some
buckets. Someone paused it through the Admin API or the CLI.

**Fix.** `tandem-cli relay resume` (add `--bucket <n>` for a single bucket). The change reaches every
instance within a few seconds.

### 2.3 The relay runs but rows stay pending

`relay status` says `RUNNING`, yet the backlog grows or its age climbs. Go through these in order.

| Cause | How to recognize it | Fix |
|---|---|---|
| **Kafka is unreachable, or the topic does not exist.** | The relay logs `Outbox row dispatch failed, retrying` for the same `rowId`, and the exception is a timeout, often naming the topic. `attempts` on the row climbs. | Fix connectivity or create the topic (Tandem does not create topics). The retries continue on their own. |
| **The rows are waiting out a backoff.** | Rows have `attempts` above zero and a `nextAttemptAt` in the future. | Nothing: they retry on schedule. If it lasts, see the row above. |
| **The rows are behind a `FAILED` row of the same aggregate.** | `blocked` above zero in `outbox summary`; a `FAILED` row exists for that aggregate. | See [§3](#3-a-row-is-failed-or-an-aggregate-is-stuck). |
| **Buckets have no owner (`LEASE`).** | `tandem-cli relay buckets --uncovered-only` lists buckets, and `uncoveredBuckets` above zero in `relay status`. | An instance died or none is running: start one. For a zombie owner with a stale heartbeat, `relay release-bucket <n>`. |
| **The write side and the relay use a different bucket count and nothing checked.** | Rows accumulate silently in buckets no worker owns; the relay is healthy and idle. | Set the same value on both. Spring checks this at startup; in plain Java call `BucketCountGuard.check(...)` on both sides. |
| **The relay reads a different database from the writers.** | The relay's outbox is empty while the application's is not. | Point both at the same schema. |
| **The rows are not committed yet.** | The row is not visible at all, only after the writing transaction commits. | Nothing: a long transaction delays its own events. |

If none of these fits, raise the log level of `com.codingful.tandem.jdbc` to `DEBUG` for a few
minutes; it shows each claim cycle and what it found.

---

## 3. A row is `FAILED` or an aggregate is stuck

A `FAILED` row blocks its own aggregate, and every event behind it waits as `PENDING`. Nothing else
is affected.

**Find the cause.**

```bash
tandem-cli outbox search --status FAILED
tandem-cli outbox get <id>            # read lastError
```

The relay's `ERROR` line for the row (`Outbox row dispatch failed permanently`) carries the same
`rowId` and the full exception.

| `lastError` says | Cause | Fix |
|---|---|---|
| A record too large error | The event is bigger than the broker accepts. | Raise the broker and topic limit, or shrink the payload; then replay. |
| An authorization or authentication error | The Kafka credentials or ACLs do not allow this topic. | Fix them; then replay. |
| An invalid topic error | The router produced a name Kafka rejects. | Fix the router or the suffix; then replay. |
| A failure to encode the message | The event cannot be built into the envelope. | Fix the row's data or the encoder. It fails the same way every time. |
| A timeout, repeated until the attempts ran out | Kafka was unavailable for longer than the retry ladder covers (about ten minutes with the defaults). | Once Kafka is back, replay every failed row in one go (below). |

**Recover.**

```bash
tandem-cli outbox replay-bulk --status FAILED --dry-run     # count first
tandem-cli outbox replay-bulk --status FAILED --yes
```

The rows go back to `PENDING` with a fresh set of attempts, and the relay delivers them in their
original order. Discard a row only if that event can never be delivered and its consumers can live
without it: it skips the event for good. Details in
[Reliability](reliability.md#23-kafka-unavailable-for-a-while).

---

## 4. The application does not start

Tandem checks its configuration when it starts and stops with a message that names the problem. These
are the messages, with what to do.

| Message begins | Cause | Fix |
|---|---|---|
| `Missing required configuration: tandem.kafka.source` | The one setting without a default. | Set it to a URI naming your application, for example `/orders/service`. |
| `The relay has no OutboxDispatcher` | Nothing to publish with: `tandem-kafka` is not on the classpath and you supplied no dispatcher. | Add `tandem-kafka`. |
| `Unsafe Kafka producer config: ...` | You set `enable.idempotence`, `acks` or `max.in.flight.requests.per.connection` to a value that would allow loss or reordering. | Remove the override. |
| `Unsafe relay config: rowLease (=...) must be > Kafka producer delivery.timeout.ms` | The row lease is not longer than the producer's delivery timeout. | Raise `tandem.relay.row-lease`, or lower the producer's `delivery.timeout.ms`. The message recommends a value. |
| `Bucket count mismatch: this process is configured with bucketCount=... but the database was first initialised with bucketCount=...` | The two sides disagree. | Align the side that reports the wrong value. The message says which. |
| `Reading tandem_meta bucket_count failed`, caused by `relation "tandem_meta" does not exist` | The schema has not been applied to this database. | Apply it: see [Getting Started](getting-started.md#4-step-2-create-the-schema). |
| `Unsafe relay config: the tandem_relay_member table is missing or unreadable ...` | `LEASE` is on but its tables are absent or the bucket count does not match. | Apply the baseline schema (it creates the lease tables) and check the bucket count, or run `SINGLE`. |
| `Two OutboxEventMappers are registered for the same event type` | Two mapper beans for one event class. | Keep one. |

If the application starts but the relay does not, and there is no error, go back to
[§2.1](#21-the-relay-is-not-running): the usual cause is more than one `DataSource`.

---

## 5. Writing events

### 5.1 It fails with an exception

| You see | Cause | Fix |
|---|---|---|
| `IllegalStateException: no seq mode stated ...` | The message states none of `seq(long)`, `managedSeq()` and `unsequenced()`. | State one; `unsequenced()` is the usual answer. |
| `IllegalArgumentException: aggregateId exceeds 255 chars` | The aggregate id is too long. | Use a shorter identifier. |
| `OutboxInsertException: failed to insert outbox row ...`, caused by `relation "tandem_outbox" does not exist` | The schema has not been applied. | Apply it. |
| `OutboxInsertException: failed to insert outbox row ...`, cause with SQLSTATE `22P02` | The payload is not valid JSON. | The column stores JSON: send JSON bytes. |
| `DuplicateSeqException: duplicate (aggregate_id, seq) = ...` | Two events of one aggregate carry the same `seq`. With an ORM the usual cause is reading the version before it is flushed. | Flush before building the message, or use `unsequenced()`. See [Adoption Guide](adoption.md#1-the-one-precondition-that-can-actually-block-you). |
| `OutboxInsertException: A Tandem outbox event was published outside an active transaction` | A Spring application event was published with no transaction open. | Publish it from inside a `@Transactional` method. |
| `OutboxInsertException: Extracted outbox message does not match the @TransactionalOutbox declaration` | The annotation names an `aggregateType` and a message carries another. | Align them, or drop the attribute. |

### 5.2 It does nothing, without an error

These are the quiet ones, so they deserve a check whenever an expected event never appears.

| Symptom | Cause | Fix |
|---|---|---|
| No row is written by a `@TransactionalOutbox` method. | The annotation's processing is only registered when AspectJ is on the classpath. | Add your Boot version's AOP starter. |
| No row is written for a published Spring event. | An event type is opted in by registering an `OutboxEventMapper`; without one it is ignored, by design. | Register a mapper for the type. |
| The row exists even though the business change rolled back. | The insert ran on a **different connection** from your transaction (plain Java with a pool `DataSource`, so it committed on its own). | Give the repository a `DataSource` that joins your transaction; see [Getting Started](getting-started.md#52-insert-it-inside-your-transaction). Test it as in [Testing](testing-and-upgrades.md#15-the-tests-only-you-can-write). |
| The event never appears although the method ran. | The transaction rolled back, or is still open. Rows exist only after commit. | Check the transaction outcome. |

---

## 6. What consumers see

### 6.1 The same event twice

Expected, and not a fault: delivery is at least once. A relay that stops between the Kafka
acknowledgement and recording it, a lease that expired mid-send, or a replay all publish an event
again. Make the consumer idempotent; see [Consuming Events](consuming-events.md). If duplicates are
frequent rather than occasional, look at the relay: `lease_expired.count` growing means workers are
dying mid-send, and `Relay worker died; restarting` in the log says why.

### 6.2 Events out of order

Events of one aggregate must arrive in the order they were written. If they do not:

| Cause | How to recognize it |
|---|---|
| **Concurrent writers to one aggregate are not serialized.** | The relay logs `Published an aggregate's events out of insert order` at `ERROR`, and `order_violation.count` increases. This is a write side defect Tandem cannot repair. See the [precondition](adoption.md#1-the-one-precondition-that-can-actually-block-you). |
| **More than one relay without `LEASE`.** | Several instances run against one database under `SINGLE`. Ordering on the row still holds, but the detector above goes blind. Switch to `LEASE`. |
| **The topic gained partitions.** | After adding partitions, an aggregate's key can map to a different partition than before, so old and new events are read from two partitions. |
| **A consumer processes one partition's messages in parallel.** | Order is lost after Kafka, not before it. |

A quiet detector does not prove a correct write side: it only reports what it can see.

### 6.3 An event is missing

Trace it from the outbox outward. `tandem-cli outbox search --aggregate-id <id>` shows the row.

| Row status | Meaning |
|---|---|
| Not there | It was never written: see [§5.2](#52-it-does-nothing-without-an-error). |
| `PENDING` or `IN_FLIGHT` | Not delivered yet: see [§2](#2-events-are-not-reaching-kafka). |
| `FAILED` | Blocked: see [§3](#3-a-row-is-failed-or-an-aggregate-is-stuck). |
| `DISCARDED` | An operator gave up on it on purpose. |
| `DONE` | Kafka acknowledged it. Look at the consumer side: the topic name (the aggregate type is kebab-cased and suffixed), the consumer group's offsets, and the topic's retention. |

### 6.4 The payload is not byte for byte what was written

The outbox stores the payload as `jsonb`, so what is published is **semantically identical, not
identical bytes**: key order, whitespace and number formatting can change. Do not sign or hash the
payload you insert, and do not compare it as a string.

---

## 7. The Admin API and the CLI

| Symptom | Cause | Fix |
|---|---|---|
| Every call answers `404` | The API is off (`tandem.admin.enabled` is not `true`), the module or a web starter is missing, or the URL lacks the prefix. | Enable it; use the full `.../tandem/admin/v1` URL, or your configured base path. |
| `401` | Your application's security rejected the credentials. Tandem checks none itself. | Fix the credentials. The CLI exits with `3`. |
| `409` with `relay-coordination-unsupported` on buckets, workers or a per-bucket pause | The relay runs under `SINGLE`, which has no per-bucket ownership. Not an error and not transient. | Use `relay status` and whole-relay `pause`, which work under both modes. The CLI exits with `6`. |
| `409` with `message-not-replayable` or `message-not-discardable` | The row is in a status that does not allow the action. | Check its status first. |
| The CLI exits with `8` | It could not reach the API at all: wrong URL, network, TLS, or the timeout. | Check `--base-url`, `--ca-cert` for a private CA, and `--timeout`. |
| The CLI exits with `2` | A usage error, most often a missing base URL. | Set `TANDEM_ADMIN_URL` or pass `--base-url`. |
| `relay status` says `DOWN` but the application is up | No relay instance has reported in for a few seconds: the relay is not running, or it is blocked. | See [§2.1](#21-the-relay-is-not-running). |

---

## 8. Metrics, logs and traces are missing

| Symptom | Cause | Fix |
|---|---|---|
| No `tandem_*` meters | `tandem-micrometer` is not on the relay's classpath, or there is no `MeterRegistry` bean, or you are looking at the write side (metrics come from the relay). | Add the module where the relay runs, and check its registry is exported. |
| A lag series stops updating | The relay is down: it is the one that reads them. | Alert on the series being absent; see [Observability](observability.md#34-what-the-metrics-cannot-tell-you). |
| No Tandem lines in the log outside Spring Boot | `System.Logger` is not bridged to your backend. | Add `org.slf4j:slf4j-jdk-platform-logging`, and only one such bridge. |
| No trace for an event | Sampling dropped it (frozen when the event was written), or `tandem.tracing.enabled` is off, or the propagation format is not one the relay can read. | See [Observability](observability.md#43-turning-them-on). |
| No `tandem.relay.publish` span | `tandem.tracing.publish-span` is off, or the row carries no trace context. | Enable it on the relay, and capture on the write side. |

---

## 9. When to ask for help

If you have gone through the relevant section and it is still unexplained, gather these before opening
an issue, because they answer the first questions asked:

- the Tandem version, and your Spring Boot and Kafka client versions;
- whether you run embedded or split, and under `SINGLE` or `LEASE`;
- `tandem-cli relay status` and `tandem-cli outbox summary`;
- for a specific event, `tandem-cli outbox get <id>`;
- the relay's log lines around the problem, at `INFO` or `DEBUG` (they carry identifiers, not
  payloads, so they are safe to share).
