# Tandem — `tandem-test` LLD (minimal)

**Version:** 0.1 (Draft)  
**Module:** `tandem-test` · package `com.codingful.tandem.test`  
**Depends on:** `tandem-core` (+ `tandem-jdbc`/`tandem-kafka` for the container helper); Testcontainers
+ JUnit 6 + AssertJ (test-scope). **Resolves:** Q26 (minimal scope for the basic round).

Test utilities supporting the **no-mocks / Detroit-school** approach (AGENTS.md): real in-memory
collaborators for unit tests, and a Testcontainers helper for integration tests. Just enough to test
the basic round (write-side + relay + end-to-end); richer helpers come later.

---

## 1. `InMemoryOutbox` — unit tests without a database

A faithful in-memory implementation of **both** `OutboxRepository` (write-side) and `OutboxStore`
(relay-side) — a real collaborator, not a mock. Backed by a thread-safe map of `OutboxRecord`.

- `insert` / `insertAll`: assign `id` (atomic counter), compute `bucket` via the **same core
  `BucketHash.bucketFor`** the JDBC adapter uses (LLD-core §4) so in-memory and real-DB buckets match,
  store as `PENDING`; enforce `UNIQUE(aggregate_id, seq)` → `DuplicateSeqException`. All three `seq`
  modes behave as they do in the database ([HLD-managed-seq](HLD-managed-seq.md) §4.5). A
  `managedSeq()` message is numbered from an **outbox-wide counter**, standing in for the `tandem_seq`
  sequence — one counter for the whole outbox, not one per aggregate, so the numbers are sparse per
  aggregate exactly as the database's are. An `unsequenced()` message stores no number and stays
  **outside** the uniqueness check, mirroring PostgreSQL treating NULLs as distinct; its row view
  reports `seq` as `null`, never `0`. `lockedWrite()` (`HLD-managed-seq` §4.2) needs no mechanism here: every insert
  already serialises under one internal lock, stricter than the advisory lock asks for — the flag is
  carried through onto the stored message rather than acted on.
- `claimBatch(buckets, worker, lease, n)`: return the **head of each aggregate's pending chain** in the
  given buckets (earliest not-DONE row, eligible by `next_attempt_at`), mark `IN_FLIGHT` — mirrors the
  SQL semantics of LLD-jdbc §3.3.
- `markDone` / `markForRetry` / `markFailed` / `reclaimExpiredLeases` / `cleanup`: mutate the map.
- Test affordances: inspect rows by status, advance a controllable clock (to test backoff/lease
  expiry deterministically).

This lets unit tests exercise the write-side and the relay loop with zero I/O.

## 2. `RecordingDispatcher` — in-memory `OutboxDispatcher`

An in-memory `OutboxDispatcher` that **records** the dispatched records (the "published" events) for
assertions, and can be told to **fail** a given record retriably or permanently — so the relay's
retry/backoff and fail-fast paths (LLD-kafka §4) are testable without Kafka. A real collaborator, not
a mock. Its `dispatch` returns a `CompletableFuture<Void>` matching the port (LLD-core §2.3):
already-completed for recorded successes, completed-exceptionally with the chosen retriable/permanent
`OutboxDispatchException` for forced failures — and a controllable variant that completes futures on
demand, so the relay's **overlapping in-flight dispatch** (LLD-jdbc §3.4) is exercised as in production.

## 3. `RecordingMetrics` — enabled `TandemMetrics`

An **enabled** `TandemMetrics` that keeps what it was told: counters accumulate, gauges keep their
latest reading. Being enabled is the point — every metric call in the relay sits behind
`isEnabled()`, and the lag reading is not even scheduled without an adapter (LLD-jdbc §4), so a test
running on the no-op default exercises none of that code. A real collaborator, not a mock; also
usable by an adopter to check their own adapter is reached.

## 4. `TandemTestContainer` — integration tests

A Testcontainers helper (tagged `@Tag("integration")`) that:
- starts a real **PostgreSQL** and a real **Kafka** (KRaft) container,
- applies the committed **baseline DDL** ([`schema/postgres/tandem-baseline.sql`](../schema/postgres/tandem-baseline.sql), generated from the changelog; LLD-jdbc §1/§6) — so every integration test also proves the generated schema applies to a real PostgreSQL,
- exposes the `DataSource` + bootstrap servers and convenience factories for `JdbcOutboxRepository`,
  the `WorkerPool` relay, and a Kafka consumer,

so an end-to-end test can: insert in a transaction → run the relay → assert the CloudEvent landed on
the topic in per-aggregate order.

`newRelay(RelayConfig, TopicRouter, KafkaRelayConfig)` publishes the default CloudEvents envelope;
`newRelay(RelayConfig, KafkaMessageEncoder)` publishes through the given encoder instead (a neutral
`MessageEncoder` lifted with `KafkaMessageEncoder.from`). The first delegates to the second with a
`CloudEventEncoder`, so both share the bucket-count guard, the store and the dispatcher's lifecycle.
`EndToEndIT` runs the application-encoder path with the worked example of §6.

The PostgreSQL image is `postgres:16-alpine` by default and comes from
`TandemTestContainer.postgresImage()` (the `tandem.test.postgres.image` system property, else the
`TANDEM_TEST_POSTGRES_IMAGE` environment variable). `tandem-jdbc`'s `AbstractPostgresIT` reads the
same method, so one run moves both suites onto another major: that is what the `postgres-majors`
workflow drives, and it is the only thing separating a CI-verified row of the compatibility matrix
([guide/compatibility.md](../guide/compatibility.md)) from an assumption.

The image is also declared an **input of every `Test` task** (root `build.gradle.kts`), and that line
is load-bearing rather than tidy: `org.gradle.caching` is on and `Test` is a cacheable task, so
without the image in the cache key a run against one major would be served the outputs cached from a
run against another. Every cell of the matrix would then go green having started no container at all,
which is the exact failure the verification exists to rule out. With it in the key a repeated run on
the same image is still `UP-TO-DATE`, so the guard costs nothing.

## 5. Scope (minimal, for the basic round)

In: the four helpers above, enough to unit-test write-side + relay loop and integration-test the
full path, and the encoder contract kit of §6. Out (later): MySQL container variants, causal-ordering/admin test fixtures,
property-based/fuzz harnesses.

## 6. `MessageEncoderContract`: the encoder contract kit

A check an application's encoder author runs in their own test suite, holding any `MessageEncoder` to
the invariants of HLD-alternative-envelope §3 (LLD-alternative-envelope §3). It lives in `main`, next to
`InMemoryOutbox`, because it is for application authors and must be in the published jar.

```java
MessageEncoderContract.of(encoder, MessageEncoderContract.header(RawHeaders.ID))
        .consumesHeaders(RawHeaders.ID, RawHeaders.TYPE)   // row headers the envelope turns into attributes
        .declaresOwnOrderingKey("reason")                  // only when the key is deliberately not aggregate_id
        .records(rows)                                     // optional; replaces the built-in fixtures
        .verify();                                         // one AssertionError listing every violation
```

**Framework-neutral:** it throws `AssertionError` and depends on nothing beyond `tandem-core`, so it puts
no test framework on the application's classpath. The event id extractor is mandatory, since only the
envelope knows where it keeps its id.

| # | Check | Fails when |
|---|---|---|
| 1 | The encoder accepts every fixture | `encode` throws or returns `null` |
| 2 | Two encodes of one row are equal | destination, key, body or a header value differs |
| 3 | The event id is present and stable | the extractor returns `null`, blank, or two different values |
| 4 | Distinct rows have distinct event ids | two rows with different ids yield one event id |
| 5 | The ordering key is the aggregate id | `key` differs from `aggregate_id`, unless declared |
| 6 | Row headers reach the wire | a header not declared consumed is missing, or its value is not the row value's UTF-8 bytes |

A violation names the check, the row id and the offending field names, never a payload or a header
value (AGENTS.md, Logging §5). Not checked: purity beyond determinism, and the permanence of an
encoding failure, a dispatcher property the relay tests pin.

**Built-in fixtures:** three rows of one aggregate with distinct ids, built as the store builds a row
(content type as both the typed field and the `content-type` header), each carrying `content-type`,
`dataschema`, `traceparent`, `tracestate`, `correlation-id` and an application header: one sequenced and
typed, one unsequenced, one untyped. The last two are the shapes a naive encoder breaks on.

**Transport-bound encoders** (`KafkaMessageEncoder`, `RabbitMessageEncoder`) are run by adapting their
output to an `EncodedMessage` in the calling test; the kit has no overload per transport. The built-in
envelopes run against it: `RawMessageEncoder` here, `CloudEventEncoder` in `tandem-kafka` and
`CloudEventAmqpEncoder` in `tandem-rabbitmq` (which declares its own ordering key, since AMQP carries
none).

**Worked example:** `DebeziumStyleEncoder` (`com.codingful.tandem.test.encoder`) lives in the
**`testFixtures`** source set, which is never published: an illustration of an application's own
envelope, verified here and walked through in `guide/message-format.md`, not a supported artifact. It
drives the custom-encoder end-to-end tests on both transports (`EndToEndIT`, `RabbitRelayIT`).

A new check can turn an application's green encoder test red on upgrade, so a release that adds one is
listed under **Breaking** (LLD-alternative-envelope §8).
