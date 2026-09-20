# Configuration Reference

---

## 0. Who this page is for

You need to look up a setting: its name, its default, what it does, and what it is called when you
use Tandem without Spring. The other chapters introduce the settings that matter for each task; this
page lists them all in one place.

!!! question "The question this page answers"
    **What can I configure, what is the default, and which side of the deployment reads it?**

Every setting has a default except one, `tandem.kafka.source`. An application that sets nothing else
behaves exactly as this page describes. The page lists every property Tandem binds.

---

## 1. How settings work

**In Spring Boot**, every setting is a property under `tandem.*`, in your `application.yml` or any
other property source, and Tandem binds it with the usual relaxed rules (`bucket-count`, `bucketCount`
and `TANDEM_OUTBOX_BUCKET_COUNT` are the same setting). Durations use Spring's notation: `500ms`,
`30s`, `15m`, `14d`. Your IDE offers completion and help for each key from metadata generated with
the modules. Each module also carries a commented reference file with every key it binds
(`tandem-producer-reference.yml` and `tandem-relay-reference.yml` inside the jars).

**Without Spring**, the same settings are arguments of the builders and constructors. The last column
of each table names the equivalent.

**Which side reads what.** In a split deployment the write side and the relay are separate processes
with separate configuration, so it matters which one reads which namespace.

| Namespace | Write side | Relay | Admin API |
|---|---|---|---|
| `tandem.outbox` | Yes | Yes | |
| `tandem.tracing.enabled`, `tandem.tracing.correlation-id-mdc-key` | Yes | | |
| `tandem.tracing.publish-span` | | Yes | |
| `tandem.relay` | | Yes | |
| `tandem.kafka` | | Yes | |
| `tandem.metrics` | | Yes | |
| `tandem.admin` | | | Yes |

---

## 2. `tandem.outbox`

Read by **both** the write side and the relay. The value must be identical on both.

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.outbox.bucket-count` | `256` | Number of virtual buckets that work is spread over. Stored in every row, so it **must match on both sides and never change** after the first deployment. Valid range 1 to 32,767. Both sides check it at startup and refuse to run on a mismatch. See [Reliability](reliability.md#4-the-bucket-count). | `JdbcOutboxRepository(dataSource, bucketCount)` and `RelayConfig.builder().bucketCount(...)`, plus `BucketCountGuard.check(...)` |
| `tandem.outbox.wakeup` | `none` | A signal from the write side to the relay that a row has been committed: `none` or `pg-notify`. With `none` the relay finds rows by polling alone. With `pg-notify` each insert call sends one PostgreSQL notification, inside your transaction, naming the buckets it wrote, and the relay holds one connection listening for it, so a row written into a bucket that has gone quiet is claimed at once instead of waiting for the next poll. **Both sides must use the same value**; a mismatch only costs the shortened wait, never delivery. PostgreSQL only, and a connection pooler in transaction pooling mode breaks the listening connection, in which case the relay carries on by polling. | Write side: `Wakeup.PG_NOTIFY` as the last argument of the `JdbcOutboxRepository` constructor. Relay: `WakeupSource.forWakeup(Wakeup.PG_NOTIFY, dataSource)` passed to the `WorkerPool` constructor |

---

## 3. `tandem.relay`

Read by the relay only. Every property is optional.

### 3.1 Whether and how the relay runs

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.relay.enabled` | `true` | Whether this application runs a relay. `false` loads the module without starting one, so one image can play either role. | Do not start the `WorkerPool` |
| `tandem.relay.coordination` | `SINGLE` | `SINGLE` for exactly one relay instance, `LEASE` for several sharing the buckets through the database. See [Reliability](reliability.md#33-single-and-lease). | `RelayConfig.builder().coordination(...)` |
| `tandem.relay.instance-id` | derived (`tandem-<host>-<pid>-<random>`) | The unique name of this instance under `LEASE`, at most 64 characters. Set it for a stable name across restarts. | `.instanceId(...)` |
| `tandem.relay.bucket-lease` | `30s` | How long a bucket claim under `LEASE` lasts unless renewed. A dead instance's buckets are taken over once it expires. | `.bucketLease(...)` |

### 3.2 Delivery and retries

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.relay.max-attempts` | `10` | Delivery attempts before a row becomes `FAILED`. Each failed attempt waits out a backoff before the next. See [Reliability](reliability.md#21-one-delivery-step-by-step). | `.maxAttempts(...)` |
| `tandem.relay.row-lease` | `60s` | How long a row being sent stays claimed. **Must be longer than the Kafka producer's `delivery.timeout.ms`**, checked at startup (twice as long is recommended). | `.rowLease(...)` |
| `tandem.relay.reclaim-interval` | `5s` | How often rows whose lease expired are taken back, and how often each instance re-reads the pause state. An Admin API pause takes effect within about this long. | `.reclaimInterval(...)` |

### 3.3 Housekeeping

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.relay.retention` | `14d` | How long `DONE` and `DISCARDED` rows are kept before cleanup, counted from when they were created. `FAILED` rows are never removed. | `.retention(...)` |
| `tandem.relay.cleanup-interval` | `15m` | How often cleanup runs. | `.cleanupInterval(...)` |
| `tandem.relay.cleanup-batch-size` | `1000` | Rows deleted per cleanup pass. | `.cleanupBatchSize(...)` |

### 3.4 Reporting and checks

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.relay.metrics-interval` | `10s` | How often the relay reads and reports the backlog, its age and the live workers. | `.metricsInterval(...)` |
| `tandem.relay.log-every-rows` | `10000` | Each worker logs one `INFO` progress line every this many delivery outcomes. | `.logEveryRows(...)` |
| `tandem.relay.order-violation-detection` | `true` | Whether the relay watches the order in which it publishes each aggregate and reports a write side that broke the ordering rule. Turn it off only where writers to one aggregate are serialized by construction. See [Advanced Integration](advanced-integration.md#7-replacing-a-default). | `.orderViolationDetection(...)` |

### 3.5 Polling and concurrency

How the relay looks for work and how much it takes at a time. The defaults suit a first deployment.

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.relay.workers-per-instance` | twice the number of available processors | Worker threads in this instance. The buckets the instance owns are split across them. | `.workersPerInstance(...)` |
| `tandem.relay.poll-interval` | `100ms` | The ceiling of the wait between polls while the outbox is idle. The relay polls quickly while rows are flowing and backs off toward this value when a claim finds nothing, so it is both what the first row after a quiet stretch can wait and what bounds the idle query rate. | `.pollInterval(...)` |
| `tandem.relay.poll-interval-floor` | `10ms` | Where that wait restarts after a poll that found rows, so what a row of a live stream waits. Set it equal to `poll-interval` for a fixed interval. | `.pollIntervalFloor(...)` |
| `tandem.relay.poll-backoff-factor` | `2.0` | How fast the wait climbs from the floor to `poll-interval`, one step per empty poll. Must be greater than 1. | `.pollBackoffFactor(...)` |
| `tandem.relay.batch-size` | `100` | How many rows a worker claims and keeps in flight at once for its share of the buckets. | `.batchSize(...)` |

---

## 4. `tandem.kafka`

Read by the relay, when it publishes to Kafka.

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.kafka.source` | **none, required** | The CloudEvents `source` of every event: a URI naming your application, for example `/orders/service`. The application refuses to start without it. | `KafkaRelayConfig.of(source)` |
| `tandem.kafka.default-content-type` | `application/json` | The `datacontenttype` for a row that stored none. | `KafkaRelayConfig` constructor |
| `tandem.kafka.default-data-schema` | unset | A default `dataschema` URI. Left out of the envelope when unset. | `KafkaRelayConfig` constructor |
| `tandem.kafka.topic-suffix` | `-topic` | Appended to the kebab-cased aggregate type to name the topic (`OrderLine` becomes `order-line-topic`). | `TopicRouter.kebabWithSuffix(...)` |
| `tandem.kafka.producer.*` | empty | Raw Kafka producer properties, passed through: `bootstrap.servers`, security, compression. Tandem protects the settings that guard delivery and refuses an unsafe override at startup. See [Advanced Integration](advanced-integration.md#1-the-kafka-producer). | The `Map` given to `KafkaRelay` |

---

## 5. `tandem.metrics`

Read by the relay, and only when `tandem-micrometer` and a Micrometer `MeterRegistry` are present.

| Property | Default | Meaning | Plain Java |
|---|---|---|---|
| `tandem.metrics.max-publish-latency` | `5m` | The ceiling of the publish latency histogram. A longer wait is still counted, but it cannot be placed precisely. | `MicrometerTandemMetrics(registry, maximum)` |

---

## 6. `tandem.tracing`

Off by default and never switched on merely because a tracing library is present. Read by different
sides. See [Observability](observability.md#4-distributed-traces).

| Property | Read by | Default | Meaning | Plain Java |
|---|---|---|---|---|
| `tandem.tracing.enabled` | Write side | `false` | Capture the trace context and the correlation id when an event is written. | Pass a `TracePropagator` to `JdbcOutboxRepository` |
| `tandem.tracing.correlation-id-mdc-key` | Write side | `correlationId` | The logging MDC key the correlation id is read from. | `TandemContext.setCorrelationId(...)` |
| `tandem.tracing.publish-span` | Relay | `false` | Emit a `tandem.relay.publish` span per published event. Builds on the write side's capture. | Pass a `TandemSpanRecorder` to `KafkaRelay` |

---

## 7. `tandem.admin`

Read by the Admin API, provided by `tandem-admin`. See [Admin API](admin-api.md#12-turning-it-on).

| Property | Default | Meaning |
|---|---|---|
| `tandem.admin.enabled` | `false` | Whether the application exposes the Admin API. It changes delivery state, so it is off unless you turn it on, and securing it is your application's job. |
| `tandem.admin.base-path` | `/tandem/admin` | The path before the fixed `/v1` version segment. |

---

## 8. Settings that depend on each other

Three relationships are checked for you at startup, and a violation stops the application with a
message that says what to change.

| Relationship | Why |
|---|---|
| Write side `bucket-count` equals relay `bucket-count`, and never changes | Otherwise events land in buckets no worker owns. |
| `tandem.relay.row-lease` is greater than the producer's `delivery.timeout.ms` | Otherwise a lease can expire while its send is still in progress and the event is delivered twice. |
| The Kafka producer keeps `enable.idempotence` on, `acks` at `all`, and `max.in.flight.requests.per.connection` at 5 or less | Delivery without loss and without reordering rests on them. |

Two more are yours to keep consistent. Every concurrent relay instance needs its own
`instance-id` under `LEASE`, and every relay instance sharing a database must run under `LEASE`
rather than `SINGLE` (see [Reliability](reliability.md#33-single-and-lease)).
