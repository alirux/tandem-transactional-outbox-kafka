# Tandem — `tandem-kafka` LLD

**Version:** 0.1 (Draft)  
**Module:** `tandem-kafka` · package `com.codingful.tandem.kafka`  
**Depends on:** `tandem-core`, `tandem-cloudevents` (the envelope, §3),
`org.apache.kafka:kafka-clients`, `io.cloudevents:cloudevents-kafka`. **Relay-side only**, never on
the client write-side (§3.2/§1.3).  
**Resolves:** Q17 (producer failure semantics), Q18 (TopicRouter default), Q19 (CloudEvents
binding), Q20 (null `type`). See [open-questions-lld.md](open-questions-lld.md).

`tandem-kafka` is the publish adapter: it implements `OutboxDispatcher` (bind the CloudEvent that
`tandem-cloudevents` built for an `OutboxRecord` to a Kafka record, send it, complete a future on the
ack) and the default `TopicRouter`. The relay engine (`tandem-jdbc`) calls it asynchronously and
overlaps `batch_size` records of distinct aggregates in flight; per-aggregate order is structural
(one head per aggregate, HLD §6, Q9/Q10).

---

## 1. Producer configuration (the mandated safe config — §4.4/§4.5)

Tandem sets these defaults and **fails fast** (`TandemConfigurationException`) if the user overrides
them to unsafe values — they protect the no-loss + ordering guarantees:

| Property | Value | Why |
|---|---|---|
| `enable.idempotence` | `true` | no duplicate/reordered batches from the producer's own retries (§4.4) |
| `acks` | `all` | the row is marked DONE only after a durable ack (§4.5); `0`/`1` risk loss → rejected |
| `max.in.flight.requests.per.connection` | ≤ 5 | required for idempotent ordering; >5 rejected |
| `retries` | high (default `Integer.MAX_VALUE`) | transient errors are retried by the producer, bounded by ↓ |
| `delivery.timeout.ms` | **30s** (Tandem's default, below Kafka's own 2 min) | caps the producer's retry window before a send fails; **must stay below `rowLease`** so a row's lease cannot expire mid-send — relay fail-fasts if `rowLease ≤ delivery.timeout.ms` (LLD-jdbc §3.5) |

Everything else (bootstrap servers, security, compression, batching) is user-supplied and passed
through.

**The filled-in `delivery.timeout.ms` grows with the floor Kafka enforces.** Kafka rejects a producer
whose `delivery.timeout.ms < linger.ms + request.timeout.ms`, and Tandem's 30 s default sits exactly on
that floor when `linger.ms` is 0 and `request.timeout.ms` is 30 s. Filling it in blindly therefore
breaks producer construction — with a raw Kafka `ConfigException` — as soon as anything raises the
floor: an ordinary `linger.ms` (the batching knob explicitly "passed through" above), **or simply a
newer client**. Kafka 4 moved the default `linger.ms` from 0 to 5 ms, which is enough on its own: a
consumer on `kafka-clients` 4.x could not construct a producer at all with Tandem's default.

So when the user set no `delivery.timeout.ms`, Tandem fills in `max(30s, linger.ms +
request.timeout.ms)`, taking those two values from the user's config or, failing that, from
**Kafka's own declared defaults** (`ProducerConfig.configDef()`) rather than from copies of them —
copying "linger defaults to 0" into Tandem is what made this break on a client upgrade. An explicit
`delivery.timeout.ms` is always left untouched.

The relay reads the **effective** value for the `rowLease` invariant (§2), so a producer that outgrows
the row lease still fails loudly at startup, with the diagnostic of LLD-jdbc §3.5 that names the fix —
never silently.

Within an aggregate the relay keeps **only the head row in flight** and never sends the
next before its ack (§6/E6); **across the distinct aggregates of a claimed batch it overlaps sends
on the single async producer** — up to `batch_size` records in flight, the per-shard concurrency
window. That overlap (not one-at-a-time publishing) is where per-shard throughput comes from
(LLD-jdbc §3.4, HLD §10).

---

## 2. `OutboxDispatcher` implementation (`KafkaRelay`)

```java
CompletableFuture<Void> dispatch(OutboxRecord record) {
    ProducerRecord<…> pr = encode(record);          // §3: CloudEvent → ProducerRecord
    CompletableFuture<Void> ack = new CompletableFuture<>();
    producer.send(pr, (md, ex) -> {                 // async; callback on the producer I/O thread
        if (ex == null) ack.complete(null);         // broker ack (acks=all)
        else            ack.completeExceptionally(classify(ex));   // §4: retriable vs permanent
    });
    return ack;
}
```
- **Asynchronous send** (`send` + callback → future) — does not block a thread on the ack, so the
  relay overlaps `batch_size` records of distinct aggregates on one producer (the per-shard
  concurrency window, §1). The mark-DONE-after-ack flow (Q9) is preserved: the relay marks DONE in
  the future's completion handler.
- The future completes **exceptionally** with `OutboxDispatchException`, which **carries the
  retriable/permanent verdict** so `tandem-jdbc` routes it to `markForRetry` (backoff) or
  `markFailed` (§4) and stops that aggregate (Q10).
- **No in-aggregate reordering to guard here:** the relay only ever dispatches one row per aggregate
  (its head, LLD-jdbc §3.3), so the producer's per-partition ordering + idempotence are sufficient.

---

## 3. CloudEvents binding (Q19)

**The format is a port, and CloudEvents is its default.** `KafkaRelay` publishes whatever a
`KafkaMessageEncoder` gives it (`CloudEventEncoder` unless the caller supplies another), so changing
the envelope costs an encoder rather than a fork of the dispatcher, and routing plus the CloudEvents
settings become that encoder's concern rather than the relay's.

`KafkaMessageEncoder` writes a `ProducerRecord` directly, which is what the *Kafka-specific* half of
the format space needs: the CloudEvents binding is per transport (`ce_*` here, `cloudEvents_*` over
AMQP), so its encoder is built with the SDK's Kafka writer rather than reassembled from a neutral
value, which would be a second copy of the spec to keep in step. A format with nothing Kafka-specific
about it implements the transport-neutral `MessageEncoder` (LLD-core §2.4) once and reaches Kafka
through `KafkaMessageEncoder.from`, and any other transport adapter unchanged.

An encoder that throws is **permanent** either way, classified at the encode site, never by the broker
error classifier (§4).

**Only the binding is in this module.** The envelope itself is built by `CloudEventFactory` in
**`tandem-cloudevents`** (HLD-cloudevents §5), which decides every attribute a consumer reads and
depends on `cloudevents-core` alone. `CloudEventEncoder` is what remains here: route the record, write
the event binary with `cloudevents-kafka`, copy the passthrough headers. A second transport adapter
reuses the factory and writes its own binding, instead of restating the attribute sources, the `type`
fallback and the `seq` rule.

Built with the **CloudEvents Java SDK** (`io.cloudevents:cloudevents-kafka`). Per record, the factory
supplies the event and this module writes it:

```java
var b = CloudEventBuilder.v1()
    .withId(String.valueOf(record.id()))            // id source: outbox id (§6)
    .withSource(source(record))                     // single configured URI: tandem.kafka.source (§6)
    .withType(typeOf(record))                       // §3.3 — null fallback (Q20); version lives here (`.v{n}`)
    .withSubject(record.aggregateId().value())      // = aggregate_id
    .withTime(record.createdAt())
    .withDataContentType(contentType(record))       // §3.2
    .withDataSchema(dataSchema(record))             // §3.2 — optional, from stored header / config
    .withData(record.payload())                     // raw bytes
    .withExtension("seq", record.seq())             // only when record.hasSeq() — see below
    .withExtension("partitionkey", record.aggregateId().value());  // always = key
// traceparent/tracestate are not extensions: they pass through with the other row headers (§3.3)
// no `logicalclock` extension: causal ordering is unbuilt and gets no code on this path
// (HLD-causal-ordering.md §0.3)
CloudEvent ce = b.build();
ProducerRecord<…> pr = KafkaMessageFactory.createWriter(topic, key=aggregateId)
    .writeBinary(ce);                               // binary mode: the only one implemented (§3.1)
```

### 3.1 Content modes
- **Binary (implemented, and the only mode this module emits):** attributes → `ce_*` Kafka headers,
  `data` → body, `content-type` header = `datacontenttype`. Tandem extensions become `ce_seq` /
  `ce_partitionkey` (and `ce_logicalclock` if causal ordering is ever built; it emits nothing today,
  HLD-causal-ordering.md §0).
- **Structured (not implemented, not planned):** the whole CloudEvent (JSON) → body, `content-type:
  application/cloudevents+json`. The SDK writes it with `writeStructured`, so an application that
  needs it implements one encoder of its own (HLD-cloudevents §1).
- **Raw (escape hatch; implemented, opt-in, `RawMessageEncoder` in `tandem-core`, lifted onto Kafka
  with `KafkaMessageEncoder.from`):** no envelope, so the body is the
  payload, the key is `aggregate_id`, a `tandem-id` and a `tandem-type` header travel alongside, and the stored
  `headers` pass through as Kafka headers. Contract: HLD-alternative-envelope §6.

### 3.2 `datacontenttype` & `dataschema` sources
- **`datacontenttype`** = `headers["content-type"]` if the producing side stored one, else the
  configured default (`tandem.kafka.default-content-type`, default `application/json`).
- **`dataschema`** (optional) = `headers["dataschema"]` if present, else config; omitted when neither
  is set. For schema-registry / Avro-Protobuf users (HLD-cloudevents §7). Reuses `headers` (no new
  column — Pareto).

Event **versioning** lives in the `type` (`.v{n}` suffix), not in the topic — see HLD-cloudevents §7.

### 3.3 Header combination
The Kafka record headers in binary mode = the `ce_*` attribute headers + `content-type` + the stored
`headers` (e.g. `correlation-id`), except `content-type` and `dataschema`, which the envelope carries
as attributes. Trace headers (`traceparent`/`tracestate`) pass through as bare headers like any other
row header; they are not mapped to the CloudEvents Distributed-Tracing extension (§6). The causal-ordering value
*would be* carried as the CloudEvents extension **`logicalclock`** → header **`ce_logicalclock`** in
binary mode, read by the consumer-side adapters (`tandem-kafka-streams` / `tandem-flink`). **None of
that ships**: the feature is designed but not built, so no published event carries the header and
neither adapter module exists ([HLD-causal-ordering.md](HLD-causal-ordering.md) §0). (The design docs keep the
technical term *lamport* for the concept; `logicalclock` is only the on-the-wire name.)

### 3.4 Null `type` fallback (Q20)
CloudEvents `type` is required but the `type` column is nullable. **`typeOf(record)` = `record.type()`
if present, else falls back to `aggregate_type`** (configurable fallback). This guarantees a valid
required attribute without forcing the user to set `type`. Raw mode applies the same fallback for its
`tandem-type` header, so a consumer always reads a value there too (HLD-alternative-envelope §6).

---

## 4. Error classification (Q17)

`dispatch` classifies the cause into the `OutboxDispatchException` so `tandem-jdbc` can act:

- **Transient → retriable** (`markForRetry` with backoff, → FAILED at max attempts): Kafka
  `RetriableException` subclasses surfacing after the producer's internal retries exhausted
  (timeouts, `NotEnoughReplicas`, leadership changes), and unknown errors by default.
- **Permanent → fail-fast** (`markFailed` immediately, no wasted retries): data/config errors that
  will never succeed — `RecordTooLargeException`, `SerializationException`, authorization/security
  exceptions, `InvalidTopicException`, `UnsupportedVersionException`.

The classifier is a pluggable `ErrorClassifier` SPI; the above is the default mapping.

**Encoding failures never reach the classifier.** A failure raised *before* the send — the
CloudEvent cannot be built, or the `TopicRouter` cannot route the record — is completed as
**permanent** at the encode site. The classifier maps *broker* errors and defaults an unknown
exception to retriable, which is the wrong default here: re-encoding the same row fails the same
way, so a retry ladder only keeps the aggregate's chain blocked (a `FAILED` head blocks its
successors exactly as a `PENDING` one does) while the row still looks like an ordinary transient
problem. Failing immediately puts the row in `FAILED` with the error recorded, where an operator
can see it.

---

## 5. `TopicRouter` default (Q18)

**Source field = `aggregate_type`** (not the CloudEvents `type`, which is the finer event type and
does not dictate the topic). Default rule: **`kebab-case(aggregate_type)` + suffix**, suffix
configurable (`tandem.kafka.topic-suffix`, default `-topic`), no pluralization:

| `aggregate_type` | topic |
|---|---|
| `Order` | `order-topic` |
| `OrderLine` | `order-line-topic` |

Override via a custom `TopicRouter` bean, or a static `aggregateType → topic` map in config.
(HLD §5.2/§12 examples corrected to `order-topic`; no pluralization.)

---

## 6. Sub-decisions — defaulted for the basic round

- **`id` source** → the **outbox `id`** (globally unique, supports consumer dedup). Settled.
- **`source` convention** → a **single configured URI** (`tandem.kafka.source`, e.g. `/tandem/orders`).
  Per-`aggregate_type` derivation can be added later without breaking consumers (additive).
- **Trace header naming** → **bare `traceparent` / `tracestate`**, passed through with the other row
  headers rather than as the `ce_`-prefixed distributed-tracing extension. Settled: changing the emitted
  names would break every consumer that reads them today (HLD §1.4).
