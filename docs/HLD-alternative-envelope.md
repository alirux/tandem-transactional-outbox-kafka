# Tandem: Alternative Publication Envelope (Analysis)

**Version:** 0.6  
**Status:** Settled. Every decision of §8 is answered; §7 is the work that follows from them.  
**Companion to:** [HLD-cloudevents.md](HLD-cloudevents.md), [LLD-core.md](LLD-core.md) §2.4,
[LLD-kafka.md](LLD-kafka.md) §3, [../guide/message-format.md](../guide/message-format.md),
[alternative-envelope-candidates.md](alternative-envelope-candidates.md)  
**Implemented by:** [LLD-alternative-envelope.md](LLD-alternative-envelope.md)

This note answers one question: **what does it take for Tandem to publish an envelope other than
CloudEvents, and how much of that already exists?** It states the current seam, the invariants any
envelope must keep, the gaps, and the options with a recommendation.

---

## 1. Summary

- The format is already a port. An application can publish its own envelope today by implementing
  `MessageEncoder` and exposing it as one bean (Kafka on Spring) or wiring it by hand (Kafka and
  RabbitMQ without Spring, RabbitMQ on Spring). No change to the write side is needed.
- What is missing is not the seam but **the guarantees around it**: a machine-checkable contract for
  what an encoder must preserve (ordering key, event identity, header passthrough), a raw passthrough
  encoder and a `causationid` extension that the design documents declare but the code does not
  contain, and a RabbitMQ Spring wiring the guide implies but does not exist.
- Decided: the steps of §7 close the eight gaps of §4, and step 5 ships a Debezium-shaped encoder as
  a **worked example** rather than a built-in: the point is to prove the path works and that an
  application can walk it, not to own a second wire contract. A second built-in envelope (option B)
  stays deferred until a concrete candidate exists.

---

## 2. What exists

| Piece | Where | Role |
|---|---|---|
| `MessageEncoder` | `tandem-core`, `port/` | Broker-neutral format port: `OutboxRecord` to `EncodedMessage` (destination, key, body, `byte[]` headers) |
| `KafkaMessageEncoder`, `RabbitMessageEncoder` | `tandem-kafka`, `tandem-rabbitmq` | Transport-specific format ports; `KafkaMessageEncoder.from(MessageEncoder)` and `RabbitMessageEncoder.from(MessageEncoder, exchange)` lift a neutral encoder onto each |
| `CloudEventFactory` | `tandem-cloudevents` | Decides every CloudEvents attribute; knows no broker |
| `CloudEventEncoder`, `CloudEventAmqpEncoder` | `tandem-kafka`, `tandem-rabbitmq` | The default envelope, bound to each transport's own CloudEvents binding |
| Spring wiring | `TandemKafkaAutoConfiguration` | A `MessageEncoder` bean is lifted onto Kafka; the `tandem.kafka.source` requirement applies only when the default encoder is used |

The write side stores only `type`, `content-type`, `dataschema` and headers. It carries no
CloudEvents dependency, so the minimal client footprint (AGENTS.md, Minimal client footprint) holds
for any envelope.

---

## 3. Invariants an envelope must keep

A custom encoder is a pure function of the record, but several of Tandem's promises are kept only
because the default encoder happens to honour them. None is enforced today.

| Invariant | Why it matters | Kept today by |
|---|---|---|
| The ordering key is the aggregate id | Per-aggregate order is observable only through the Kafka partition key, which is taken verbatim from `EncodedMessage.key()` | The default encoder |
| The event id is stable across encodes of the same row | Consumer dedup and replay ("a replay republishes the same id") depend on it | `CloudEventFactory` deriving `id` from the outbox row id |
| Row headers not consumed as envelope attributes reach the wire (`traceparent`, `tracestate`, `correlation-id`) | Trace continuation and correlation on the consumer side | `CloudEventFactory.passthroughHeaders`, which withholds `content-type` and `dataschema` because they become `datacontenttype` and `dataschema` |
| `seq` is published only when the row carries one | Consumers must not depend on a value that an unsequenced row lacks | The default encoder |
| The ordering key reaches the consumer as data, not only as the broker key | A consumer can see which key the event was partitioned by without knowing the broker's key concept | The `partitionkey` extension, which `CloudEventFactory` always emits; an envelope that drops it drops this with it |
| Encoding is pure and thread-safe | Called concurrently from several relay workers | Documented rule, not verified |
| A failure to encode is permanent | The dispatcher fails the row instead of retrying | Documented rule, covered by relay tests |

The two transports differ in what an encoder can break:

- **Kafka:** the key is taken verbatim, so an encoder that sets it wrongly breaks per-aggregate order
  with no signal.
- **AMQP:** `RabbitMessageEncoder.from` does not transmit `EncodedMessage.key()` at all (AMQP has no
  partition key; order preservation is a topology concern, LLD-rabbitmq §7), and the relay overwrites
  `messageId` and `deliveryMode` itself, so an encoder cannot break return correlation or
  persistence.

Step 4 adds this asymmetry to the guide.

---

## 4. Gaps

1. **Raw passthrough is specified but not implemented.** Its contract is §6; the code contains no
   encoder for it and no test. Structured mode is outside this gap: it is not implemented and not
   planned, because its purpose is surviving a hop that drops headers and neither transport Tandem
   publishes to does that; an application that needs it writes one encoder of its own (§8 decision
   2).
2. **No contract for encoders.** The guide states three rules (§4 of the guide) in prose. Nothing in
   `tandem-test` lets an encoder author verify them or the invariants of §3.
3. **The guide over-claims RabbitMQ on Spring.** It states that the relay autoconfiguration lifts a
   `MessageEncoder` bean onto whichever adapter is on the classpath. Only Kafka has an
   autoconfiguration; on RabbitMQ the application wires the dispatcher by hand and the bean is not
   lifted.
4. **The guide's example encoder drops every row header and fails on a row with no `type`.** An
   application that copies it loses trace continuation and correlation id on the consumer side without
   any signal. The example also reads `record.type()` without the fallback to `aggregate_type` that
   `CloudEventFactory` applies, so a row with a null `type` throws, and the dispatcher fails that row
   permanently.
5. **Trace headers are documented inconsistently.** LLD-kafka §3.3 and HLD-cloudevents §3 map them
   onto the CloudEvents distributed-tracing extension, and LLD-kafka §6, HLD-cloudevents §8 and the
   open-decisions table of HLD.md list their naming as deferred. The code emits them as bare
   `traceparent` and `tracestate`, and the consumer guide documents them that way.
6. **No end-to-end test with a custom encoder.** The seam is covered per junction (encoder lift,
   relay dispatch, autoconfiguration) but never through a real broker.
7. **Tooling assumes CloudEvents.** The benchmark harnesses and scenarios, both sample modules and
   `EndToEndIT` read `ce_*` headers on Kafka; the RabbitMQ harness reads the `cloudEvents_*` form.
   With another envelope the benchmark measures nothing and the end-to-end test cannot verify order.
8. **The `causationid` extension is documented but never emitted.** HLD §4.8 and HLD-cloudevents §3
   describe `ce_causationid`. The code has only the reserved row-header name
   `TandemHeaders.CAUSATION_ID`, and the javadoc of `CloudEventsHeaders.EXT_SEQ` says `seq` is "always
   present", which contradicts HLD-cloudevents §3.

**Not coupled to CloudEvents:** the Admin API contract and the CLI (they mention CloudEvents only in
the description of the generic `type` column), `tandem-micrometer`, `tandem-tracing-otel`,
`tandem-admin`, `SeqSource` and `OutboxRowMapper` (which name `ce_seq` only in comments), and the
write side.

---

## 5. Options

| | Option | Cost | Assessment |
|---|---|---|---|
| A | Keep the seam as it is; add the contract kit, fix the guide, add an end-to-end test | Low | Makes the custom-encoder path verifiable rather than advisory |
| A+ | A, plus implement the raw passthrough (§6) as an opt-in encoder | Low | Closes gap 1 by adding a small, dependency-free encoder that is itself an alternative envelope |
| A++ | **Chosen:** A+, plus one worked example encoder, exercised end to end and documented in the guide | Low | An example costs a test-fixture class and a guide chapter, carries no wire contract, and answers the question a reader actually has: is writing an encoder a weekend or a quarter? |
| B | Ship a second, fully specified built-in envelope as a separate opt-in module | Medium to high | **Deferred.** Full module registration checklist (AGENTS.md), a second wire contract to version additively, and almost certainly an external dependency. Justified only by a concrete, widely needed format; the trigger to reopen it is users migrating from Debezium's outbox |
| C | Replace CloudEvents as the default | High | Rejected: breaks the published Kafka contract (HLD §1.4) and serves a minority at the cost of the common case (Pareto) |

The candidate formats for B are surveyed in
[alternative-envelope-candidates.md](alternative-envelope-candidates.md).

Sub-choice for A+: a **configuration flag** selecting the content mode, or **distinct encoder
classes** the application wires. With structured dropped, a flag would select between exactly two
encoders, so distinct classes are the answer: they keep the default path free of mode branching and
match how a custom encoder is already selected.

---

## 6. Compatibility

- Any envelope Tandem ships becomes a public wire contract and follows the additive-only rule of
  AGENTS.md (API-first, compatibility section): new optional fields and headers only, never rename or
  retype, unknown headers ignored by readers.
- Raw passthrough carries the smallest vocabulary that keeps the invariants of §3, except the ordering
  key as data: on raw the key is visible only as the broker key. Its contract:
  - **body** is the stored payload;
  - **key** is the aggregate id;
  - a **`tandem-id`** header carries the outbox row id, so consumers can deduplicate and a replay
    republishes the same id;
  - a **`tandem-type`** header carries the event `type`, falling back to `aggregate_type` when the
    column is null, as the CloudEvents encoder does (LLD-kafka §3.4), so a consumer can dispatch
    without parsing the body;
  - every **row header** follows verbatim, except that `tandem-id` and `tandem-type` are the
    encoder's own: a row header with either name does not reach the wire, the same rule
    `CloudEventFactory` applies to `content-type` and `dataschema`;
  - a **`content-type`** header always carries the payload's media type: the stored row header, or
    the record's content type when the row stored none.

  **The raw envelope's own headers use the reserved `tandem-` prefix.** The header namespace is shared
  with the application's row headers, so a generic name would silently drop an application header of
  the same name, and every header raw added later would shadow one more; under the prefix raw can grow
  additively, and an application only has to stay out of `tandem-*` names. Rejected: bare `id` and
  `type`, for exactly those collisions, and because a consumer framework may treat `id` as its own
  (Spring Messaging reserves it on the message it builds). `tandem-id` and `tandem-type` become wire
  names the moment raw ships, and are then frozen under the additive rule.
- The default remains CloudEvents binary mode. No existing consumer changes.

---

## 7. Recommendation

The steps, in order. Each ships on its own, except that step 6 runs the encoder of step 5, and
steps 4 and 5 land together because the guide walks through that encoder. The last column names the
gap of §4 the step closes.

| # | Step | Closes |
|---|---|---|
| 1 | **Align the documents with the code.** The documents state binary as the only implemented content mode, structured as not planned and raw as specified and opt-in. Restate both the `logicalclock` and the `causationid` extensions as reserved in HLD §4.8 (its extension list included) and HLD-cloudevents §3, and correct the "always present" javadoc of `CloudEventsHeaders.EXT_SEQ` in the same change | 8 |
| 2 | **Implement raw passthrough** as its own opt-in encoder class (§5) with the contract of §6, with no new dependency. In the same change: the content-mode status in HLD-cloudevents §4 and LLD-kafka §3.1, and the guide section a consumer of a raw stream reads (§8 decision 4), including the router an ordered AMQP topology needs, since there the destination becomes the routing key | 1 |
| 3 | **Add a `MessageEncoderContract` to `tandem-test`** that any encoder can be run against: every row shape the write side accepts encodes (unsequenced and untyped rows included), two encodes of one row are equal, the event id is present and stable across them, distinct rows have distinct event ids, the key equals the aggregate id unless the encoder declares its own key, in which case the check is skipped and the declaration is the record of a deliberate choice (§8 decision 3), and row headers reach the wire except those the encoder declares as consumed by its envelope. Purity beyond that determinism cannot be checked from outside, and permanence of a failure is a dispatcher property already covered by the relay tests, so neither is part of the kit. The built-in encoders run against it too | 2 |
| 4 | **Correct the guide:** the RabbitMQ Spring claim, the example that drops row headers and ignores the `type` fallback, and the Kafka versus AMQP asymmetry of §3 | 3, 4 |
| 5 | **Ship one worked example encoder, Debezium-shaped** (topic `outbox.event.<aggregate type>`, the payload as body, the aggregate id as key, an `id` header), in `tandem-test`'s **test fixtures**, which are internal by design and never published, so the example is verified without becoming a supported artifact. It drives the tests of step 6, and the guide's custom-encoder chapter walks through it, stating plainly that it is an illustration rather than a faithful or supported Debezium implementation, and naming the one thing a real migration must check: Debezium's `id` is typically a UUID where Tandem's is the numeric row id. Option B stays deferred | none |
| 6 | **Add end-to-end integration tests with a custom encoder on both transports** (§8 decision 5): the full DB-to-broker path in `EndToEndIT` on Kafka, and a case on RabbitMQ in `RabbitRelayIT`, which already has a broker container. AMQP is where the lift can break silently, since it drops the key and narrows headers to UTF-8 strings. The encoder under test is the example of step 5. Also make the ordering assertion of the end-to-end test independent of `ce_seq` | 6 |
| 7 | **Reconcile the trace header documentation with the code.** State the bare `traceparent` and `tracestate` form in LLD-kafka §3.3 and HLD-cloudevents §3, and close the deferred naming entries in LLD-kafka §6, HLD-cloudevents §8 and the open-decisions table of HLD.md. Changing the emitted names would break consumers (HLD §1.4), so the documents move, not the code | 5 |
| 8 | **Fix the scope of the CloudEvents-bound tooling.** State in `docs/LLD-benchmark.md` that the benchmark harnesses are bound to the default envelope by design, and do the same for the sample modules in `docs/LLD-base.md` or in a class comment, since no sample design document exists. Both transports are covered by step 6 | 7 |

Documentation changes accompany each step in the same change, per AGENTS.md.

---

## 8. Decisions

1. **Which envelope, and why not CloudEvents?** **Answered:** none today, so no second built-in
   envelope. Option A++ of §5 proceeds and B is deferred: raw passthrough, the contract kit, and a
   **worked example**, because a seam nobody has walked end to end is a claim, not a feature. The example is
   Debezium-shaped, since that is the one candidate with an identifiable audience
   ([alternative-envelope-candidates.md](alternative-envelope-candidates.md) §2), and it lands as
   step 5. Reopen B if users migrating from Debezium's outbox appear.
2. **Structured mode: implement or restate as not implemented?** **Answered:** not implemented
   and not planned, and the documents say so. The raw encoder of step 2 is a class of its own rather
   than a configuration flag (§5), and its contract is §6.
3. **Is the aggregate id as ordering key an enforced invariant or a recommendation?** **Answered:**
   enforced, with a declared opt-out (step 3). The contract kit fails an encoder whose key is not the
   aggregate id unless that encoder declares another key, which keeps a deviation possible but
   deliberate and visible in the source, instead of breaking per-aggregate order in silence (§3).
4. **Is the consumer side in scope?** **Answered:** no. Tandem ships no consumer library, and one
   would be a large public surface serving a minority (Pareto). The obligation is documentary: the
   guide states the raw contract of §6 so a consumer of a raw-published stream knows what it reads
   (step 2). The sample and the consuming guide stay on the default envelope.
5. **Is the custom-encoder end-to-end test required on RabbitMQ too, or Kafka only?** **Answered:**
   both (step 6). RabbitMQ already has a broker container in `RabbitRelayIT`, so the cost is a test
   case, and AMQP is the transport where a custom encoder can break unnoticed.
