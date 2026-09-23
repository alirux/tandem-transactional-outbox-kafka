# Tandem: Alternative Publication Envelope (LLD)

**Version:** 0.1  
**Status:** Implemented  
**Implements:** [HLD-alternative-envelope.md](HLD-alternative-envelope.md) §7, steps 1 to 8  
**Modules touched:** `tandem-core` (the raw encoder, its header names), `tandem-test` (the contract
kit, the worked example, the end-to-end test), `tandem-kafka` and `tandem-rabbitmq` (tests only),
`guide/`, `docs/`. **No new module and no new dependency:** the module registration checklist and
`THIRD-PARTY-NOTICES.md` are untouched.  
**Companion to:** [LLD-core.md](LLD-core.md) §2.4, [LLD-kafka.md](LLD-kafka.md) §3,
[LLD-rabbitmq.md](LLD-rabbitmq.md) §5, [LLD-test.md](LLD-test.md),
[HLD-cloudevents.md](HLD-cloudevents.md)

The HLD settles *what* to build: a raw passthrough encoder, a contract kit any encoder can be run
against, a worked Debezium-shaped example exercised end to end on both transports, and the document
corrections that go with them. This document settles *where each piece lives, what its public surface
is, and how it is tested*, one section per HLD step, in the order the steps ship. "HLD §n" below means
HLD-alternative-envelope; the main design document is cited as HLD.md.

---

## 0. Placement at a glance

| Piece | Module, source set | Package | Published |
|---|---|---|---|
| `RawMessageEncoder` | `tandem-core`, `main` | `com.codingful.tandem.core` | Yes |
| `RawHeaders` | `tandem-core`, `main` | `com.codingful.tandem.core` | Yes |
| `CloudEventsHeaders.EXT_CAUSATION_ID`, `CE_CAUSATION_ID` | `tandem-core`, `main` | `com.codingful.tandem.core` | Yes (reserved names) |
| `CloudEventsHeaders.CE_ID`, `AMQP_ID` | `tandem-core`, `main` | `com.codingful.tandem.core` | Yes |
| `MessageEncoderContract` | `tandem-test`, `main` | `com.codingful.tandem.test` | Yes |
| `TandemTestContainer.newRelay(RelayConfig, KafkaMessageEncoder)` | `tandem-test`, `main` | `com.codingful.tandem.test` | Yes |
| `DebeziumStyleEncoder` | `tandem-test`, `testFixtures` | `com.codingful.tandem.test.encoder` | **No**, by design (§4.1) |

`TopicRouter` changes only in its javadoc (§2.2).

Every addition to a published type is additive, so none of this is a breaking change (HLD.md §1.4).

---

## 1. Step 1: documents and javadoc aligned with the code

A documentation step, plus one reserved constant.

- **HLD.md §4.8 and HLD-cloudevents §3** restate `causationid` as **reserved**, in the same words
  already used for `logicalclock`: a wire name declared so it cannot drift, emitted by nothing, and
  belonging to the unbuilt causal-ordering surface (HLD-causal-ordering §0). In the attribute table of
  HLD-cloudevents §3 the `seq, logicalclock, causationid` row splits in two: `seq` (emitted when the
  row carries a number) and `logicalclock, causationid` (reserved, never emitted). The extension list
  of HLD.md §4.8 names the wire names (`seq` / `logicalclock` / `causationid`, not the design term
  `lamport`) and marks the last two reserved.
- **`CloudEventsHeaders.EXT_SEQ`**: the javadoc "(always present)" becomes "present only when the row
  carries a number; a row written unsequenced publishes none (HLD-managed-seq §4.5)".
- **`CloudEventsHeaders.EXT_CAUSATION_ID = "causationid"`** and `CE_CAUSATION_ID` are added, with the
  same "Reserved, a name only" javadoc as `EXT_LOGICAL_CLOCK`. The row-header name
  `TandemHeaders.CAUSATION_ID` (`causation_id`) and the extension name (`causationid`) differ, because
  CloudEvents extension names admit only lowercase letters and digits; pinning both in the core is
  what stops a future emitter from inventing a third spelling. Nothing reads or writes the new
  constant, and no test is added for it: a test asserting a constant equals its own literal is a
  tautology (AGENTS.md, Testing §3).

---

## 2. Step 2: raw passthrough (`RawMessageEncoder`)

### 2.1 Where it lives, and why

`RawMessageEncoder` implements the transport-neutral `MessageEncoder` port, so it reaches Kafka
through `KafkaMessageEncoder.from` and RabbitMQ through `RabbitMessageEncoder.from` without either
adapter knowing it exists. It lives in **`tandem-core`**:

- it needs nothing but the JDK, so the zero-dependency invariant of `tandem-core` holds;
- the core is the only module both transport adapters share besides `tandem-cloudevents`, and raw has
  nothing to do with CloudEvents;
- it follows the precedent of `TopicRouter.kebabWithSuffix`, a pure default that lives in the core
  and is wired by the adapters.

Rejected: a copy per transport adapter, which restates one contract twice and lets the two drift;
and a new `tandem-raw` module, which pays the whole registration checklist for one class (Pareto).

This puts relay-side code in a jar the client imports. That is accepted because the
minimal-footprint rule (HLD.md §1.3) constrains dependencies, and the class adds none: a few hundred
bytes of JDK-only code.

### 2.2 Public surface

```java
package com.codingful.tandem.core;

/** Wire names of the raw passthrough envelope (HLD-alternative-envelope §6). Frozen once published. */
public final class RawHeaders {
    public static final String PREFIX = "tandem-";        // reserved for raw's own headers
    public static final String ID = PREFIX + "id";        // the outbox row id, as decimal text
    public static final String TYPE = PREFIX + "type";    // the event type, falling back to aggregate_type
}

public final class RawMessageEncoder implements MessageEncoder {

    /**
     * @param router names the destination of each record: the topic on Kafka, the routing key on AMQP
     * @throws NullPointerException if {@code router} is {@code null}
     */
    public RawMessageEncoder(TopicRouter router) { ... }

    @Override
    public EncodedMessage encode(OutboxRecord record) { ... }
}
```

`RawHeaders` exists for the same reason as `CloudEventsHeaders`: consumers read these exact names off
the wire, so they are declared once in the core and never re-typed as literals, in production code or
in tests (AGENTS.md, Testing §4).

**`PREFIX` is a reservation, not just a spelling.** Raw's headers share one namespace with the
application's row headers, so every name raw emits is a name the application loses. Under the prefix
the loss is confined to `tandem-*`, and a header raw adds later (always under the prefix) is an
additive change rather than a new collision. The class javadoc states that applications must not
name row headers `tandem-*`. The write side does not enforce it: rejecting a header name at insert
would be a new failure on the client path for a rule only raw needs (Pareto).

The destination comes from a `TopicRouter`, the routing port every Kafka deployment already has, so
switching an application from CloudEvents to raw changes no topic. On AMQP the same value becomes the
routing key (LLD-rabbitmq §5), and there **the choice of router decides ordering**: the ordered AMQP
topology (a consistent-hash exchange, LLD-rabbitmq §7) needs the aggregate id as routing key, which
is what `RabbitRouter.byAggregateId` gives the CloudEvents encoder. With the default router every
event of a type would share the routing key `order-topic` and the topology would lose per-aggregate
order with no signal. The router to pass there is `r -> r.aggregateId().value()`, stated in §2.4 and
in the guide. `TopicRouter`'s javadoc, which today says it returns "the destination Kafka topic
name", widens to "the destination: the topic on Kafka, the routing key when a neutral encoder is
lifted onto AMQP".

### 2.3 Encoding

For a record `r`, `encode(r)` returns:

| `EncodedMessage` field | Value |
|---|---|
| `destination` | `router.topicFor(r)` |
| `key` | `r.aggregateId().value()` |
| `body` | `r.payload()`, not copied (the ownership rule of `EncodedMessage`) |
| `headers` | a new `LinkedHashMap`: first `tandem-id`, then `tandem-type`, then every row header in stored order except one named `tandem-id` or `tandem-type`, then `content-type` if the row stored none |

- `tandem-id` is `String.valueOf(r.id())` as UTF-8.
- `tandem-type` is `r.type()` when non-null, else `r.aggregateType()`, as UTF-8. The fallback is the
  CloudEvents one (LLD-kafka §3.4), restated in two lines rather than shared: `CloudEventFactory` is in
  a module the core cannot depend on, and moving the fallback into the core to share it would make a
  CloudEvents rule look like a core rule.
- Row headers are copied verbatim, `content-type` and `dataschema` included: raw has no envelope
  attribute to carry them, so the stored header is the only place a consumer can find the payload's
  media type.
- **`content-type` is always present.** A row read from the store carries it both as a header and as
  `r.contentType()` (the JDBC mapper and `InMemoryOutbox` both set the header at insert), but a record
  built by hand may carry only the latter. When the row has no `content-type` header and
  `r.contentType()` is non-null, the encoder adds `content-type` from it, the same precedence
  `CloudEventFactory` uses for `datacontenttype`. It is not a `tandem-*` name because it is the row's
  own header, restored, not an attribute of raw's.
- A row header named `tandem-id` or `tandem-type` is dropped. Copying it would overwrite the
  encoder's own value in the header map, which is keyed by name; dropping it is also the rule
  `CloudEventFactory.passthroughHeaders` applies to the names it consumes.
- `seq` is not emitted. The raw contract (HLD §6) has no `seq` header, so the invariant "seq only
  when the row carries one" holds trivially, and `encode` never calls `r.seq()`, which throws on an
  unsequenced row.

The encoder is stateless beyond its router, so it is thread-safe whenever the router is (the default
router is).

### 2.4 Wiring

No property selects raw: it is a class the application wires, exactly like any custom encoder (HLD
§5, sub-choice for A+).

- **Plain Java, Kafka:** `new KafkaRelay(producerConfig, KafkaMessageEncoder.from(new
  RawMessageEncoder(router)), spanRecorder)`.
- **Plain Java, RabbitMQ:** `new RabbitRelay(factory, cfg, RabbitMessageEncoder.from(new
  RawMessageEncoder(r -> r.aggregateId().value()), cfg.exchange()), spanRecorder)` for the ordered
  topology; any other router is a choice to give up per-aggregate order on the consumer side (§2.2).
- **Spring, Kafka:** a `@Bean MessageEncoder` returning `new RawMessageEncoder(topicRouter)`, with
  the `TopicRouter` bean the Kafka autoconfiguration already contributes injected into it.
  `TandemKafkaAutoConfiguration` lifts it (LLD-spring-config §4.4), and `tandem.kafka.source` is then
  not required. No autoconfiguration change.

### 2.5 Tests

`tandem-core`, `RawMessageEncoderTest`, pinning the contract of HLD §6 behaviour by behaviour:

- the body is the stored payload and the key is the aggregate id;
- the `tandem-id` header is the row id;
- the `tandem-type` header is the event type, and the aggregate type when the row stored none;
- every row header reaches the wire, `content-type` included, in stored order;
- a row header named `id` or `type` reaches the wire like any other: only the prefixed names are
  raw's;
- a row header named `tandem-id` or `tandem-type` does not reach the wire, and the encoder's own
  value does;
- a record carrying its content type only as `contentType()` publishes it as `content-type`, and a
  stored `content-type` header wins over it;
- an unsequenced row encodes;
- the destination is what the router names (a router that returns a fixed transform output, per
  Testing §4, not the default router restated).

Assertions reference `RawHeaders` and `TandemHeaders` constants, never the literals. The run against
the contract kit is in `tandem-test` (§3.5), because `tandem-core` has no test dependency on
`tandem-test` and one class is no reason to add it.

### 2.6 Documents

In the same change:

- the status of raw becomes **implemented, opt-in** in HLD-cloudevents §1 and §4, LLD-kafka §3.1,
  the raw sentence of HLD.md §4.8, and the Q19 entry of `open-questions-lld.md`;
- LLD-core §2: the ports table names `RawMessageEncoder` among the implementations of
  `MessageEncoder`, and §2.4 documents it and `RawHeaders` beside the port;
- `TopicRouter`'s javadoc (§2.2);
- **the guide**, because a consumer of a raw stream must be able to read its contract the day raw
  ships (HLD §8 decision 4): a new "Raw passthrough" section in `guide/message-format.md` (when to
  choose it; the wiring of §2.4, the AMQP router included; the contract of HLD §6: body, key,
  `tandem-id`, `tandem-type` with its fallback, every row header, `content-type` always present, no
  `seq`, no ordering key as data, and the `tandem-*` reservation), and one line in
  `guide/consuming-events.md` pointing a consumer of a raw stream to it.

---

## 3. Step 3: the contract kit (`MessageEncoderContract`)

### 3.1 Where it lives, and what it depends on

`tandem-test`, `main` source set, package `com.codingful.tandem.test`: it is for application
encoder authors, so it must be in the published jar, next to `InMemoryOutbox` and
`RecordingDispatcher`.

It is **framework-neutral**: it throws `AssertionError`, which every test framework reports as a
failure, and depends on nothing beyond `tandem-core`. Rejected: a JUnit interface with `@Test`
default methods (the usual "contract test" shape), because it would make JUnit Jupiter part of
`tandem-test`'s published API and force the application's test framework; and AssertJ assertions
inside the kit, for the same reason.

### 3.2 Public surface

```java
public final class MessageEncoderContract {

    /** Starts a contract run for an encoder whose event id the given function reads off its output. */
    public static MessageEncoderContract of(MessageEncoder encoder, Function<EncodedMessage, String> eventId);

    /** Reads the event id from a header, as UTF-8 text: header(RawHeaders.ID), header(CloudEventsHeaders.CE_ID). */
    public static Function<EncodedMessage, String> header(String name);

    /** Row headers the envelope consumes as attributes of its own; exempt from the passthrough check. */
    public MessageEncoderContract consumesHeaders(String... names);

    /** Declares that the ordering key is deliberately not the aggregate id; skips the key check. */
    public MessageEncoderContract declaresOwnOrderingKey(String reason);

    /** Replaces the built-in fixture rows (§3.4) with the caller's; at least two distinct row ids. */
    public MessageEncoderContract records(List<OutboxRecord> fixtures);

    /** Runs every check; throws one AssertionError listing every violation found. */
    public void verify();
}
```

- The **event id extractor is mandatory**, a parameter of `of` rather than a setter with a default:
  the kit cannot guess where an envelope keeps its id (a header, a body field, a transport property),
  and a default that guessed wrong would report a stable id as missing.
- **`declaresOwnOrderingKey(String reason)`** takes a reason that is not checked, only required to be
  non-blank. It is there so the deviation reads as a sentence in the test source, which is the "record
  of a deliberate choice" of HLD §8 decision 3. It also covers an encoder whose transport has no key
  at all (§3.5).
- `verify()` collects every violation before throwing, so one run shows an author everything wrong
  with the encoder rather than one problem per run.
- Transport-bound encoders (`KafkaMessageEncoder`, `RabbitMessageEncoder`) are run by adapting their
  output to an `EncodedMessage` in the calling test (§3.5). The kit takes no overload per transport:
  `tandem-test` depends on `tandem-kafka` but must not depend on the independently versioned
  `tandem-rabbitmq`, and one overload without the other would be a kit that half-knows its transports.

### 3.3 The checks

Run on every fixture row. Each violation names the check, the fixture's row id and the offending
field names; it never prints a payload or a header value, the same rule as `toString()` in
AGENTS.md, Logging §5.

| # | Check | Fails when |
|---|---|---|
| 1 | The encoder accepts every fixture | `encode` throws or returns `null`. A throw is permanent in production, so an encoder that throws on an unsequenced or untyped row fails that row for good |
| 2 | Two encodes of one row are equal | Destination, key, body bytes or the header map (compared by name, values by content) differ between two calls. This is the part of purity observable from outside: it catches a clock, a random value or a counter |
| 3 | The event id is present and stable | The extractor returns `null` or blank, or a different value across the two encodes of check 2 |
| 4 | Distinct rows have distinct event ids | Two fixtures with different row ids yield the same event id. Stability alone would accept a constant, which defeats consumer deduplication |
| 5 | The ordering key is the aggregate id | `key` differs from `aggregateId().value()`, unless `declaresOwnOrderingKey` was called |
| 6 | Row headers reach the wire | A row header whose name is not in `consumesHeaders` is missing from the output, or its value differs from the row value's UTF-8 bytes |

Check 4 exists because an id that is stable but shared is as useless for deduplication as one that
is unstable. Not checked, as the HLD records: purity beyond determinism, and the permanence of a failure, which is a dispatcher property
already pinned by the relay tests.

Check 6 does not assert the converse (that a consumed header is absent): the CloudEvents Kafka binding
writes `datacontenttype` as a `content-type` header, so a consumed name may legitimately reappear as
the envelope's own attribute.

### 3.4 Built-in fixtures

Three rows, with distinct row ids and the same aggregate:

1. **sequenced and typed**, a JSON payload, and the row headers `content-type`, `dataschema`,
   `traceparent`, `tracestate`, `correlation-id` and one application header;
2. **unsequenced** (`unsequenced()`), otherwise as 1;
3. **untyped** (`type` null), otherwise as 1.

The fixtures are built the way the store builds a row: the content type is set both as the message's
`contentType` and as the `content-type` header, so an encoder is verified on what it will actually
receive. `dataschema` is there so that an envelope declaring it consumed (the CloudEvents ones) is
held to that declaration on a row that carries it.

Rows 2 and 3 are the two shapes the write side accepts and a naive encoder breaks on: `record.seq()`
throws on the first, `record.type()` returns `null` on the second (HLD §4, gap 4). An encoder that
needs a specific payload shape, or that must be verified on rows the defaults do not cover, passes its
own through `records(...)`; its passthrough check then covers only the headers those rows carry, which
the javadoc of `records` states.

### 3.5 Who runs it

| Encoder | Test class | Adapter to `EncodedMessage` | Declarations |
|---|---|---|---|
| `RawMessageEncoder` | `tandem-test`, `RawMessageEncoderContractTest` | none | `consumesHeaders(RawHeaders.ID, RawHeaders.TYPE)`, event id `header(RawHeaders.ID)` |
| `DebeziumStyleEncoder` (§4) | `tandem-test`, `DebeziumStyleEncoderTest` | none | `consumesHeaders(DebeziumStyleEncoder.ID_HEADER)`, event id `header(DebeziumStyleEncoder.ID_HEADER)` |
| `CloudEventEncoder` | `tandem-kafka`, `CloudEventEncoderTest` | topic, key, value, headers of the `ProducerRecord` | `consumesHeaders(CONTENT_TYPE, DATA_SCHEMA)`, event id `header(CloudEventsHeaders.CE_ID)` |
| `CloudEventAmqpEncoder` | `tandem-rabbitmq`, `CloudEventAmqpEncoderTest` | routing key as destination, no key, header values as UTF-8 | as Kafka, event id `header(CloudEventsHeaders.AMQP_ID)`, and `declaresOwnOrderingKey("AMQP carries no ordering key; order is a topology concern, LLD-rabbitmq §7")` |

Each is one test method, e.g.
`GIVEN_the_default_kafka_envelope_WHEN_it_is_held_to_the_encoder_contract_THEN_it_keeps_every_invariant`.
The two adapters are private helpers of those test classes; they are ten lines each and describe the
test's view of the transport, not an API.

`CloudEventsHeaders` gains `CE_ID` (`ce_id`) and `AMQP_ID` (`cloudEvents_id`) in this step, so these
tests name the id header through a constant rather than a literal (Testing §4). They are the binary
forms of the standard `id` attribute, composed from the existing prefixes, and a consumer-side reader
has the same use for them.

### 3.6 The kit's own tests

`tandem-test`, `MessageEncoderContractTest`. Each case builds a small encoder inline, a real
collaborator rather than a mock (AGENTS.md, Testing §2), and asserts `verify()` passes or throws an
`AssertionError` naming the check:

- a compliant encoder passes;
- an encoder keying by aggregate type fails check 5, and passes once it declares its own key;
- an encoder dropping row headers fails check 6, and passes once it declares them consumed;
- an encoder minting a random UUID per encode fails checks 2 and 3;
- an encoder emitting a constant id fails check 4;
- an encoder stamping the current time into a header fails check 2;
- an encoder calling `record.seq()` unguarded fails check 1 on the unsequenced fixture;
- an encoder calling `record.type().getBytes(...)` fails check 1 on the untyped fixture;
- one run with several violations reports all of them;
- `records(...)` with fewer than two distinct row ids is refused with `IllegalArgumentException`,
  and `declaresOwnOrderingKey` with a blank reason too.

### 3.7 Documents

LLD-test gains a section for the kit (its checks, its fixtures, how a transport-bound encoder is
adapted), and the kit joins the "In" list of its §5. LLD-core §2.4 notes the new `CloudEventsHeaders`
constants.

---

## 4. Steps 4 and 5: the guide and the worked example

The two steps land as one change, because the guide chapter walks through the example's code.

### 4.1 `DebeziumStyleEncoder`

`tandem-test`, **`testFixtures`** source set, package `com.codingful.tandem.test.encoder`. The
testFixtures variants are already excluded from publication (`tandem-test/build.gradle.kts`), so the
class is verified in this repository without becoming a supported artifact, which is the point of
HLD §8 decision 1.

A `MessageEncoder`:

| Field | Value |
|---|---|
| `destination` | `"outbox.event." + aggregateType()`, the aggregate type verbatim (Debezium's default route) |
| `key` | `aggregateId().value()` |
| `body` | `payload()` |
| `headers` | `id` (the public constant `ID_HEADER`) = the row id as UTF-8, then every row header except one named `id` |

Its class javadoc states, in this order: that it illustrates the shape of Debezium's outbox event
router and is neither faithful to it nor supported; that Debezium's `id` is typically a UUID where
this one is the numeric row id, which is the first thing a real migration must reconcile; that a
consumer built on Spring Messaging reads it from the raw transport header, because Spring reserves
`id` on the message it builds and replaces it; and that it is a test fixture, never published. The
header name is the class's own constant `ID_HEADER`, not `RawHeaders.ID`: the example is an
application's encoder and must read as one, with no dependency on Tandem's own wire names. It is
public so the tests reference it rather than repeating the literal.

It is also the encoder the end-to-end tests of step 6 run (§5), so a change that breaks it breaks a
test on both transports.

### 4.2 Guide changes (`guide/message-format.md`)

| Where | Change |
|---|---|
| §3, the example | Pass row headers through (`record.headers()` copied, UTF-8), and use `record.type() != null ? record.type() : record.aggregateType()`. Cite the contract kit as the way to check an encoder before deploying it |
| §3, the Spring paragraph | Kafka only: the relay autoconfiguration lifts a `MessageEncoder` bean onto Kafka. On RabbitMQ there is no autoconfiguration, and the application builds the `RabbitRelay` with `RabbitMessageEncoder.from` itself |
| §3, the note on `destination` and `key` | Add the asymmetry of HLD §3: on Kafka the key is taken verbatim and a wrong one breaks per-aggregate order silently; on AMQP the lift drops the key, narrows header values to UTF-8 strings, sets no `contentType` property, and the relay owns `messageId` and `deliveryMode` |
| §4, the three rules | Add a fourth: run `MessageEncoderContract` in your test suite, with a two-line example |
| New section, "A worked example" | Walks through `DebeziumStyleEncoder` with a link to its source (an absolute GitHub URL, since the guide renders on the site), the kit run, and the migration warning of §4.1 |

The raw section of the guide ships with raw itself (§2.6). The rest of `guide/consuming-events.md`
stays on the default envelope (HLD §8 decision 4).

---

## 5. Step 6: end to end with a custom encoder

### 5.1 `TandemTestContainer`

One published overload:

```java
/**
 * A relay wired end to end as {@link #newRelay(RelayConfig, TopicRouter, KafkaRelayConfig)} is,
 * publishing through the given encoder instead of the CloudEvents default.
 */
public WorkerPool newRelay(RelayConfig cfg, KafkaMessageEncoder encoder);
```

Same bucket-count guard, same store, same tracking and closing of the dispatcher; the only difference
is the `KafkaRelay(producerConfig, encoder, NOOP)` constructor. The existing overload delegates to it
with a `CloudEventEncoder`, so the two cannot drift.

### 5.2 `EndToEndIT` (Kafka)

- **Ordering read from the payload.** The fixture payload already carries `seq`
  (`{"agg":"…","seq":n}`); `consumeAll` takes a `Function<ConsumerRecord<String, byte[]>, Long>`
  that reads the order, and the numbered test passes a payload reader (a pattern match on the fixed
  fixture shape: no JSON library is on that classpath, and the test writes the payload itself), so its ordering assertion no
  longer depends on `ce_seq`. The managed-seq test keeps reading `ce_seq`: what it asserts is the
  number Tandem assigned, which exists only in the CloudEvents envelope.
- **New test**,
  `GIVEN_an_application_encoder_WHEN_the_relay_publishes_through_it_THEN_each_aggregate_arrives_in_order_with_its_row_headers_and_its_own_id`:
  inserts the numbered fixture with a `correlation-id` row header, creates the topic
  `outbox.event.Order` with four partitions (the existing tests create their topics explicitly),
  relays through `KafkaMessageEncoder.from(new DebeziumStyleEncoder())`, and
  asserts no loss, per-aggregate order from the payload, the record key equal to the aggregate id,
  the `correlation-id` header equal to the inserted value, and `id` headers all distinct. The
  inserted header value is a named constant used as both input and expectation (Testing §4).

### 5.3 `RabbitRelayIT` (AMQP)

- `tandem-rabbitmq/build.gradle.kts` adds `testImplementation(testFixtures(project(":tandem-test")))`,
  a test-only dependency, so the module's own versioning (LLD-rabbitmq §9) is unaffected.
- **New test**,
  `GIVEN_an_application_encoder_WHEN_the_relay_publishes_through_it_THEN_the_message_arrives_on_its_routing_key_with_its_headers_as_text`:
  declares a queue bound to the configured exchange with the routing key `outbox.event.Order`,
  publishes two rows of one aggregate through `RabbitMessageEncoder.from(new DebeziumStyleEncoder(),
  EXCHANGE)`, one after the other's confirm, and asserts the arrival order, the body equal to the
  stored payload, the `id` header equal to the row id as text, and the `correlation-id` header equal
  to the stored value. Header values are compared through `toString()`, because the AMQP client hands
  them back as `LongString`: that narrowing is exactly what the test exists to observe.

`RabbitRelayIT` publishes stored records directly rather than through a database; the database half
of the path is transport-independent and already covered by `EndToEndIT`, so repeating it here would
test PostgreSQL twice and RabbitMQ no more.

---

## 6. Steps 7 and 8: documents only

**Step 7, trace headers.** The code emits `traceparent` and `tracestate` as bare headers, copied from
the row (LLD-kafka §3.3 describes them otherwise). The documents move:

- LLD-kafka §3.3 and HLD-cloudevents §3: the trace headers pass through as bare `traceparent` and
  `tracestate`, like any other row header, rather than as the CloudEvents distributed-tracing
  extension; the attribute table row for them moves from "extension" to "passthrough header";
- the code sketch of LLD-kafka §3, whose comment says the trace extensions are "copied from stored
  headers", says instead that they pass through with the other row headers;
- the deferred naming entries in LLD-kafka §6, HLD-cloudevents §8 and the open-decisions table of
  HLD.md are closed with that decision and its reason: changing the emitted names would break every
  consumer that reads them today (HLD.md §1.4).

**Step 8, tooling bound to the default envelope.**

- `docs/LLD-benchmark.md` §5 (`CorrelationConsumer`), beside the table that reads `ce_seq` and
  `cloudEvents_partitionkey`: the harnesses verify order and correlation through the CloudEvents
  attributes by design, because the benchmark measures the relay and the default envelope is the one
  every user runs; a custom envelope's cost is its own encoder's, outside the benchmark's question.
- The samples (`SampleApplication`, `SampleConsumer`, `SampleRunner`) get one class-level sentence
  each saying they read the default envelope on purpose, since no sample design document exists and
  `LLD-base.md` describes coordinates, not behaviour.

---

## 7. Per step: files, tests, documents

| Step | Production files | Test files | Documents in the same change |
|---|---|---|---|
| 1 | `CloudEventsHeaders` | none | HLD.md §4.8, HLD-cloudevents §3 |
| 2 | `RawMessageEncoder`, `RawHeaders`, `TopicRouter` javadoc | `RawMessageEncoderTest` | HLD-cloudevents §1 and §4, LLD-kafka §3.1, HLD.md §4.8, LLD-core §2, `open-questions-lld.md` Q19, `guide/message-format.md` (raw section), `guide/consuming-events.md` |
| 3 | `MessageEncoderContract`, `CloudEventsHeaders.CE_ID` and `AMQP_ID` | `MessageEncoderContractTest`, `RawMessageEncoderContractTest`, one case each in `CloudEventEncoderTest` and `CloudEventAmqpEncoderTest` | LLD-test, LLD-core §2.4 |
| 4 and 5 | `DebeziumStyleEncoder` (testFixtures) | `DebeziumStyleEncoderTest` | `guide/message-format.md` |
| 6 | `TandemTestContainer` overload, `tandem-rabbitmq/build.gradle.kts` | `EndToEndIT`, `RabbitRelayIT` | LLD-test §4 (the overload) |
| 7 | none | none | LLD-kafka §3 (code sketch), §3.3 and §6, HLD-cloudevents §3 and §8, HLD.md open decisions |
| 8 | sample class comments | none | LLD-benchmark §5 |

When the last step lands, HLD-alternative-envelope's status line moves from "Settled" to
"Implemented", and its §4 gaps are marked closed with the step that closed each.

---

## 8. Compatibility

- **New wire names:** `tandem-id` and `tandem-type`, on raw only, frozen from the first release
  that ships them (HLD §6), and the `tandem-*` namespace reserved for raw's future headers. The
  default envelope's wire is unchanged: no header added, renamed or removed.
- **New public API:** `RawMessageEncoder`, `RawHeaders`, `CloudEventsHeaders.EXT_CAUSATION_ID`,
  `CE_CAUSATION_ID`, `CE_ID` and `AMQP_ID`, `MessageEncoderContract`, one `TandemTestContainer`
  overload. All additive, so a minor release.
- **A new check in the kit is a breaking change.** It can make an application's existing encoder
  test fail on upgrade: that is the kit doing its job, but it breaks a green build, so the release
  that adds a check lists it under **Breaking** and is versioned as one (AGENTS.md, Releases).
  Removing or loosening a check is a behaviour change too, and goes in the release notes.
