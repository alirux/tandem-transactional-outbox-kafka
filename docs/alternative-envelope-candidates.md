# Tandem: Candidate Alternative Envelopes

**Version:** 0.5  
**Status:** Survey, concluded. Option B is deferred and the Debezium shape ships as a worked
example instead ([HLD-alternative-envelope.md](HLD-alternative-envelope.md) §7, step 5, and §8
decision 1).

The question: **which envelope, other than CloudEvents, is widespread and standard enough that
shipping it as a built-in encoder would serve a real share of Tandem's users?** The test is the
Pareto rule of AGENTS.md: a format earns a built-in only if it serves a clear group without making the
common path harder, and needs no dependency on the client side.

---

## 1. Summary

| # | Candidate | Kind | Diffusion | Serves | Built-in envelope? |
|---|---|---|---|---|---|
| 1 | Debezium Outbox Event Router output | Topic and header convention | High among outbox users | Teams migrating from Debezium's outbox | No, but it ships as the **worked example**: a small `MessageEncoder` of its own, in test fixtures |
| 2 | Spring Kafka type header (`__TypeId__`) | Header convention | High in Spring shops | Spring consumers using its JSON deserializer | No: custom encoder |
| 3 | Confluent Schema Registry wire format | Payload framing | Very high | Avro and Protobuf users | No: payload serialization, not an envelope |
| 4 | In-house JSON envelope `{id, type, ..., payload}` | Ad hoc | Very high, but every company differs | Anyone | No: no shared standard to implement |
| 5 | AWS EventBridge event shape | Structured JSON | High in its ecosystem | Publishers targeting EventBridge | No: a transport concern |

Only the first is a candidate, and it is small enough that it ships as an example rather than a
module of its own (§7).

---

## 2. Debezium Outbox Event Router format

**What it is.** Debezium ships an outbox routing transformation. Its outbox table has the same shape
as Tandem's (event id, aggregate type, aggregate id, event type, payload), and it publishes one
message per row:

- **Key:** the aggregate id (`table.field.event.key`, default `aggregateid`).
- **Topic:** derived from the aggregate type, by default `outbox.event.${routedByValue}` with
  `route.by.field` set to `aggregatetype`.
- **Value:** the payload alone (`table.expand.json.payload` defaults to `false`).
- **Headers:** by default a single header, `id` (`table.field.event.id`). Further columns reach the
  headers only when `table.fields.additional.placement` says so; the event `type` is not a header by
  default.
- **Tombstone:** an empty or null payload produces a tombstone only when
  `route.tombstone.on.empty.payload` is set; the default is `false`.

**Who it serves.** Teams that run the Debezium outbox pattern today and want to move to Tandem
without changing their consumers. The group is identifiable, and the data models line up almost one
to one.

**Fit with Tandem.** Body is the stored payload and key is the aggregate id, as in raw passthrough
(HLD-alternative-envelope §6). What differs:

- **The id header's name.** Debezium's is `id`; raw passthrough's is `tandem-id`, because raw keeps
  its own names under a reserved prefix. The example therefore is not raw with another router: it is
  an encoder of its own, emitting `id`.
- **Extra headers.** The example also passes every row header through, where Debezium emits `id`
  alone by default. A tolerant consumer ignores headers it does not read (AGENTS.md, compatibility),
  so the example keeps them: filtering them out would drop `traceparent` and `correlation-id`, which
  are not envelope attributes and so are exactly what the header rule of the encoder contract
  protects (HLD-alternative-envelope §7, step 3). Like Debezium, it emits no `type` header.
- **Id format.** Debezium's `id` is whatever the outbox table stores, in practice a UUID; Tandem's is
  the numeric row id. A consumer that parses the header as a UUID breaks, so "no consumer change"
  holds only for consumers that treat the id as an opaque string. The migration guide must say so.
- **Topic derivation.** `TopicRouter` is a functional interface, so a router of the form
  `outbox.event.<aggregate type>` is one lambda; the shipped helper `kebabWithSuffix` does not
  produce that shape (it appends a suffix to the kebab-cased type, where Debezium prefixes the value
  verbatim), so the example computes its own destination.
- **No dependency.** The minimal client footprint is untouched.

**Verdict.** The worked example (§7): a `MessageEncoder` of a dozen lines, plus the caveat on the
id format. It lives in `tandem-test`'s internal test fixtures, is exercised end to end on both
transports, and the guide walks through it. It is an illustration of the custom-encoder path, not a
supported built-in and not a faithful Debezium implementation; a team migrating for real starts from
it and owns what it builds.

---

## 3. Spring Kafka type header

**What it is.** Spring Kafka's JSON serializer writes the Java class name of the value into a
`__TypeId__` header (`ADD_TYPE_INFO_HEADERS` defaults to `true`), and its JSON deserializer uses it
to pick the target class. A `TYPE_MAPPINGS` setting maps a logical token to a class instead of
carrying the class name.

**Who it serves.** Spring consumers that deserialize straight into a class through the framework.

**Fit with Tandem.** Poor as a built-in. By default the header carries a fully qualified class name,
which ties every consumer to the producer's package layout. With `TYPE_MAPPINGS` the coupling is
avoidable, but the setup is on both sides and specific to Spring. It is one line in a custom encoder,
not a contract Tandem should own.

**Verdict.** Leave to a custom encoder. Worth a mention in the guide's custom-encoder chapter.

---

## 4. Confluent Schema Registry wire format

**What it is.** A framing of the payload: a magic byte (`0`), a four byte big-endian schema id, then
the serialized data. For Protobuf, an array of message indexes (zigzag varints) sits between the
schema id and the data. The registry enforces schema compatibility.

**Who it serves.** Anyone with Avro or Protobuf payloads and a registry. It is the most widespread of
all the candidates.

**Fit with Tandem.** It is not an envelope. It transforms the **payload bytes**, which is the job of
the payload serializer, and Tandem treats payload encoding as orthogonal to the envelope
(HLD-cloudevents §5). It composes with any envelope, CloudEvents included. A registry-aware payload
serializer on the relay side is a separate question, outside this survey.

**Verdict.** Not an option for B. Track it as a serializer question if there is demand.

---

## 5. In-house JSON envelope

**What it is.** A JSON object such as `{id, type, aggregate, occurredAt, payload, metadata}` that a
company defines for itself.

**Who it serves.** Many teams, each with its own field names, nesting and timestamp format.

**Fit with Tandem.** No shared specification exists, so a built-in would match one company's shape and
none of the others. This is the case the custom encoder path exists for: `MessageEncoder` is the
answer.

**Verdict.** Custom encoder. The guide's example is the reference once it preserves row headers and
applies the `type` fallback (HLD-alternative-envelope §7, step 4).

---

## 6. AWS EventBridge event shape

**What it is.** A JSON event with a fixed set of top-level fields (version, id, detail type, source,
account, time, region, resources, detail).

**Who it serves.** Publishers whose destination is EventBridge.

**Fit with Tandem.** The destination decides the format here, which makes it a transport concern: it
would be an EventBridge adapter carrying its own event shape, not an alternative envelope on Kafka or
RabbitMQ. It is out of scope while Tandem publishes to those two.

**Verdict.** Out of scope.

---

## 7. What this means for option B

- No candidate justifies a **separate module** with its own versioned wire contract and the full
  module registration checklist of AGENTS.md.
- The one candidate with an identifiable audience, the Debezium outbox format, is reachable as a
  **single `MessageEncoder`**: no dependency, no new module. That makes it the
  right shape for the **worked example** that proves the seam, which is where it ships.
- The remaining candidates are payload serialization (§4), belong to the custom encoder (§3, §5), or
  belong to a different transport (§6).

The decision: raw passthrough ships as Tandem's opt-in encoder, and the Debezium shape ships as a
worked example beside it.
Option B, a built-in envelope with its own versioned wire contract, stays deferred; the trigger to
reopen it is users migrating from Debezium's outbox
([HLD-alternative-envelope.md](HLD-alternative-envelope.md) §8, decision 1).
