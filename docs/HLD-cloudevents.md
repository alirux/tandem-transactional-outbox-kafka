# Tandem — CloudEvents Publication Format (Spec)

**Version:** 1.0  
**Status:** Draft  
**Companion to:** HLD §4.8 (CloudEvents as the publication format)

Tandem publishes to Kafka using the **CloudEvents** (CNCF) format as its default,
standard envelope. This note specifies the attribute mapping, the Kafka content modes, the
Tandem-specific extensions, and where the dependency lives.

---

## 1. Decision & scope

- **CloudEvents 1.0 is the default publication envelope** for every message the relay sends
  to Kafka.
- **Binary content mode is the only one the relay implements** (CloudEvents attributes → Kafka
  `ce_` headers; the event payload stays the Kafka message **body**).
- **Structured mode** (the whole CloudEvent serialized into the body) is **not implemented and not
  planned.** Its purpose is surviving a hop that drops headers, which neither transport Tandem
  publishes to does; an application that needs it writes one encoder against `MessageEncoder`
  ([HLD-alternative-envelope.md](HLD-alternative-envelope.md) §8, decision 2).
- A **raw passthrough** encoder (no CloudEvents envelope, payload as-is) is the escape hatch for
  existing consumers and migration; CloudEvents is the *standard*, not a hard lock-in. It is
  specified in [HLD-alternative-envelope.md](HLD-alternative-envelope.md) §6 and **implemented,
  opt-in** (`RawMessageEncoder` in `tandem-core`).

---

## 2. Why CloudEvents

- **Interoperability** — a standard envelope understood by the CNCF ecosystem (Knative
  Eventing, brokers, gateways, tracing/observability tooling) without bespoke decoders.
- **Consistent metadata** — `id`, `source`, `type`, `subject`, `time` are first-class and
  uniformly available for routing, filtering, and dead-lettering without deserializing the
  payload (binary mode exposes them as headers).
- **Decoupling** — consumers depend on a public standard, not on Tandem-internal shapes.
- **Natural fit** — CloudEvents' `partitionkey` extension and `subject` map cleanly onto
  Tandem's `aggregate_id`, preserving the per-aggregate ordering model.

---

## 3. Attribute mapping (outbox row → CloudEvent)

| CloudEvents attribute | Kind | Source in Tandem | Notes |
|---|---|---|---|
| `id` | required | outbox `id` | globally unique; supports consumer dedup |
| `source` | required | configured (service/app URI), optionally per `aggregate_type` | e.g. `/tandem/orders` |
| `specversion` | required | `1.0` | constant |
| `type` | required | outbox `type` (captured at produce time) | e.g. `com.acme.order.placed` |
| `subject` | optional | `aggregate_id` | the entity the event is about |
| `time` | optional | `created_at` | event timestamp |
| `datacontenttype` | optional | stored content type / config | default `application/json` |
| `dataschema` | optional | stored `dataschema` header / config (optional) | URI to the payload schema; for schema-registry users (§7) |
| `data` | — | `payload` | the serialized event body |
| `partitionkey` | extension | `aggregate_id` | = Kafka record key; preserves ordering |
| `traceparent` / `tracestate` | passthrough header | from `headers` (§7.1) | bare names, copied from the row like any other row header; not the CloudEvents distributed-tracing extension (§8) |
| `seq` | extension (Tandem) | outbox `seq` column | binary-mode header `ce_seq`. **Present only when the row carries a number**: a row written `unsequenced()` publishes none, and consumers deduplicate on `ce_id`, which is unique by construction and always present (HLD-managed-seq §4.5) |
| `logicalclock`, `causationid` | extension (Tandem), **reserved** | none | never emitted. Wire names (`ce_logicalclock`, `ce_causationid`) declared in `CloudEventsHeaders` so they cannot drift; they belong to the unbuilt causal-ordering surface (HLD-causal-ordering §0) |

The **Kafka record key remains `aggregate_id`** (= `partitionkey`), so the full ordering
chain (DB lock → `seq` → worker shard → Kafka partition) is unchanged.

---

## 4. Content modes

| Mode | Where attributes go | Where `data` goes | `content-type` | Status |
|---|---|---|---|---|
| **Binary** (default) | Kafka `ce_*` headers | message body (raw payload) | the data's type, e.g. `application/json` | **Implemented**, and the only mode the relay emits |
| **Structured** | inside the body | inside the body | `application/cloudevents+json` | **Not implemented, not planned** (§1) |
| **Raw** (escape hatch) | not emitted, beyond `tandem-id` and `tandem-type` | message body (raw payload) | the data's type | **Implemented, opt-in** (`RawMessageEncoder`, HLD-alternative-envelope §6) |

Binary mode is recommended for Kafka: consumers that only want the payload read the body
directly, while routing/filtering can use the `ce_*` headers without deserializing.

---

## 5. Where it lives (and what it does NOT touch)

- **Relay-side only, and split from the transport.** The envelope is built in the **relay**,
  via the official **CloudEvents Java SDK**, by `CloudEventFactory` in **`tandem-cloudevents`**
  (`io.cloudevents:cloudevents-core`). That module decides everything a consumer reads and knows
  nothing about brokers. The **binding onto the wire** is per transport and belongs to the
  transport adapter: `tandem-kafka` adds `io.cloudevents:cloudevents-kafka` and writes the `ce_*`
  headers of §4, and any further adapter (AMQP, whose binding prefixes attributes with
  `cloudEvents_` instead) brings its own. Neither dependency reaches the client write-side.
- **No client/write-side dependency.** Consistent with the deployment topology (§3.2), the
  client and `tandem-spring-producer` do **not** depend on CloudEvents. The write-side only
  needs to capture the event **`type`** (and optionally `datacontenttype`) into the outbox;
  the relay builds the CloudEvent from the stored row + configuration.
- **Topic routing unchanged.** `TopicRouter` still maps `aggregate_type` → topic; the
  CloudEvents `type` is the finer event type and does not dictate the topic.
- **Serialization is orthogonal.** The `PayloadSerializer` produces `data`; CloudEvents is
  the envelope around it. JSON / Avro / Protobuf payloads are carried via `datacontenttype`.

---

## 6. Schema impact

CloudEvents `type` is event-level information the relay cannot infer from `aggregate_type`,
so it must be captured at produce time. Because CloudEvents is the **default** publication
format (§1), the `type` column is part of the **baseline** `tandem_outbox` schema (HLD §5.1) —
not an add-on:

```sql
-- part of the baseline CREATE TABLE (HLD §5.1), shown here for reference:
type VARCHAR(255)   -- CloudEvents `type`, e.g. com.acme.order.placed; nullable (Q20)
```

All other CloudEvents attributes derive from existing baseline columns (`id`, `aggregate_id`,
`aggregate_type`, `created_at`, `payload`, `headers`). (`type` storage as a dedicated column is
the decided option, §8 — not a `headers` entry.)

---

## 7. Event versioning

Two distinct "versions" must not be conflated:
- **`specversion` = `1.0`** — the CloudEvents *envelope* spec (CNCF). Fixed; not Tandem's concern.
- **The event (payload) schema version** — e.g. `OrderPlaced` v1 → v2. The application owns the
  payload schema (Tandem ships bytes); Tandem provides the convention and carries it.

**Convention (decided): the version lives in the `type`**, as a `.v{n}` major suffix:
`com.acme.order.placed.v1` → `com.acme.order.placed.v2`.

**Compatibility (§1.4), applied to events:**
- **Additive** change (a new optional field) → **same version**; consumers are tolerant readers
  (ignore unknown fields). Do **not** bump.
- **Breaking** change (remove/rename/retype a field, or change its meaning) → **bump the major
  version** = a new `type`. The old `type` keeps flowing for existing consumers until they migrate.

**Topic stays version-agnostic.** Every version of an event publishes to the **same topic** (routed
by `aggregate_type`, LLD-kafka §5); consumers discriminate by the `type` (which now carries the
version). The topic never changes per version.

**`dataschema` (optional, decided).** The producer may supply a `dataschema` URI — via a stored
`dataschema` header or config — pointing at the payload schema; the relay maps it to the CloudEvents
`dataschema` attribute. For **Avro/Protobuf with a schema registry**, the registry enforces
compatibility and the schema id rides in the payload bytes, while `dataschema` points at the registry
entry. **JSON** users typically rely on the `type` version alone and may omit `dataschema`.

---

## 8. Decisions

**Decided:**
- **Content mode:** binary, the only mode the relay implements. Structured is not implemented and
  not planned; it belongs to a custom encoder if an application ever needs it (§1, §4).
- **Standard, not lock-in:** CloudEvents is the default + a **raw passthrough** escape hatch,
  specified in HLD-alternative-envelope §6, implemented and opt-in.
- **Trace headers:** bare `traceparent` / `tracestate`, passed through like any other row header,
  not the `ce_`-prefixed distributed-tracing extension. Changing the emitted names would break every
  consumer that reads them today (HLD §1.4).
- **`type` storage:** a dedicated `type` column (queryable; also serves the Admin API search).
- **`id` source:** the **outbox `id`** (globally unique, supports consumer dedup).
- **`source` convention:** a **single configured URI** (`tandem.kafka.source`); per-`aggregate_type`
  derivation can be added later (additive, non-breaking).
