# Tandem — `tandem-rabbitmq` LLD

**Version:** 0.1 (Draft)
**Module:** `tandem-rabbitmq` · package `com.codingful.tandem.rabbitmq`
**Depends on:** `tandem-core`, `tandem-cloudevents` (the envelope, §5), `com.rabbitmq:amqp-client`
(AMQP 0.9.1). **Relay-side only**, never on the client write-side (HLD §1.3).
**Versioning:** independent of the library (§9).

`tandem-rabbitmq` is the second publish adapter: it implements `OutboxDispatcher` by binding the
CloudEvent that `tandem-cloudevents` built for an `OutboxRecord` to an AMQP message, publishing it
with publisher confirms, and completing a future on the confirm. Everything upstream of the port is
unchanged: the relay engine, the claim protocol and the per-aggregate head rule are the same code
that serves Kafka.

It exists for two reasons. The first is adoption: a service on PostgreSQL and RabbitMQ gets the same
ordered, no-loss delivery without moving to Kafka. The second is that a port with one adapter is a
claim, not a fact. Writing the second one is what proves `OutboxDispatcher` and `MessageEncoder` are
real seams, and the differences this document records (no partitions, no delivery timeout, a
two-valued route) are exactly the places where the abstraction had to be honest.

---

## 1. Channel configuration (the mandated safe config)

Tandem sets these and **fails fast** (`TandemConfigurationException`) when the broker or the
configuration contradicts them. They protect the no-loss and ordering guarantees, and each one is a
way a message can be lost silently if left to its default:

| Setting | Value | Why |
|---|---|---|
| Publisher confirms | `confirmSelect()` on the channel | The confirm **is** the ack. Without it `basicPublish` returns as soon as the bytes are written to the socket, and the row would be marked DONE for a message the broker never accepted. This is the AMQP counterpart of `acks=all` |
| `deliveryMode` | `2` (persistent) | A transient message is confirmed and then lost when the broker restarts. Mark-DONE-after-ack (HLD §4.5) would be marking DONE after an ack that guarantees nothing |
| `mandatory` | `true` on every publish | A message no binding matches is otherwise **dropped silently and still confirmed**: the row goes DONE and no consumer ever sees it. With `mandatory` the broker returns it first, and §4 fails the row permanently |
| Exchange | passively verified at startup | `exchangeDeclarePassive` turns a typo in the exchange name into one startup failure instead of one unroutable return per row, for every row, forever |

**Tandem declares no topology.** No exchange, no queue, no binding. Which queues exist, how they are
bound and how they are consumed is the operator's design and often predates Tandem; a relay that
declared them would be asserting ownership of the consumer's side of the system, and a passive
declare of an existing exchange with different properties fails anyway. The startup check above reads
the topology, it does not create it.

**Connection recovery is on, and pending confirms do not survive it.** The client reconnects on its
own, but the delivery-tag sequence restarts and every confirm still outstanding is lost. On channel
or connection shutdown the adapter therefore completes every pending future **retriably** (§4) and
the relay redelivers those rows. This is the ordinary outbox path, not an error path: at-least-once
delivery with deduplication on `ce_id` is what the design already promises (HLD §4.5).

---

## 2. `OutboxDispatcher` implementation (`RabbitRelay`)

```java
CompletableFuture<Void> dispatch(OutboxRecord record) {
    AmqpMessage message = encoder.encode(record);       // §5: CloudEvent → exchange, routing key, properties, body
    CompletableFuture<Void> ack = new CompletableFuture<>();
    synchronized (publishLock) {
        long tag = channel.getNextPublishSeqNo();        // must be atomic with the publish below
        pending.put(tag, new Pending(record, ack));
        channel.basicPublish(exchange, routingKey, MANDATORY, properties, body);
    }
    return ack;                                          // completed from the confirm listener
}
```

- **One channel, one lock, and the lock covers only the publish.** A `Channel` is not thread-safe,
  and `getNextPublishSeqNo` plus `basicPublish` must be atomic together or a confirm resolves the
  wrong record. The relay's workers publish concurrently, so the critical section is held for the
  write and released immediately; the confirm arrives asynchronously on the client's own thread. The
  overlap that per-shard throughput comes from (LLD-jdbc §3.4) is therefore preserved: what
  serialises is the socket write, not the round trip.
- **Confirms resolve by delivery tag**, in a navigable map, because an ack may carry `multiple=true`
  and resolve every tag up to and including its own.
- **Returns resolve by message id.** `basic.return` carries no delivery tag, so the adapter sets the
  AMQP `messageId` property to the outbox row id and keeps an id-to-tag index. `messageId` is
  therefore **reserved by the adapter**: an encoder that sets it is overridden. The cost is nil, since
  the CloudEvents `id` is that same row id, and the alternative (correlating on exchange plus routing
  key plus body) cannot distinguish two rows of the same aggregate.
- **A return precedes its confirm.** The broker sends `basic.return` and then `basic.ack` for the
  same message, so the return marks the pending entry unroutable and the confirm that follows reads
  the mark and fails the row permanently, rather than completing it successfully first.

---

## 3. The confirm timeout, and why it is not optional

The relay wires the dispatch future with `whenComplete` and imposes **no deadline of its own**
(`RelayWorker.claimAndDispatch`). On Kafka the deadline is the producer's `delivery.timeout.ms`,
which `deliveryTimeoutMillis()` reports so the `rowLease > deliveryTimeout` invariant is checked
against the real value (LLD-jdbc §3.5). AMQP has no equivalent: a confirm that never arrives leaves
the future pending forever.

The consequence is not a slow row, it is a leak. The in-flight slot stays occupied for the lifetime
of the process, so the worker's concurrency window shrinks by one per lost confirm; meanwhile the
row's lease expires and another worker legitimately redelivers it, so the guarantee is intact but the
relay's capacity is not.

`RabbitRelay` therefore enforces its own **confirm timeout**: a scheduled task per pending publish
that completes the future **retriably** and removes the entry. The scheduled task is cancelled when
the confirm arrives, so the steady-state cost is one cancellation per message. The timeout is
reported through `deliveryTimeoutMillis()`, which makes the `rowLease` invariant meaningful for this
adapter too, with the same startup diagnostic and no second value for an operator to keep in sync.

---

## 4. Failure semantics

| Failure | Verdict | Why |
|---|---|---|
| Encoding throws | **Permanent** | Re-encoding the same row fails the same way. Classified at the encode site and deliberately not routed through the classifier, which defaults the unknown to retriable and would block the aggregate for the whole backoff ladder |
| `basic.return` (unroutable) | **Permanent** | No binding matches the routing key. Retrying cannot create one. The row fails, its aggregate stops, and the operator sees the routing key in the log |
| `basic.nack` | Retriable | The broker accepted responsibility and then could not honour it, typically an internal or resource error. It is not a statement about the message |
| Channel or connection shutdown | Retriable | Includes the recovery case of §1. The rows are still PENDING and come back on the next claim |
| Confirm timeout (§3) | Retriable | The message may well have been delivered; the row is redelivered and consumers deduplicate on `ce_id`, as they already must |
| Anything else | Retriable | Same default as the Kafka classifier: an unrecognised failure is assumed transient, because failing a row permanently is the unrecoverable direction |

The last three rows go through `RabbitErrorClassifier`, which mirrors `ErrorClassifier` in
`tandem-kafka` and is pluggable. The first three do not: an encode failure, a return and a nack are
protocol facts rather than thrown exceptions, and their verdict is part of the adapter's contract
rather than a mapping an implementation may reinterpret.

---

## 5. The format port and the CloudEvents binding

**The format is a port here too, and the same one.** `RabbitRelay` publishes whatever a
`RabbitMessageEncoder` gives it. That interface returns an `AmqpMessage` (exchange, routing key,
`BasicProperties`, body) because AMQP's destination is two values and its headers are a typed
property model, neither of which the neutral `EncodedMessage` carries.

A format with nothing AMQP-specific about it is written once against the core's `MessageEncoder` and
reaches this adapter through `RabbitMessageEncoder.from(encoder, exchange)`, exactly as it reaches
Kafka through `KafkaMessageEncoder.from(encoder)`. **This is the demonstration the port was built
for**: one encoder class, no broker type in its signature, running unchanged on two transports.

The lift reads `EncodedMessage.destination()` as the **routing key**, not as the exchange, and takes
the exchange from the adapter's configuration. A neutral encoder names the stream its consumers bind
to, and on AMQP that is the routing key; the exchange is the routing fabric an operator configures
once. Reading it the other way would leave the routing key undefined and make every neutral encoder
publish to a fanout of one.

`CloudEventAmqpEncoder` is the default. The event comes from `CloudEventFactory` in
`tandem-cloudevents`, unchanged and shared with Kafka, so every attribute a consumer reads (the `id`
source, the `type` fallback, whether `seq` is emitted) is decided in one place for both transports.
Only the binding differs, and it is written here:

- attributes become headers prefixed `cloudEvents_` (the CloudEvents AMQP binding), where Kafka uses
  `ce_`. The prefix is declared in `tandem-core`'s `CloudEventsHeaders` beside the Kafka one, because
  a consumer reads these exact names off the wire and they must never drift (HLD §1.4);
- `datacontenttype` becomes the AMQP `contentType` property rather than a header, which is what the
  binding specifies and what an AMQP consumer expects to find;
- the payload is the body, and the stored passthrough headers are copied verbatim.

There is no CloudEvents SDK binding to delegate to: the SDK ships an AMQP binding for Proton, which
is AMQP 1.0, and none for 0.9.1. The binding is written by hand, which is thirty lines and the reason
the envelope had to be a separate module in the first place.

---

## 6. Routing

`RabbitRouter` returns an `AmqpRoute` (exchange, routing key) per record. The default mirrors the
Kafka one so that a reader of both is not surprised: the exchange is the single configured one, and
the routing key is `kebab-case(aggregateType) + suffix`, sharing `TopicRouter.kebabCase` in the core
rather than growing a second copy of the transform.

`aggregate_type` is the routing source, not the finer CloudEvents `type`, for the same reason as on
Kafka (LLD-kafka §5): the routing granularity a consumer binds to is the aggregate, and a per-`type`
routing key would multiply bindings for no gain.

---

## 7. The ordering contract, stated plainly

**Publish order per aggregate holds, and it holds for the same structural reason as on Kafka**: the
relay dispatches only the head row of each aggregate and never sends the next before its confirm
(LLD-jdbc §3.3), so what reaches the exchange is in order.

**Consumer-observed order does not follow from it.** RabbitMQ has no partitions. Several consumers on
one queue are handed messages concurrently, so two events of the same aggregate can be processed out
of order even though they were published in order. This is the one guarantee where this adapter is
weaker than `tandem-kafka` by construction, and it is the difference an adopter must be told about
rather than left to discover.

Two supported ways to keep it, both operator-side:

1. **A queue with `x-single-active-consumer`.** One consumer at a time, order preserved, throughput
   bounded by that consumer. Simplest, and correct by default.
2. **The consistent-hash exchange, keyed on the aggregate id**, with a `RabbitRouter` returning the
   aggregate id as the routing key. Concurrency across aggregates, order within each one. This is the
   closest structural equivalent of a Kafka partition key, and the `cloudEvents_partitionkey` header
   carries that key regardless, so a consumer can see what it was partitioned by.

A consumer that needs neither (an aggregate whose events are commutative) needs no arrangement.

---

## 8. Logging

SLF4J, like `tandem-kafka` and unlike the write-side modules: `amqp-client` already brings
`slf4j-api` transitively, so the choice costs nothing on a footprint that is relay-side anyway
(HLD-logging §2.2). Every `ERROR` and `WARN` carries the row id and the route, the two identifiers
that let an operator find the message without re-running the failure.

---

## 9. Versioning — independent of the library

**`tandem-rabbitmq` carries its own version and its own release cadence**, tagged
`rabbitmq-v<semver>`. It is never bumped to match a library release, and a library release never
implies a connector one. The mechanics mirror `tandem-cli` (LLD-cli §9.1): the library's workflow
triggers on `v*`, which does not match `rabbitmq-v*`, so neither release path can fire on the other's
tag.

**Why independent:** the connector's compatibility contract is the **port** (`OutboxDispatcher`,
`MessageEncoder`, `OutboxRecord`), not the implementation behind it. A library release that leaves
those interfaces untouched cannot break this module, and this module can fix a confirm-handling bug
or add a router with no library change at all. A shared number would assert a coupling that does not
exist.

**What the connector's semver governs:**

| Surface | In the contract? |
|---|---|
| `RabbitRelay`, `RabbitRelayConfig`, the encoder and router interfaces | **Yes** |
| The default route, and the headers and properties put on the wire | **Yes**, it is what a consumer reads |
| The declared floor on `tandem-core` / `tandem-cloudevents` | **Yes**; raising it is a minor bump, never a patch |
| The AMQP client version and the broker versions verified | No; they belong to the compatibility matrix |

**Consequences for the build**, because the two are easy to get wrong in opposite directions: the
module depends on **published coordinates with a floor**, never on sibling projects (a project
dependency stamps this module's own version into the POM as the core's), and a substitution puts the
local projects back during development so the repository still builds as one unit. `floorTest`
re-runs the module's tests with the substitution off, against the declared floor, so the floor is a
verified claim rather than a comment.

**It is not in `tandem-bom`.** The BOM promises one aligned version for every module it lists;
pinning an independently versioned module there would force a BOM release on every connector release
and re-create the coupling this section removes.
