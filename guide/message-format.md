# Publishing Your Own Message Format

---

## 0. Who this page is for

Tandem publishes a CloudEvent by default, to Kafka by default. Neither is a premise. This page is for
you if one of these is true:

- your consumers already read a different envelope, and rewriting them is not on the table;
- you publish to something other than Kafka;
- you need one extra attribute on the wire, and nothing else about the default is wrong.

!!! question "The question this page answers"
    **What do I have to write, and where does it plug in, to change what Tandem puts on the wire?**

The short answer is: one class, implementing one interface, wired as one bean. The rest of this page
is about which interface, and what you owe your consumers afterwards.

---

## 1. Two seams, and the difference between them

Tandem splits **what** it publishes from **where** it publishes it.

| Seam | Interface | You implement it when |
|---|---|---|
| The format | `MessageEncoder` | The bytes and headers are wrong for your consumers |
| The transport | `OutboxDispatcher` | The broker is wrong for you |

They are independent on purpose. A new format costs an encoder and inherits every transport; a new
transport inherits every format already written. If you find yourself implementing `OutboxDispatcher`
in order to change the envelope, stop: that means rebuilding the producer hardening, the failure
classification and the delivery-timeout reporting by hand, and those are the parts that are easy to
get subtly wrong.

Everything below is about the format seam. The transport seam is `tandem-kafka` and `tandem-rabbitmq`
themselves.

---

## 2. Which case are you in?

=== "Keep CloudEvents, change a detail"

    A different `source`, a default `dataschema`, a content type: these are **configuration**, not
    code. `tandem.kafka.source`, `tandem.kafka.default-content-type`, `tandem.kafka.default-data-schema`,
    and their equivalents on other adapters. Stop here.

    An extra header on every message is configuration too, of a sort: write it as a header on the
    outbox row and Tandem passes it through untouched.

=== "Your own envelope, any broker"

    Implement **`MessageEncoder`**. This is the case to reach for: your class names no broker type, so
    the same class works on every transport adapter Tandem has, and on the next one. §3.

=== "Your own envelope, tied to one broker"

    Implement **`KafkaMessageEncoder`** or **`RabbitMessageEncoder`**. Only when the format genuinely
    needs the transport in its signature, which in practice means a binding library that writes the
    broker's own message type. Tandem's own CloudEvents encoders are in this category. §7.

=== "No envelope at all"

    Your consumers want the payload as it is, plus the event id and type. Tandem ships that encoder:
    **`RawMessageEncoder`**, wired like any other. §4.

---

## 3. The portable case: one encoder, any transport

`MessageEncoder` turns a stored row into an `EncodedMessage`: a destination, an ordering key, a body
and flat headers.

```java
public final class OrderEventEncoder implements MessageEncoder {

    @Override
    public EncodedMessage encode(OutboxRecord record) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put("event-id", String.valueOf(record.id()).getBytes(UTF_8));
        // type is optional on the row: fall back to the aggregate type, as the default envelope does
        String type = record.type() != null ? record.type() : record.aggregateType();
        headers.put("event-type", type.getBytes(UTF_8));
        // pass the row headers through: traceparent, correlation-id and your own travel here
        record.headers().forEach((name, value) -> headers.putIfAbsent(name, value.getBytes(UTF_8)));

        return new EncodedMessage(
                "orders",                            // destination
                record.aggregateId().value(),        // ordering key
                record.payload(),                    // body
                headers);
    }
}
```

Two details in it are easy to get wrong. A row may be written without a `type`, so `record.type()` can
be `null`, and an encoder that dereferences it fails that row for good (§5). And an encoder that does
not copy `record.headers()` silently drops trace continuation and the correlation id on the consumer
side. Before deploying, run it against the contract kit (§5, rule 4): it checks both.

That class is the whole change. Wired onto Kafka:

```java
var relay = new KafkaRelay(producerConfig,
        KafkaMessageEncoder.from(new OrderEventEncoder()),
        TandemSpanRecorder.NOOP);
```

and onto RabbitMQ, **unchanged**:

```java
var relay = new RabbitRelay(connectionFactory, config,
        RabbitMessageEncoder.from(new OrderEventEncoder(), "tandem-events"),
        TandemSpanRecorder.NOOP);
```

On Spring with Kafka, there is no wiring to write at all. Contribute the bean and the relay
autoconfiguration lifts it onto Kafka (`tandem.kafka.source` is then not required, since it only
configures the default envelope):

```java
@Bean
MessageEncoder orderEventEncoder() {
    return new OrderEventEncoder();
}
```

On RabbitMQ there is no autoconfiguration: build the `RabbitRelay` yourself, as above, with
`RabbitMessageEncoder.from`, and contribute it as a bean
([Publishing to RabbitMQ](rabbitmq.md#41-spring-boot)).

!!! note "What `destination` and `key` mean per transport"
    `destination` is the stream your consumers subscribe to: the **topic** on Kafka, the **routing
    key** on AMQP (where the exchange is adapter configuration, because it is the routing fabric an
    operator sets up once).

    `key` is the ordering key, and it is what makes published order observable. On Kafka it is the
    partition key. On AMQP there are no partitions: see [§7 of the RabbitMQ
    design](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/docs/LLD-rabbitmq.md)
    for the two arrangements that preserve consumer-side order. Use `aggregateId` unless you know
    exactly why you are not.

!!! warning "What each transport does with your message"
    **Kafka** takes the key verbatim. A wrong key breaks per-aggregate order, silently: nothing fails,
    events of one aggregate just land on different partitions.

    **AMQP** (`RabbitMessageEncoder.from`) drops the key, because AMQP has no partition key; order is a
    matter of topology. It narrows every header value from bytes to a UTF-8 string, sets no
    `contentType` property (a `content-type` row header travels as a plain header), and overwrites
    `messageId` and `deliveryMode` itself, because the relay needs them for return correlation and
    persistence. An encoder cannot break those two, and cannot set them either.

---

## 4. Raw passthrough: no envelope at all

`RawMessageEncoder` publishes the stored payload as it is, with the few headers a consumer needs to
deduplicate and dispatch. Choose it when your consumers read plain payloads and a CloudEvents envelope
would be noise to them, or when you migrate from a system that published payloads bare. It is opt-in:
nothing selects it but your own wiring, and the default stays CloudEvents.

```java
// Kafka: the router you already use, so no topic changes
var relay = new KafkaRelay(producerConfig,
        KafkaMessageEncoder.from(new RawMessageEncoder(TopicRouter.kebabWithSuffix("-topic"))),
        TandemSpanRecorder.NOOP);

// RabbitMQ, ordered topology: route by aggregate id
var relay = new RabbitRelay(connectionFactory, config,
        RabbitMessageEncoder.from(new RawMessageEncoder(r -> r.aggregateId().value()), config.exchange()),
        TandemSpanRecorder.NOOP);
```

On Spring with Kafka, a `@Bean MessageEncoder` returning `new RawMessageEncoder(topicRouter)`, with the
`TopicRouter` bean the Kafka autoconfiguration contributes injected into it, is all the wiring there is.

!!! warning "On AMQP the router decides ordering"
    Lifted onto AMQP, the destination becomes the routing key. The ordered topology (a consistent-hash
    exchange) needs the **aggregate id** as routing key. With the default router every event of a type
    would share the routing key `order-topic`, and per-aggregate order on the consumer side would be
    lost with no signal. Any other router is a choice to give that order up.

**What a consumer of a raw stream reads.** This is a published contract, frozen under the rules of §8:

| Part | Content |
|---|---|
| **Body** | The stored payload, unchanged. |
| **Key** | The aggregate id (Kafka only; AMQP has none). The ordering key is **not** repeated as data. |
| `tandem-id` | The outbox row id, as decimal text: the event id to deduplicate on. A replay republishes the same id. |
| `tandem-type` | The event type; the aggregate type when the row stored none. |
| Every row header | Copied verbatim, in stored order: `content-type`, `dataschema`, `traceparent`, `tracestate`, `correlation-id` and your own. |
| `content-type` | Always present when the event has a content type: the stored header, or the content type the event was written with. |

There is no sequence number, even for a row written with one.

!!! danger "`tandem-*` header names are reserved"
    Raw's own headers share one namespace with yours. A row header named `tandem-id` or `tandem-type`
    does not reach the wire, and any header raw adds later will be named `tandem-*` too. Do not name
    your own headers with that prefix. Nothing rejects such a header at insert; it just disappears
    from the published message.

---

## 5. Four rules your encoder must follow

1. **It must be a pure function of the record.** It is called once per dispatch, from several relay
   worker threads at once. No shared mutable state, no caches keyed on anything but the record.
2. **It must not retain what it returns.** `EncodedMessage` hands over ownership and copies nothing,
   because it lives for exactly one dispatch on the relay's hot path.
3. **Throwing is permanent.** A row that cannot be encoded will not encode on the next attempt either,
   so the dispatcher fails it rather than scheduling a retry. That is the behaviour you want: a retry
   ladder would block that aggregate's whole chain while the row still looked merely pending. Make
   sure you throw only for genuinely unencodable rows, never for a transient dependency.
4. **Run the contract kit in your test suite.** `MessageEncoderContract`, in `tandem-test`, holds your
   encoder to what Tandem's guarantees rest on: every row shape encodes (unsequenced and untyped rows
   included), two encodes of one row are equal, the event id is present, stable and distinct per row,
   the key is the aggregate id, and row headers reach the wire.

   ```java
   MessageEncoderContract.of(new OrderEventEncoder(), MessageEncoderContract.header("event-id"))
           .verify();
   ```

   Declare the row headers your envelope turns into attributes of its own with `consumesHeaders(...)`,
   and a deliberate key other than the aggregate id with `declaresOwnOrderingKey("why")`, so the
   deviation reads as a sentence in your test. `verify()` throws one `AssertionError` listing every
   violation, so it works under any test framework. To run a transport-bound encoder (§7), adapt its
   output to an `EncodedMessage` in your test.

---

## 6. A worked example

Tandem's repository carries one complete custom envelope, exercised end to end on Kafka and on
RabbitMQ: [`DebeziumStyleEncoder`](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/tandem-test/src/testFixtures/java/com/codingful/tandem/test/encoder/DebeziumStyleEncoder.java).
It produces the shape Debezium's outbox event router produces: the topic
`outbox.event.<aggregate type>`, the aggregate id as key, the payload as body, an `id` header with the
event id, and every row header passed through.

```java
public final class DebeziumStyleEncoder implements MessageEncoder {

    public static final String ID_HEADER = "id";

    @Override
    public EncodedMessage encode(OutboxRecord record) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put(ID_HEADER, String.valueOf(record.id()).getBytes(UTF_8));
        record.headers().forEach((name, value) -> {
            if (!name.equals(ID_HEADER)) {
                headers.put(name, value.getBytes(UTF_8));
            }
        });
        return new EncodedMessage("outbox.event." + record.aggregateType(), record.aggregateId().value(),
                record.payload(), headers);
    }
}
```

Its test is the kit, declaring the one header name the envelope claims for itself:

```java
MessageEncoderContract.of(new DebeziumStyleEncoder(), MessageEncoderContract.header(DebeziumStyleEncoder.ID_HEADER))
        .consumesHeaders(DebeziumStyleEncoder.ID_HEADER)
        .verify();
```

That is the whole cost of a custom envelope: one class of twenty lines and one test.

!!! warning "An illustration, not a Debezium implementation"
    The class shows the shape; it is neither faithful to Debezium nor supported by Tandem, and it is
    not published. If you migrate from Debezium, the first thing to reconcile is the id: **Debezium's
    `id` is typically a UUID, where this one is Tandem's numeric row id**, so consumers deduplicating on
    `id` see a different value space. And a consumer built on Spring Messaging must read `id` from the
    raw transport header: Spring reserves `id` on the message it builds and replaces it with its own.

---

## 7. When the format needs the transport

Some formats cannot be expressed as neutral bytes plus headers, because their specification is defined
*per transport*. CloudEvents is the example: the same attribute is a `ce_` header on Kafka and a
`cloudEvents_` header on AMQP, and the binding libraries write the broker's own message type directly.

For those, implement the adapter's own encoder interface instead: `KafkaMessageEncoder` returns a
`ProducerRecord`, `RabbitMessageEncoder` returns an `AmqpMessage`. You give up portability across
adapters, which is the honest trade: a format with a per-transport binding was never portable in the
first place.

If you are extending Tandem's CloudEvents envelope rather than replacing it, reuse `CloudEventFactory`
from `tandem-cloudevents`. It decides every attribute a consumer reads, for every transport, and
writing the binding on top of it is far less code than restating the attribute sources.

---

## 8. What you owe your consumers afterwards

The moment your encoder runs in production, its output is a contract, with the same standing as a REST
API. Tandem treats its own envelope this way and you should treat yours the same, because a relay, its
consumers and the applications writing to the outbox all run at versions that drift apart.

!!! danger "Evolve additively, or not at all"
    **Add** optional headers, new attributes, new event types. **Never** remove a header, rename one,
    change its type, narrow its range, or make an optional one required. Each of those breaks a
    consumer that is running fine today, at the moment you deploy, with no signal until it fails.

And make your consumers **tolerant readers**, which is the other half of the same bargain:

- ignore headers and attributes they do not recognise, rather than rejecting the message;
- ignore unknown fields and unknown enum values in the payload;
- never pin a strict schema that fails closed on an addition.

Over-strict validation on the consumer side breaks the moment the contract grows, which is to say the
moment you do the additive, supposedly safe thing.

---

## 9. What not to do

- **Do not implement `OutboxDispatcher` to change the envelope.** §1.
- **Do not put business data in the ordering key.** It decides partitioning and therefore ordering;
  changing it later reorders events for every aggregate.
- **Do not encode the payload differently per row** without saying so on the wire. That is what
  `content-type` on the outbox row is for, and it reaches the consumer as the envelope's content type.
- **Do not log what you encode.** The payload is business data; the structural identifiers (row id,
  aggregate id, destination) are not.
