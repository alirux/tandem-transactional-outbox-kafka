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
    broker's own message type. Tandem's own CloudEvents encoders are in this category. §5.

---

## 3. The portable case: one encoder, any transport

`MessageEncoder` turns a stored row into an `EncodedMessage`: a destination, an ordering key, a body
and flat headers.

```java
public final class OrderEventEncoder implements MessageEncoder {

    @Override
    public EncodedMessage encode(OutboxRecord record) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put("event-type", record.type().getBytes(UTF_8));
        headers.put("event-version", "2".getBytes(UTF_8));

        return new EncodedMessage(
                "orders",                            // destination
                record.aggregateId().value(),        // ordering key
                record.payload(),                    // body
                headers);
    }
}
```

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

On Spring, there is no wiring to write at all. Contribute the bean and the relay autoconfiguration
lifts it onto whichever adapter is on your classpath:

```java
@Bean
MessageEncoder orderEventEncoder() {
    return new OrderEventEncoder();
}
```

!!! note "What `destination` and `key` mean per transport"
    `destination` is the stream your consumers subscribe to: the **topic** on Kafka, the **routing
    key** on AMQP (where the exchange is adapter configuration, because it is the routing fabric an
    operator sets up once).

    `key` is the ordering key, and it is what makes published order observable. On Kafka it is the
    partition key. On AMQP there are no partitions: see [§7 of the RabbitMQ
    design](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/docs/LLD-rabbitmq.md)
    for the two arrangements that preserve consumer-side order. Use `aggregateId` unless you know
    exactly why you are not.

---

## 4. Three rules your encoder must follow

1. **It must be a pure function of the record.** It is called once per dispatch, from several relay
   worker threads at once. No shared mutable state, no caches keyed on anything but the record.
2. **It must not retain what it returns.** `EncodedMessage` hands over ownership and copies nothing,
   because it lives for exactly one dispatch on the relay's hot path.
3. **Throwing is permanent.** A row that cannot be encoded will not encode on the next attempt either,
   so the dispatcher fails it rather than scheduling a retry. That is the behaviour you want: a retry
   ladder would block that aggregate's whole chain while the row still looked merely pending. Make
   sure you throw only for genuinely unencodable rows, never for a transient dependency.

---

## 5. When the format needs the transport

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

## 6. What you owe your consumers afterwards

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

## 7. What not to do

- **Do not implement `OutboxDispatcher` to change the envelope.** §1.
- **Do not put business data in the ordering key.** It decides partitioning and therefore ordering;
  changing it later reorders events for every aggregate.
- **Do not encode the payload differently per row** without saying so on the wire. That is what
  `content-type` on the outbox row is for, and it reaches the consumer as the envelope's content type.
- **Do not log what you encode.** The payload is business data; the structural identifiers (row id,
  aggregate id, destination) are not.
