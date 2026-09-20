# Consuming Events

---

## 0. Who this page is for

You write the code that **reads** what Tandem publishes: a service that builds a projection, sends a
notification, or updates another system from events. Tandem takes care of getting the event onto Kafka
reliably and in order; this page is about what arrives, what you can rely on, and what your consumer
must do about the guarantees that come with it.

!!! question "The question this page answers"
    **What does a consumer receive, how do I read it, and how do I make my processing correct when
    delivery is at least once?**

It covers Kafka with the default CloudEvents envelope. The consumer is your own code, in whatever
language you use: Tandem has no consumer library to depend on, and needs none.

---

## 1. What arrives

Each event is one Kafka record, in the CloudEvents **binary** mode: the attributes travel as headers
and the payload is the record's value.

| Part | Content |
|---|---|
| **Topic** | Derived from the aggregate type: `Order` goes to `order-topic` by default. See [Advanced Integration](advanced-integration.md#2-topics-keys-and-partitions). |
| **Key** | The aggregate id. |
| **Value** | The payload, as JSON bytes. |
| **Headers** | The attributes below, then any headers stored with the event. |

This is a real record, read back from a running relay:

```
key:    order-1
value:  {"total": 10}
ce_specversion=1.0
ce_id=1
ce_source=/orders/service
ce_type=com.acme.order.placed
content-type=application/json
ce_subject=order-1
ce_time=2026-09-20T13:37:56.615651Z
ce_partitionkey=order-1
x-tenant=acme
correlation-id=req-42
```

| Header | Meaning |
|---|---|
| `ce_specversion` | The CloudEvents version, `1.0`. |
| `ce_id` | The event id: the outbox row id. See §2. |
| `ce_source` | Which application produced it, the `tandem.kafka.source` you configured. |
| `ce_type` | What happened, for example `com.acme.order.placed`. When the event stored no type, the aggregate type. |
| `ce_subject` | The aggregate id. |
| `ce_time` | When the row was created, in UTC. It is the time of the insert, not of the commit or of the publish. |
| `ce_partitionkey` | The Kafka key again, so a consumer can see it without knowing Kafka's key concept. |
| `ce_seq` | The aggregate's sequence number. **Present only** when the event was written with one (`seq` or `managedSeq`); absent for `unsequenced`. |
| `content-type` | The payload's content type, `application/json` by default. |
| `ce_dataschema` | Only when a data schema was configured. |
| `traceparent`, `tracestate`, `correlation-id` | Only when [tracing](observability.md#4-distributed-traces) is enabled on the write side. |
| Anything else | Headers your application attached to the event, copied unchanged. |

Because this is the standard binary binding, any CloudEvents SDK with a Kafka binding reads these
records as events with no Tandem-specific code.

---

## 2. What you can rely on, and what you cannot

**You can rely on:**

- **The key is the aggregate id**, so all events of one aggregate are on one partition, in the order
  they were written.
- **`ce_id` is stable.** The same event carries the same `ce_id` every time it is delivered, including
  after a relay restart and after an operator's replay.
- **`ce_type` is stable.** The meaning of an event type never changes once published, and new types
  and new headers are only ever added.

**Do not rely on:**

- **`ce_id` being unique across sources.** It is the outbox row id, a number that counts up within
  **one** application's database. Two services each produce an event with id `1`. The identity of an
  event is the pair **`ce_source` and `ce_id`**.
- **An order between aggregates.** Events of different aggregates are independent, and arrive in any
  order relative to each other.
- **`ce_seq` being there,** unless you know the write side sends one.
- **The payload being byte for byte what was written.** It is stored as `jsonb` and comes back
  semantically identical, with key order, whitespace and number formatting possibly normalized. Never
  sign, hash or compare the payload as a string.
- **Headers you do not know.** More will be added. Ignore what you do not recognize, and do the same
  with unknown fields inside the payload.

---

## 3. Reading an event

Every example does the same four things: read the attributes from the headers, build the identity
`(ce_source, ce_id)`, skip the event if it was already processed, and **commit the offset only after
processing**. Committing first would lose the event if the consumer died in between.

=== "Java"

    ```java
    Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("group.id", "order-projection");
    props.put("enable.auto.commit", "false");
    props.put("auto.offset.reset", "earliest");
    props.put("key.deserializer", StringDeserializer.class.getName());
    props.put("value.deserializer", ByteArrayDeserializer.class.getName());

    try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(props)) {
        consumer.subscribe(List.of("order-topic"));
        while (true) {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofSeconds(1))) {
                Map<String, String> event = new HashMap<>();
                for (Header header : record.headers()) {
                    event.put(header.key(), new String(header.value(), StandardCharsets.UTF_8));
                }
                String eventKey = event.get("ce_source") + "|" + event.get("ce_id");
                if (!alreadyProcessed(eventKey)) {
                    handle(event.get("ce_type"), record.key(), record.value());
                    markProcessed(eventKey);
                }
            }
            consumer.commitSync();
        }
    }
    ```

=== "Spring Kafka"

    ```yaml
    spring:
      kafka:
        consumer:
          group-id: order-projection
          auto-offset-reset: earliest
          key-deserializer: org.apache.kafka.common.serialization.StringDeserializer
          value-deserializer: org.apache.kafka.common.serialization.ByteArrayDeserializer
    ```

    ```java
    @KafkaListener(topics = "order-topic")
    void onEvent(ConsumerRecord<String, byte[]> record) {
        Map<String, String> event = new HashMap<>();
        record.headers().forEach(header ->
            event.put(header.key(), new String(header.value(), StandardCharsets.UTF_8)));

        String eventKey = event.get("ce_source") + "|" + event.get("ce_id");
        if (!alreadyProcessed(eventKey)) {
            handle(event.get("ce_type"), record.key(), record.value());
            markProcessed(eventKey);
        }
    }
    ```

    Spring Kafka commits the offset after the listener returns without an exception, which is the
    order you want.

=== "Python"

    ```python
    import json
    from confluent_kafka import Consumer, KafkaException

    consumer = Consumer({
        "bootstrap.servers": "localhost:9092",
        "group.id": "order-projection",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumer.subscribe(["order-topic"])


    def attributes(message):
        return {name: value.decode("utf-8") for name, value in (message.headers() or [])}


    while True:
        message = consumer.poll(1.0)
        if message is None:
            continue
        if message.error():
            raise KafkaException(message.error())

        event = attributes(message)
        event_key = (event["ce_source"], event["ce_id"])
        if not already_processed(event_key):
            handle(event["ce_type"], message.key().decode(), json.loads(message.value()))
            mark_processed(event_key)
        consumer.commit(message)
    ```

    Needs `pip install confluent-kafka`. `message.headers()` is a list of name and bytes pairs, or
    `None` when there are none.

`alreadyProcessed`, `markProcessed` and `handle` are yours. The next section is about the first two.

---

## 4. Idempotency

Delivery is **at least once**. A relay that stops between Kafka's acknowledgement and recording it, a
lease that expires mid-send, a consumer rebalance and an operator's replay all deliver an event again.
Seen from the consumer that is not an error: it is the rule, so the consumer has to produce the same
result whether it sees an event once or five times.

There are three ways, from the simplest.

### 4.1 Make the operation naturally idempotent

If applying the event twice changes nothing, you need no bookkeeping. Setting a field to a value, an
upsert by aggregate id, or a `DELETE` are all idempotent. A counter increment is not.

### 4.2 Remember the events you processed

Record each event's identity in the **same transaction** as its effect, and let a unique constraint
reject the repeat.

```sql
CREATE TABLE processed_event (
    source   text   NOT NULL,
    event_id text   NOT NULL,
    PRIMARY KEY (source, event_id)
);
```

```sql
-- inside the transaction that applies the event's effect:
INSERT INTO processed_event (source, event_id) VALUES (?, ?) ON CONFLICT DO NOTHING;
-- 0 rows inserted: already processed, skip the effect and commit
```

Doing both in one transaction is what closes the gap: a crash cannot leave the effect applied and the
event unrecorded, or the reverse. Keep the identity as `(source, id)`, not `id` alone (§2).

### 4.3 Use the sequence number, when there is one

If the write side sends `ce_seq`, the aggregate's own version, you can keep the highest one applied per
aggregate and **ignore any event that is not higher**. This also drops a stale event that arrives late,
which the table above would not.

!!! note "A replay is deliberately a repeat"
    An operator's replay republishes the event with the **same `ce_id`**. A consumer that remembers what
    it processed will ignore it, which is correct when the goal is only to make sure every consumer
    got it. If you replay because a consumer **lost** the data and needs to process it again, clear that
    consumer's record of the events first.

---

## 5. Order

Kafka orders the records of **one partition**, and Tandem sends one aggregate's events to one partition
by the key. So order is preserved for an aggregate exactly as long as your consumer handles that
aggregate's events **one after another**.

- **Process a partition sequentially,** or, if you parallelize, hand every event with the same key to
  the same worker.
- **Commit offsets in order,** after processing. A consumer group rebalance redelivers whatever was not
  committed, which is a duplicate, and your idempotency covers it.
- **Do not assume an order across aggregates,** and do not build logic that needs one.
- If you add partitions to a topic, keys map to new partitions and an aggregate's older and newer events
  can sit on different ones. See [Advanced Integration](advanced-integration.md#22-the-key-and-what-it-means-for-partitions).

---

## 6. When processing an event fails

Tandem delivers the event; what happens next is your consumer's decision, and Tandem cannot make it for
you. The choices are the usual ones for a Kafka consumer:

- **Retry in place**, with a backoff, for a failure that is likely to pass (a dependency is briefly
  down). Stop consuming that partition while you retry: moving on would process later events of the same
  aggregate before this one.
- **Park it on a dead-letter topic** and continue, for an event that will never succeed (malformed,
  or failing a business rule). Do it deliberately: the aggregate's later events now run without this one,
  which is the same trade as discarding a failed event on the producer side.
- **Alert** on both, so a stuck partition is noticed.

Whichever you choose, keep the event's `ce_source`, `ce_id`, `ce_type` and key with whatever you park,
and its `correlation-id` and `traceparent` if present, so it can be traced and replayed.

---

## 7. Continuing the trace

When the write side has [tracing enabled](observability.md#43-turning-them-on), each record carries a
standard W3C `traceparent` header. A consumer instrumented with OpenTelemetry or Micrometer Tracing
continues the producing operation's trace by itself, with nothing Tandem-specific to configure. Without
that, read the `correlation-id` header and put it in your logging context, so your logs join the same
business operation (see [Observability](observability.md#1-three-signals-three-questions)).

---

## 8. Events change over time

Producers evolve additively, and you should read defensively to match.

- **Ignore unknown headers and unknown payload fields.** They are how the contract grows without
  breaking you.
- **The event type carries the version.** A change that cannot be additive is published as a new type
  (a suffix such as `.v2` on the type), so old and new consumers keep working side by side while you
  migrate.
- **Handle a missing optional attribute.** `ce_seq`, `traceparent` and `correlation-id` may be absent.

---

## 9. Checklist

- [ ] Offsets are committed **after** the event is processed.
- [ ] The identity of an event is `(ce_source, ce_id)`, never `ce_id` alone.
- [ ] Processing the same event twice gives the same result, and there is a test for it.
- [ ] Events of one aggregate are handled in order; nothing depends on order across aggregates.
- [ ] The consumer ignores headers and payload fields it does not know.
- [ ] Nothing depends on the payload's exact bytes.
- [ ] A permanently failing event has a defined path (retry, park, alert), not an endless loop.
