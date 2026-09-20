# Advanced Integration

---

## 0. Who this page is for

The basics are in place and you want to go further: control exactly how events reach Kafka, write
events in other ways than a plain insert, work with the outbox from your own code, or replace one of
Tandem's defaults with something of yours.

!!! question "The question this page answers"
    **Which knobs and extension points does Tandem offer once the defaults stop being enough, and how
    do I use each one?**

Each section stands on its own; read the ones you need.

---

## 1. The Kafka producer

The relay publishes through an ordinary Kafka producer, and you configure it with ordinary Kafka
properties: under `tandem.kafka.producer.*` in Spring, or as the `Map` given to `KafkaRelay` in plain
Java.

```yaml
tandem:
  kafka:
    source: /orders/service
    producer:
      bootstrap.servers: kafka-1:9093,kafka-2:9093
      security.protocol: SASL_SSL
      sasl.mechanism: PLAIN
      sasl.jaas.config: "org.apache.kafka.common.security.plain.PlainLoginModule required username='...' password='...';"
      ssl.truststore.location: /etc/tandem/truststore.jks
      compression.type: lz4
```

Anything the Kafka client understands is passed through as it is: security, compression, timeouts,
client id. Keep credentials in your secret store and inject them as you would for any other Kafka
client; Tandem never logs them.

### 1.1 The settings Tandem protects

Delivery guarantees rest on a few producer settings, so Tandem sets them itself and **refuses to
start** (with a `TandemConfigurationException` naming the setting) if you weaken them.

| Property | Tandem's value | Why it is protected |
|---|---|---|
| `enable.idempotence` | `true`. Setting `false` is refused. | The producer's own retries must not duplicate or reorder a batch. |
| `acks` | `all`. Anything other than `all` or `-1` is refused. | A row is marked `DONE` only after a durable acknowledgement. `0` or `1` could acknowledge before the write is replicated, and lose the event. |
| `max.in.flight.requests.per.connection` | At most `5`. A higher value is refused. | Idempotent ordering needs it. |
| `retries` | A very large number by default. | Transient errors are retried by the producer, within the time limit below. |
| `delivery.timeout.ms` | `30s` unless you set it. | Caps how long the producer keeps retrying one send. It must stay **below** the relay's `row-lease` (see [Reliability](reliability.md#25-a-relay-that-crashes-mid-send)). |

Two more details. The key and value serializers are fixed (a string key and a byte array value, which
is what the CloudEvents binding produces) and are not yours to set. And when you do not set
`delivery.timeout.ms`, Tandem raises its default as far as Kafka's own rule requires (the timeout must
be at least `linger.ms + request.timeout.ms`), so a Kafka client upgrade cannot break producer
construction.

---

## 2. Topics, keys and partitions

### 2.1 Which topic

By default the topic comes from the **aggregate type**: kebab-cased, plus a suffix.

| Aggregate type | Topic (default suffix `-topic`) |
|---|---|
| `Order` | `order-topic` |
| `OrderLine` | `order-line-topic` |

Change the suffix with `tandem.kafka.topic-suffix`. To route differently, supply your own
**`TopicRouter`**: a function from the stored row to a topic name.

=== "Spring Boot"

    Define a bean; it replaces the default.

    ```java
    @Bean
    TopicRouter topicRouter() {
        return record -> switch (record.aggregateType()) {
            case "Order", "OrderLine" -> "orders";
            case "Customer" -> "customers";
            default -> record.aggregateType().toLowerCase() + "-events";
        };
    }
    ```

=== "Plain Java"

    ```java
    TopicRouter router = record -> record.aggregateType().equals("Order") ? "orders" : "other-events";

    OutboxDispatcher kafka = new KafkaRelay(producerConfig, router, KafkaRelayConfig.of("/orders/service"));
    ```

The router receives the whole stored row, but routes by **aggregate type**, not by the finer event
type, on purpose: the topic is a property of the kind of thing, not of what happened to it. A router
that throws makes the row fail permanently.

### 2.2 The key, and what it means for partitions

The Kafka **key is always the aggregate id**. That is what puts all events of one aggregate on one
partition, in order, while different aggregates spread over the partitions. Two consequences follow.

- **Tandem does not create topics.** Create them with the partition count your consumers need, or rely
  on your cluster's auto-creation.
- **Adding partitions later changes which partition a key maps to.** This is how Kafka works, not
  something Tandem controls: after the change, an aggregate's new events can land on a different
  partition from its old ones, and a consumer reading in parallel may see the two interleaved. Choose
  the partition count with room to grow, or plan the change.

---

## 3. Writing several events at once

`OutboxRepository` has a second method for a transaction that produces more than one event:

```java
outbox.insertAll(List.of(placed, reserved, notified));
```

It inserts them in order, in your transaction. If any message asks for the write lock
(`lockedWrite()`), the locks the batch needs are taken **in sorted aggregate-id order** before any row
is inserted, which prevents two such transactions from deadlocking each other. Across separate
`insert` calls in one transaction that ordering is yours to keep.

---

## 4. The other ways to write an event

[Getting Started](getting-started.md#52-insert-it-inside-your-transaction) shows the explicit insert and
the template. There are two more, for code that already has a shape they fit. The
[Adoption Guide](adoption.md#31-which-write-side-tier) helps you choose; this is how each one works.
All of them insert in **your transaction**, and all use the same messages as before.

### 4.1 `@TransactionalOutbox`: the aggregate carries its events

Your aggregate implements `TandemAggregate`, exposing the messages its mutations produced. A method
annotated `@TransactionalOutbox` opens a transaction, and after the method returns, Tandem reads the
pending messages of the aggregate it returned and inserts them, still inside that transaction.

```java
final class Order implements TandemAggregate {

    private final List<OutboxMessage> pending = new ArrayList<>();

    void place() {
        pending.add(OutboxMessage.builder()
            .aggregateType("Order")
            .aggregateId(id)
            .type("com.acme.order.placed")
            .unsequenced()
            .payload(json(this))
            .contentType("application/json")
            .build());
    }

    @Override
    public Collection<OutboxMessage> pendingOutboxMessages() {
        return pending;
    }
}

@Service
class OrderService {

    @TransactionalOutbox
    public Order place(String id) {
        Order order = store.load(id);
        order.place();
        store.save(order);
        return order;                 // the annotation extracts and inserts its events
    }
}
```

The annotation is a `@Transactional` with every attribute you would expect (`rollbackFor`,
`isolation`, `timeout`), so a method never needs both. It also accepts an optional
`aggregateType = "Order"`, which fails the insert if a message carries a different one. The method may
return one aggregate or an `Iterable` of aggregates. The annotation needs Spring AOP and AspectJ on the
classpath, which your Boot version's AOP starter provides.

Build each aggregate's pending list **fresh for the operation**: a list that keeps events already
inserted would insert them again.

### 4.2 Spring application events: existing `publishEvent` calls

If your code already publishes domain events with Spring's `ApplicationEventPublisher`, register an
`OutboxEventMapper` for the event type and change nothing at the call site.

```java
record OrderNoted(String orderId) {}

@Component
class OrderNotedMapper implements OutboxEventMapper<OrderNoted> {

    @Override
    public Collection<OutboxMessage> map(OrderNoted event) {
        return List.of(OutboxMessage.builder()
            .aggregateType("Order")
            .aggregateId(event.orderId())
            .type("com.acme.order.noted")
            .unsequenced()
            .payload(json(event))
            .contentType("application/json")
            .build());
    }
}

@Transactional
public void note(String id) {
    events.publishEvent(new OrderNoted(id));      // becomes an outbox row, in this transaction
}
```

The mapper is what opts an event type in: an event with no mapper is left alone, so unrelated events
are never touched. A mapper may return several messages for one event. You can also publish an
`OutboxMessage` itself as the event. The listener runs inline, so it **requires an active
transaction** and fails immediately if there is none, rather than inserting under autocommit.

---

## 5. Object payloads and the serializer

The template and the collector's `record(...)` methods take an **object** and serialize it for you,
through a `PayloadSerializer`. The interface is two methods:

```java
public interface PayloadSerializer {
    byte[] serialize(Object payload);        // must produce valid JSON
    String contentType();                    // stored as the row's content type
}
```

Tandem contributes a Jackson serializer, but only when Jackson 2's `ObjectMapper` is on the classpath,
and it reuses your application's `ObjectMapper` bean when there is one. That matters on **Spring Boot
4**: a stock Boot 4 application brings Jackson 3, which is a different library, so unless you add
Jackson 2 back (`spring-boot-jackson2`) there is no default serializer. Then either build the message
yourself and pass the bytes, or supply your own serializer as a bean:

```java
@Bean
PayloadSerializer payloadSerializer(tools.jackson.databind.json.JsonMapper mapper) {
    return new PayloadSerializer() {
        public byte[] serialize(Object payload) { return mapper.writeValueAsBytes(payload); }
        public String contentType() { return "application/json"; }
    };
}
```

A bean of your own always wins over the default. In plain Java there is no default at all: pass bytes
straight to the message builder, or implement this interface around whichever library you use.

---

## 6. Working with the outbox from your own code

The [Admin API](admin-api.md) and the [CLI](cli.md) are the operator's tools. When your own code needs
the same operations, the services behind them are plain classes in `tandem-jdbc`, built from a
`DataSource`, with no HTTP in between. Use a plain, non-transaction-aware `DataSource` here: these are
administrative operations, not part of a business transaction.

```java
ReplayService replay = new JdbcReplayService(dataSource);
DiscardService discard = new JdbcDiscardService(dataSource);
OutboxQuery query = new JdbcOutboxQuery(dataSource);
```

**Read the outbox:**

```java
Map<OutboxStatus, Long> counts = query.statusCounts();

List<OutboxRowView> failed = query.search(OutboxSearchCriteria.builder()
    .status(OutboxStatus.FAILED)
    .aggregateType("Order")
    .limit(100)
    .build());

Optional<OutboxRowDetail> detail = query.findById(7);       // with payload and headers
```

`search` returns rows in ascending id order. To read the next page, pass the last id you received as
`afterId(...)` in the next criteria.

**Replay:**

```java
ReplayResult preview = replay.replay(new ReplayCriteria(
    null, "Order", null, null, Set.of(OutboxStatus.FAILED), true));   // dryRun = true
System.out.println(preview.matched() + " rows would be replayed");
```

`ReplayCriteria` takes an aggregate id, an aggregate type, an id range and a set of statuses, and
requires at least one of them; it cannot be built empty. The last argument is `dryRun`.

**Discard** a `FAILED` row, with the same effect and the same warning as through the API (it skips the
event and breaks that aggregate's ordering for good):

```java
boolean discarded = discard.discard(7, "obsolete event");   // false if it is not FAILED
```

These services do the work and nothing more. The Admin API adds an audit log line with the acting user
around each write; calling the services directly does not, so log what you do.

---

## 7. Replacing a default

Most of Tandem's behavior sits behind a small interface with a default you can swap. In Spring, define a
bean of the type and the default steps aside. In plain Java, hand your implementation to the
constructor that takes it.

| Extension point | What it is for | Spring | Plain Java |
|---|---|---|---|
| `TopicRouter` | Which topic a row goes to (§2). | Bean | `KafkaRelay` constructor |
| `PayloadSerializer` | Object to JSON bytes (§5). | Bean | Yours to call |
| `MessageEncoder` | The whole message format, see [Message Format](message-format.md). | Bean | `KafkaRelay` constructor |
| `TandemMetrics` | Where the relay's measurements go. | Bean | `WorkerPool` constructor |
| `TracePropagator` | What is captured at write time for tracing. | Bean | `JdbcOutboxRepository` constructor |
| `TandemSpanRecorder` | The relay's publish span. | Bean | `KafkaRelay` constructor |
| `BackoffStrategy` | The wait between retries. | No property | `WorkerPool` constructor |

**Your own metrics.** `TandemMetrics` has a default (empty) method for each measurement, so you
override only what you want, **and `isEnabled()` must return `true`**: the relay skips every metric call
while it is false.

```java
class MyMetrics implements TandemMetrics {
    @Override public boolean isEnabled() { return true; }
    @Override public void recordLagAgeSeconds(double age) { myGauge.set(age); }
    @Override public void incrementRetry() { myCounter.increment(); }
}
```

If you use Micrometer, `tandem-micrometer` already does this (see
[Observability](observability.md#3-metrics)).

**Your own retry wait.** `BackoffStrategy` is one method, from the number of attempts made so far to a
delay. The default is the full-jitter ladder of [Reliability](reliability.md#21-one-delivery-step-by-step):

```java
BackoffStrategy steady = attempts -> Duration.ofSeconds(Math.min(60, 2L << attempts));

WorkerPool relay = new WorkerPool(store, kafka, cfg, TandemMetrics.NOOP, Clock.systemUTC(),
    steady, BucketSource.embedded(cfg.bucketCount()));
```

**The ordering detector.** The relay watches the order it publishes each aggregate's events in, and
reports a write side that broke the ordering rule (an `ERROR` and a metric). It is on by default
because that defect is otherwise invisible. Switch it off only where writers to one aggregate are
serialized by construction:

```yaml
tandem:
  relay:
    order-violation-detection: false
```

Its reach is limited (it forgets what it published across a relay restart, a bucket rebalance and
after tracking a very large number of aggregates), so a quiet detector is not proof of a correct write
side. That is covered in [Observability](observability.md#34-what-the-metrics-cannot-tell-you).
