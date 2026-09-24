# Publishing to RabbitMQ

---

## 0. Who this page is for

Your service runs on PostgreSQL and **RabbitMQ**, and you want the events it emits to reach an
exchange reliably and in order, without moving to Kafka. The write side, the schema and the relay
engine are exactly the ones [Getting Started](getting-started.md) describes; only the last hop
changes. This page covers that hop: the dependency, the wiring, what lands on the wire, and the one
guarantee that needs your help on the consumer side.

!!! question "The question this page answers"
    **What changes when the relay publishes to RabbitMQ instead of Kafka, and what do I have to set
    up myself?**

---

## 1. What is the same, and what is not

| | Kafka (`tandem-kafka`) | RabbitMQ (`tandem-rabbitmq`) |
|---|---|---|
| Write side, schema, relay engine | Same | Same |
| Envelope | CloudEvents, `ce_` headers | CloudEvents, `cloudEvents_` headers (the AMQP binding) |
| Where an event goes | Topic `order-topic` for `Order` | Routing key `order-topic` on one configured exchange |
| The acknowledgement | `acks=all` | Publisher confirm, persistent delivery, `mandatory` |
| Order per aggregate, published | Yes | Yes |
| Order per aggregate, **consumed** | Yes, by partition | Only with the topology of §6 |
| Spring Boot autoconfiguration | Yes | No: you contribute one bean (§4.1) |
| Version | Follows the BOM | Its own, outside the BOM (§2) |

---

## 2. Add the dependency

`tandem-rabbitmq` is **versioned independently of the library**: it is not in `tandem-bom`, so it
always takes an explicit version. Use the current one from
[Maven Central](https://central.sonatype.com/artifact/com.codingful/tandem-rabbitmq) in place of
`a.b.c` below (`x.y.z` is the BOM's, as in
[Getting Started](getting-started.md#3-step-1-add-the-dependencies)). It requires Tandem **0.10.0 or
later**: keep `tandem-bom` at that version or above. With Maven this matters, because the BOM's
version overrides the one the connector asks for, even when the BOM's is older.

=== "Spring Boot"

    === "Gradle"

        ```kotlin
        dependencies {
            implementation(platform("com.codingful:tandem-bom:x.y.z"))
            implementation("com.codingful:tandem-spring-producer")      // the write
            implementation("com.codingful:tandem-spring-relay")         // the relay
            implementation("com.codingful:tandem-rabbitmq:a.b.c")       // publishing to RabbitMQ
        }
        ```

    === "Maven"

        ```xml
        <dependencies>
          <dependency>
            <groupId>com.codingful</groupId>
            <artifactId>tandem-spring-producer</artifactId>  <!-- the write -->
          </dependency>
          <dependency>
            <groupId>com.codingful</groupId>
            <artifactId>tandem-spring-relay</artifactId>     <!-- the relay -->
          </dependency>
          <dependency>
            <groupId>com.codingful</groupId>
            <artifactId>tandem-rabbitmq</artifactId>         <!-- publishing to RabbitMQ -->
            <version>a.b.c</version>
          </dependency>
        </dependencies>
        ```

        With `tandem-bom` imported in `<dependencyManagement>`, as in
        [Getting Started](getting-started.md#3-step-1-add-the-dependencies).

    Do not add `tandem-kafka`: `tandem-spring-relay` does not bring it, and without it no Kafka
    client lands on your classpath.

=== "Plain Java"

    === "Gradle"

        ```kotlin
        dependencies {
            implementation(platform("com.codingful:tandem-bom:x.y.z"))
            implementation("com.codingful:tandem-jdbc")                 // the write, and the relay engine
            implementation("com.codingful:tandem-rabbitmq:a.b.c")       // publishing to RabbitMQ
        }
        ```

    === "Maven"

        ```xml
        <dependencies>
          <dependency>
            <groupId>com.codingful</groupId>
            <artifactId>tandem-jdbc</artifactId>      <!-- the write, and the relay engine -->
          </dependency>
          <dependency>
            <groupId>com.codingful</groupId>
            <artifactId>tandem-rabbitmq</artifactId>  <!-- publishing to RabbitMQ -->
            <version>a.b.c</version>
          </dependency>
        </dependencies>
        ```

The connector brings `com.rabbitmq:amqp-client`. It belongs only where the relay runs: a service that
only writes events needs neither it nor any other broker client.

---

## 3. Prepare the broker

**Tandem declares no topology**: no exchange, no queue, no binding. Which queues exist and how they
are bound is your design, and often predates Tandem. Before the relay starts:

1. **Create the exchange** the relay publishes to. The relay checks that it exists at startup and
   refuses to start otherwise, so a typo in its name is one startup failure, not one lost event per
   row.
2. **Bind a queue for every routing key** you publish. With the defaults, an event of aggregate type
   `Order` goes out with routing key `order-topic`, `OrderLine` with `order-line-topic`.

An event whose routing key matches no binding is **not** dropped silently: the broker returns it, and
the row fails permanently (§7) with the routing key in the log.

---

## 4. Start the relay

### 4.1 Spring Boot

There is no `tandem.rabbitmq.*` property. Contribute the dispatcher as a bean, and the relay
autoconfiguration publishes through it; everything else (`tandem.outbox.*`, `tandem.relay.*`) is
configured as usual.

```java
@Configuration
class RelayConfiguration {

    @Bean
    RabbitRelay outboxDispatcher(ObjectProvider<TandemSpanRecorder> spans) {
        ConnectionFactory factory = new ConnectionFactory();   // com.rabbitmq.client
        factory.setHost("localhost");
        factory.setUsername("guest");
        factory.setPassword("guest");

        return new RabbitRelay(factory,
                RabbitRelayConfig.of("/orders/service", "tandem-events"),
                spans.getIfAvailable(() -> TandemSpanRecorder.NOOP));
    }
}
```

`RabbitRelay` is an `OutboxDispatcher`, so the relay picks it up by type, and it is `AutoCloseable`,
so Spring closes the connection on shutdown. The `ObjectProvider` hands over the publish span recorder
when tracing is enabled (see [Observability](observability.md)) and a no-op otherwise.

Read the connection settings from your own configuration rather than hard-coding them, as you would
for any other client. If the application also uses Spring AMQP, keep this `ConnectionFactory`
separate: the relay needs a confirm-mode channel of its own.

### 4.2 Plain Java

```java
int bucketCount = 256;   // the same value as the write side

RelayConfig relayConfig = RelayConfig.builder().bucketCount(bucketCount).build();

BucketCountGuard.check(dataSource, bucketCount);

ConnectionFactory factory = new ConnectionFactory();
factory.setHost("localhost");

OutboxStore store = new JdbcOutboxStore(dataSource, relayConfig.maxAttempts());
RabbitRelay rabbit = new RabbitRelay(factory, RabbitRelayConfig.of("/orders/service", "tandem-events"));

WorkerPool relay = new WorkerPool(store, rabbit, relayConfig);
relay.start();
// on shutdown:
relay.stop();
rabbit.close();
```

`RabbitRelay` and `RabbitRelayConfig` are in `com.codingful.tandem.rabbitmq`; `ConnectionFactory` in
`com.rabbitmq.client`.

### 4.3 The settings

`RabbitRelayConfig.of(source, exchange)` sets the two values without a default. The full record has
four more:

| `RabbitRelayConfig` component | Default in `of(...)` | What it is |
|---|---|---|
| `source` | none, required | The CloudEvents `source` of every event: a URI naming your application. |
| `exchange` | none, required | The exchange every event is published to. Must already exist (§3). |
| `routingKeySuffix` | `-topic` | Appended to the kebab-cased aggregate type to form the routing key. |
| `defaultContentType` | `application/json` | Used for a row that stored none. |
| `defaultDataSchema` | none | The CloudEvents `dataschema`, when you want one on every event. |
| `confirmTimeout` | 30 s | How long a publisher confirm may take (§7). Must stay below the relay's `rowLease` (60 s by default). |

Connection settings (host, credentials, TLS, virtual host) are the `ConnectionFactory`'s, not
Tandem's. Tandem switches on automatic connection recovery and leaves the rest to you.

---

## 5. What lands on the broker

Each event is one AMQP message:

- **exchange**: the configured one;
- **routing key**: the kebab-cased aggregate type plus the suffix, `order-topic` by default;
- **body**: your payload, exactly as inserted;
- **`contentType` property**: the event's content type;
- **`messageId` property**: the outbox row id;
- **delivery mode**: persistent;
- **headers**: the CloudEvents attributes as `cloudEvents_id`, `cloudEvents_type`,
  `cloudEvents_source`, `cloudEvents_time`, plus `cloudEvents_partitionkey` (the aggregate id), and
  any header you added yourself.

Consumers deduplicate on `cloudEvents_id`, exactly as Kafka consumers do on `ce_id`: delivery is
at-least-once on both transports. What a consumer owes the outbox in general is in
[Consuming Events](consuming-events.md); only the header prefix differs.

---

## 6. Keeping the order on the consumer side

**The relay publishes the events of an aggregate in the order they were written**, for the same reason
as on Kafka: it sends the next event of an aggregate only after the previous one is confirmed.

**What your consumers observe is another matter.** RabbitMQ has no partitions. Several consumers on
one queue receive messages concurrently, so two events of the same aggregate can be *processed* out
of order even though they were *published* in order. This is the one guarantee where RabbitMQ needs
your help. Pick one of these:

=== "Single active consumer (simplest)"

    Declare the queue with `x-single-active-consumer: true`. Only one consumer receives at a time,
    the others stand by, and order is preserved. Throughput is bounded by that one consumer. This is
    the right default when you do not know which to choose.

=== "Consistent-hash exchange (scales out)"

    Publish to an exchange of type `x-consistent-hash` (the `rabbitmq_consistent_hash_exchange`
    plugin) with the **aggregate id as routing key**, and bind several queues to it, one consumer
    each. Every aggregate lands on one queue, so it keeps its order while different aggregates run in
    parallel. This is the closest equivalent of a Kafka partition key.

    The default envelope routes by aggregate type, so this topology needs the router that routes by
    aggregate id:

    ```java
    RabbitRelayConfig config = RabbitRelayConfig.of("/orders/service", "tandem-events-hash");

    var relay = new RabbitRelay(factory, config,
            new CloudEventAmqpEncoder(RabbitRouter.byAggregateId(config.exchange()), config),
            TandemSpanRecorder.NOOP);
    ```

If the events of an aggregate can be applied in any order, you need neither.

---

## 7. When publishing fails

| What happens | What the relay does |
|---|---|
| The broker confirms the message | The row turns `DONE`. |
| The broker rejects it (`basic.nack`) | Retried, with the relay's usual backoff. |
| No binding matches the routing key | The row **fails permanently** and its aggregate stops. The log names the routing key; add the binding, then replay the row from the [Admin API](admin-api.md) or the [CLI](cli.md). |
| The connection or channel drops | Every unconfirmed row is retried after the client reconnects. |
| No confirm within `confirmTimeout` | Retried. The message may have arrived, which is why consumers deduplicate. |
| The exchange is missing, or the broker is unreachable, at startup | The relay does not start (`TandemConfigurationException`). |

The same row lifecycle, statuses and recovery tools apply as on Kafka: see
[Reliability and Topology](reliability.md) and [Troubleshooting](troubleshooting.md).

---

## 8. Another envelope

The CloudEvents envelope is the default, not a requirement. A custom format, or the raw passthrough
that publishes the payload with no envelope around it, is written once against `MessageEncoder` and
lifted onto AMQP with `RabbitMessageEncoder.from(encoder, exchange)`, unchanged from Kafka. How, and
what you owe your consumers when you do it:
[Publishing Your Own Message Format](message-format.md).

---

## 9. Upgrading the connector

The connector has its own version and its own release notes, published on the
[Releases](https://github.com/alirux/tandem-transactional-outbox-kafka/releases) page under the tags
`rabbitmq-v<version>`, beside the library's `v<version>`. Changing the BOM version does not change it,
and changing the connector does not require a library release.

Each connector release states the **oldest Tandem version** it works with. A connector upgrade may
raise it (only in a minor version, never in a patch); keep your BOM at that version or later. Which
versions move first in a mixed deployment is the same as for the library: see
[Testing and Upgrades](testing-and-upgrades.md#22-upgrading-step-by-step).
