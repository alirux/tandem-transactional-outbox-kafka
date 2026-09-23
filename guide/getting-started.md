# Getting Started

---

## 0. Who this page is for

You have a service on PostgreSQL and Kafka, and you want the events it emits to reach Kafka
reliably and in order. This page takes you from an empty project to events flowing, one step at a
time, with Spring Boot and without it.

It covers the path most services take: **PostgreSQL, Kafka, and the CloudEvents envelope Tandem
publishes by default**. On RabbitMQ, follow this page for the schema and the write, and
[Publishing to RabbitMQ](rabbitmq.md) for the dependency and the relay; other message formats are in
[Publishing Your Own Message Format](message-format.md). It
explains what to do, not how Tandem works inside; for that, read the
[design documents](https://github.com/alirux/tandem-transactional-outbox-kafka/tree/main/docs).

!!! question "The question this page answers"
    **What do I add to my service, in what order, so that an event written in a transaction ends up
    on a Kafka topic?**

---

## 1. The shape of an integration

Four pieces, whichever way you run it:

| Piece | What you do | Where it runs |
|---|---|---|
| The schema | Create the `tandem_*` tables once | Your database |
| The write | Insert one row per event, inside the transaction that changes your data | Your application |
| The relay | Start it; it reads the outbox and publishes to Kafka | Your application, or a separate process |
| The consumer | Read the topic, and tolerate a repeated event | Whoever consumes |

The write and the relay share **one setting**, the bucket count (§6.1). Everything else on the relay
side has a default.

The relay can live in the same application as the write or in its own process. Nothing below
changes: run the relay steps where you want it to run.

---

## 2. Before you start

- **Java 17 or later.**
- **PostgreSQL**, and a JDBC driver for it on your application's classpath. Tandem does not bring
  one. See [Database Compatibility](compatibility.md) for the versions and engines covered.
- **A Kafka cluster** your application can reach.
- **A transaction around the change you want to publish.** The outbox row must commit or roll back
  together with your data, so the write has to happen inside the transaction that modifies it.
- **An identifier for each aggregate** (an order id, a customer id), up to 255 characters. Events
  with the same identifier are delivered in the order they were written.

---

## 3. Step 1: add the dependencies

Import the BOM, then name the modules without versions. Use the current version from
[Maven Central](https://central.sonatype.com/artifact/com.codingful/tandem-core) in place of
`x.y.z`.

!!! tip "Publishing to RabbitMQ?"
    Take `tandem-rabbitmq` in place of `tandem-kafka`. It is versioned on its own, outside the BOM:
    [Publishing to RabbitMQ](rabbitmq.md#2-add-the-dependency) has the coordinates and the relay
    wiring.

=== "Spring Boot"

    === "Gradle"

        ```kotlin
        dependencies {
            implementation(platform("com.codingful:tandem-bom:x.y.z"))
            implementation("com.codingful:tandem-spring-producer")  // the write
            implementation("com.codingful:tandem-spring-relay")     // the relay
            implementation("com.codingful:tandem-kafka")            // publishing to Kafka
        }
        ```

    === "Maven"

        ```xml
        <dependencyManagement>
          <dependencies>
            <dependency>
              <groupId>com.codingful</groupId>
              <artifactId>tandem-bom</artifactId>
              <version>x.y.z</version>
              <type>pom</type>
              <scope>import</scope>
            </dependency>
          </dependencies>
        </dependencyManagement>

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
            <artifactId>tandem-kafka</artifactId>            <!-- publishing to Kafka -->
          </dependency>
        </dependencies>
        ```

    `tandem-spring-producer` and `tandem-spring-relay` work with Spring Boot 3.x and 4.x and leave
    Spring itself to your application's own versions. The relay wires whichever publish adapter is
    on the classpath, which is why `tandem-kafka` is declared next to it.

    In a service that only writes events and leaves the relay to another process, take
    `tandem-spring-producer` alone: it pulls no Kafka client.

=== "Plain Java"

    === "Gradle"

        ```kotlin
        dependencies {
            implementation(platform("com.codingful:tandem-bom:x.y.z"))
            implementation("com.codingful:tandem-jdbc")   // the write, and the relay engine
            implementation("com.codingful:tandem-kafka")  // publishing to Kafka
        }
        ```

    === "Maven"

        ```xml
        <dependencyManagement>
          <dependencies>
            <dependency>
              <groupId>com.codingful</groupId>
              <artifactId>tandem-bom</artifactId>
              <version>x.y.z</version>
              <type>pom</type>
              <scope>import</scope>
            </dependency>
          </dependencies>
        </dependencyManagement>

        <dependencies>
          <dependency>
            <groupId>com.codingful</groupId>
            <artifactId>tandem-jdbc</artifactId>   <!-- the write, and the relay engine -->
          </dependency>
          <dependency>
            <groupId>com.codingful</groupId>
            <artifactId>tandem-kafka</artifactId>  <!-- publishing to Kafka -->
          </dependency>
        </dependencies>
        ```

    In a service that only writes events, `tandem-jdbc` alone is enough: it pulls no Kafka client.

---

## 4. Step 2: create the schema

Tandem does not create its tables at startup. You apply them once, with the migration tool you
already use, like any other table of yours.

=== "Liquibase"

    The changelog ships inside the `tandem-jdbc` jar. Include it from your own master changelog:

    ```xml
    <include file="classpath:tandem/schema/postgres/changelog/db.changelog-master.xml"/>
    ```

=== "Flyway or plain SQL"

    Use [`schema/postgres/tandem-baseline.sql`](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/schema/postgres/tandem-baseline.sql)
    as one ordinary migration, for example `V<n>__tandem_baseline.sql`.

Both routes create the same tables. The schema only ever grows by appending, so upgrading Tandem
later means applying the new changesets and nothing else. Applying it to a database that already
holds data is covered in the
[Adoption Guide](adoption.md#4-applying-the-schema-to-a-database-that-already-has-data-in-it).

---

## 5. Step 3: write an event

### 5.1 What an event contains

An event is an `OutboxMessage`:

```java
OutboxMessage.builder()
    .aggregateType("Order")                 // which kind of thing; decides the topic
    .aggregateId(order.id().toString())     // which one; decides the ordering and the Kafka key
    .type("com.acme.order.placed")          // what happened; becomes the CloudEvents type
    .unsequenced()                          // see below
    .payload(jsonBytes)                     // the event body, as JSON bytes
    .contentType("application/json")
    .build();
```

| Field | Notes |
|---|---|
| `aggregateType` | Required. `Order` publishes to `order-topic`, `OrderLine` to `order-line-topic`. |
| `aggregateId` | Required, up to 255 characters. Every event with the same id is delivered in write order. |
| `type` | Optional. Falls back to `aggregateType` when absent. |
| `payload` | Required. **Must be valid JSON**, because the outbox column stores it as `jsonb`. It is read back semantically identical, not byte for byte, so do not sign or hash the bytes you insert. |
| `contentType` | Optional. `application/json` when left out. |
| `header(name, value)` | Optional. A header you add here reaches Kafka untouched. |

**The sequence mode is a required choice.** A message that states none fails to build, because the
choice fixes what consumers read and cannot be changed later without breaking them. When in doubt,
use `unsequenced()`: nothing is asked of your domain, and consumers deduplicate on the event id,
which is always present. The two alternatives, `seq(long)` and `managedSeq()`, are compared in the
[Adoption Guide](adoption.md#32-where-seq-comes-from-and-whether-you-need-tandems-lock).

### 5.2 Insert it inside your transaction

=== "Spring Boot"

    Inject `OutboxRepository`, which the producer module provides, and call `insert` in your
    `@Transactional` method:

    ```java
    @Service
    public class OrderService {

        private final OrderRepository orders;
        private final OutboxRepository outbox;

        public OrderService(OrderRepository orders, OutboxRepository outbox) {
            this.orders = orders;
            this.outbox = outbox;
        }

        @Transactional
        public Order place(PlaceOrder command) {
            Order order = orders.save(new Order(command));
            outbox.insert(OutboxMessage.builder()
                .aggregateType("Order")
                .aggregateId(order.id().toString())
                .type("com.acme.order.placed")
                .unsequenced()
                .payload(toJsonBytes(order))
                .contentType("application/json")
                .build());
            return order;
        }
    }
    ```

    If you would rather hand over an object than serialize it yourself, use
    `TransactionalOutboxTemplate`. It opens the transaction, runs your work, and inserts what you
    recorded, all in one call:

    ```java
    public Order place(PlaceOrder command) {
        return template.execute(outbox -> {
            Order order = orders.save(new Order(command));
            outbox.record("Order", order.id().toString(), new OrderPlaced(order.id(), order.total()));
            return order;
        });
    }
    ```

    The object is serialized with your application's Jackson `ObjectMapper`, so Jackson 2 must be
    on the classpath; if it is not, or you use another JSON library, register your own
    `PayloadSerializer` bean. Two more ways to write exist (`@TransactionalOutbox` and Spring
    application events); the
    [Adoption Guide](adoption.md#31-which-write-side-tier) helps you pick between all four.

=== "Plain Java"

    Build the repository once at startup, and call `insert` where you change your data:

    ```java
    int bucketCount = 256;

    BucketCountGuard.check(plainDataSource, bucketCount);   // once per process, at startup
    OutboxRepository outbox = new JdbcOutboxRepository(transactionAwareDataSource, bucketCount);

    // inside your transaction, after the domain write:
    outbox.insert(OutboxMessage.builder()
        .aggregateType("Order")
        .aggregateId(orderId)
        .type("com.acme.order.placed")
        .unsequenced()
        .payload(toJsonBytes(event))
        .contentType("application/json")
        .build());
    ```

    The classes are in `com.codingful.tandem.jdbc` and `com.codingful.tandem.core`.

    !!! danger "The DataSource must join your transaction"
        `insert` uses the connection its `DataSource` hands back, and never opens, commits or rolls
        back a transaction of its own. It also closes that connection when it is done. So the
        `DataSource` you give it must return **the connection your transaction is already using**,
        and ignore the close. A Jakarta EE, Quarkus or Micronaut container already injects a
        `DataSource` that behaves this way. With hand-managed JDBC, wrap your transaction's
        connection yourself. Hand it a plain pool instead and the row commits on its own connection,
        outside your transaction, which is exactly the dual write Tandem exists to remove.

    `BucketCountGuard` takes the plain, non-transaction-aware `DataSource`: it runs at startup, not
    inside a transaction.

!!! warning "One rule about concurrent writers"
    Tandem keeps the order your writes established; it does not create one. Two transactions
    changing the **same aggregate** at the same time must be serialized by something, usually a row
    lock or an optimistic version check on the aggregate. If your writers already do that, you are
    done. If they might not, read
    [the one precondition that can actually block you](adoption.md#1-the-one-precondition-that-can-actually-block-you)
    before going live.

---

## 6. Step 4: start the relay

The relay reads the outbox and publishes to Kafka. It needs a database connection, a Kafka
connection, and the `source` that identifies your application on every event.

=== "Spring Boot"

    With `tandem-spring-relay` on the classpath and a `DataSource` configured, the relay starts and
    stops with your application. Add the two settings it cannot guess:

    ```yaml
    tandem:
      kafka:
        source: /orders/service            # required: who produced these events
        producer:
          bootstrap.servers: localhost:9092
    ```

    The `DataSource` is your application's own (`spring.datasource.*`); Tandem never configures it.

    To run the relay in another process, deploy the same application image with
    `tandem.relay.enabled: false` where you only want to write.

=== "Plain Java"

    ```java
    int bucketCount = 256;   // the same value as the write side

    RelayConfig relayConfig = RelayConfig.builder().bucketCount(bucketCount).build();

    BucketCountGuard.check(dataSource, bucketCount);

    OutboxStore store = new JdbcOutboxStore(dataSource, relayConfig.maxAttempts());
    OutboxDispatcher kafka = new KafkaRelay(
        Map.of("bootstrap.servers", "localhost:9092"),
        TopicRouter.kebabWithSuffix("-topic"),
        KafkaRelayConfig.of("/orders/service"));

    WorkerPool relay = new WorkerPool(store, kafka, relayConfig);
    relay.start();
    // on shutdown:
    relay.stop();
    ```

    `KafkaRelay` and `KafkaRelayConfig` are in `com.codingful.tandem.kafka`; `WorkerPool` and
    `RelayConfig` in `com.codingful.tandem.jdbc`.

### 6.1 The settings you will touch

Every setting has a default except `source`. These are the ones a first deployment sets or checks:

| Spring property | Plain Java | Default | What it is |
|---|---|---|---|
| `tandem.kafka.source` | `KafkaRelayConfig.of(...)` | none, required | The CloudEvents `source` of every event: a URI naming your application. |
| `tandem.kafka.producer.*` | the `Map` given to `KafkaRelay` | empty | Ordinary Kafka producer properties: `bootstrap.servers`, security, compression. |
| `tandem.kafka.topic-suffix` | `TopicRouter.kebabWithSuffix(...)` | `-topic` | Appended to the kebab-cased aggregate type to name the topic. |
| `tandem.kafka.default-content-type` | | `application/json` | Used for a row that stored none. |
| `tandem.outbox.bucket-count` | `bucketCount(...)` on both sides | `256` | See below. |
| `tandem.relay.enabled` | | `true` | `false` loads the module without starting a relay. |

Tandem hardens the producer settings that protect ordering and delivery (idempotence, `acks`,
in-flight requests) and refuses an unsafe override at startup, so leaving them alone is the right
default.

!!! danger "The bucket count: set it once, on both sides, and never change it"
    `bucket-count` is stored in every row. The write and the relay **must use the same value**, and
    it must **never change after the first deployment**. Both sides check it at startup and fail
    loudly on a mismatch, rather than letting events sit in buckets nobody reads. The default is
    almost always right: leave it unset.

!!! note "Run one relay at a time"
    The default coordination, `SINGLE`, assumes one active relay. Starting a second one against the
    same database only duplicates work. Running several relays is a separate topic.

### 6.2 Topics

Tandem does not create Kafka topics. Create the topics your aggregate types map to (`order-topic`
for `Order` with the defaults), or rely on your cluster's auto-creation if it has it enabled. Give
them the partition count you want your consumers to scale with: the aggregate id is the Kafka key,
so one aggregate always lands on one partition.

---

## 7. Step 5: check that it works

Write one event, then look at both ends.

**In the database**, the row appears as `PENDING` and turns `DONE` once Kafka acknowledged it:

```sql
SELECT id, aggregate_id, type, status, attempts
FROM tandem_outbox
ORDER BY id DESC
LIMIT 10;
```

| `status` | Meaning |
|---|---|
| `0` | Waiting to be published |
| `1` | Being published right now |
| `2` | Published and acknowledged |
| `3` | Failed after all attempts; see §9 |

**In Kafka**, read the topic. Each record carries:

- **key**: the aggregate id;
- **value**: your payload, exactly as inserted;
- **headers**: the CloudEvents attributes as `ce_id`, `ce_type`, `ce_source`, `ce_time`, plus
  `ce_partitionkey` and `content-type`, and any header you added yourself.

If the row stays `PENDING`, the relay is not running or cannot reach Kafka, and its log says why.
How to route Tandem's logs to your backend is in the
[README](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/README.md#logging).

---

## 8. Step 6: write your consumer to tolerate repeats

Tandem delivers **at least once**. If the relay publishes an event and stops before recording that
it did, it publishes the event again after restarting. A duplicate is possible; a lost event and a
divergence between your database and Kafka are not.

So a consumer must be **idempotent**. The simplest way is to remember the CloudEvents id (`ce_id`)
of what it has processed and skip one it has seen. That id is unique per event and is the same on
every redelivery of that event.

Within one aggregate, events arrive in the order they were written. Across aggregates there is no
order, and there should not be: that is what lets them travel in parallel.

---

## 9. When something goes wrong

- **A row is `PENDING` and not moving.** The relay is down, cannot reach Kafka, or is retrying a
  failing publish. Its log line carries the outbox row id, the aggregate id and the topic.
- **A row is `FAILED` (`status = 3`).** Publishing it failed on every attempt. Only that aggregate
  waits behind it; every other aggregate keeps flowing, so a stuck event never stops the service.
  Fix the cause first (a missing topic, a payload the broker rejects); the row itself is then handled with the operations tooling.
- **Startup fails with a bucket count message.** The write and the relay disagree on
  `bucket-count`. Set the same value on both.

Inspecting and replaying rows is the job of the operations tooling, which has its own chapters.

---

## 10. Testing your own code

The `tandem-test` module holds what a test needs, and it is a test dependency only:

```kotlin
testImplementation("com.codingful:tandem-test")
```

- **`InMemoryOutbox`** implements the write port with no database, so a unit test can assert what
  your code inserted.
- **`TandemTestContainer`** starts a real PostgreSQL and a real Kafka with Testcontainers, for an
  integration test that goes from your write to a topic. Docker must be running.

The `tandem-sample` and `tandem-sample-spring` modules in the repository are runnable versions of
this page, one per column of the tabs above:

```bash
./tandem-sample/run.sh          # plain Java
./tandem-sample-spring/run.sh   # Spring Boot
```

Both start their own PostgreSQL and Kafka with Testcontainers, so Docker must be running. The Spring
sample also turns on the [Admin API](admin-api.md#5-a-worked-case-recovering-a-failed-event) and
leaves one row `FAILED` on purpose, so once its narration ends you can practise finding, replaying
and discarding it with `curl` or [`tandem-cli`](cli.md#4-a-worked-case-recovering-a-failed-event)
against a real outbox. It prints the exact commands, with that row's id.

---

## 11. Before you go to production

- [ ] The schema is applied by your migration tool.
- [ ] `bucket-count` is identical on the write and the relay, and you never change it.
- [ ] Every message states its sequence mode, and you know why (§5.1).
- [ ] Writers to one aggregate are serialized, or you have read
  [why that matters](adoption.md#1-the-one-precondition-that-can-actually-block-you).
- [ ] With plain Java, the insert joins your transaction; you have checked that a rollback after
  the insert leaves no row.
- [ ] The topics exist.
- [ ] Exactly one relay is running.
- [ ] Every consumer is idempotent.

---

## 12. Where to go next

- [Adoption Guide](adoption.md): fitting Tandem into an existing domain model and an existing
  Kafka producer.
- [Database Compatibility](compatibility.md): which engines and versions are covered.
