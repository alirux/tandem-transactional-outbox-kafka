# Testing and Upgrades

---

## 0. Who this page is for

You want to **test the code that uses Tandem**, at the speed of a unit test where you can and against
real infrastructure where you must, and you want to **upgrade Tandem** without surprises, including
when the write side, the relay and the Admin API do not move at the same moment.

!!! question "The question this page answers"
    **How do I test my use of the outbox, and what is the safe way to move to a new version?**

---

## 1. Testing

### 1.1 Three levels

Everything you need is in one test dependency:

```kotlin
testImplementation("com.codingful:tandem-test")
```

| Level | Uses | Needs Docker | Proves |
|---|---|---|---|
| **Your write code** | `InMemoryOutbox` | No | That your code inserts the right events. |
| **Relay behavior** | `InMemoryOutbox`, `RecordingDispatcher` and the real relay | No | Ordering, retries, blocked aggregates, without Kafka. |
| **End to end** | `TandemTestContainer` | Yes | That an event written in a transaction reaches a Kafka topic. |

None of them uses a mock. `InMemoryOutbox` is a working implementation of the outbox, written to
behave like the database one (it computes the same buckets and enforces the same rules), so a test
exercises real collaborators.

### 1.2 Testing your write code

`InMemoryOutbox` implements `OutboxRepository`, so you hand it to your service where production hands
in the JDBC one, then look at what was inserted.

```java
@Test
void placing_an_order_records_an_event() {
    InMemoryOutbox outbox = new InMemoryOutbox();
    OrderService service = new OrderService(orders, outbox);

    service.place(command);

    assertThat(outbox.all()).hasSize(1);
    OutboxRecord row = outbox.all().get(0);
    assertThat(row.aggregateType()).isEqualTo("Order");
    assertThat(row.type()).isEqualTo("com.acme.order.placed");
    assertThat(row.aggregateId().value()).isEqualTo(command.orderId());
}
```

`all()` returns every row, and `byStatus(...)` and `statusCounts()` filter and count them.

What this test does **not** prove is atomicity: there is no database transaction here. That is the job
of §1.5.

### 1.3 Testing how the relay behaves

Run the real relay engine against the in-memory outbox, with a dispatcher that records what it was
asked to publish instead of talking to Kafka. This is where you check ordering and failure handling in
milliseconds.

```java
InMemoryOutbox outbox = new InMemoryOutbox();
RecordingDispatcher dispatcher = new RecordingDispatcher();

outbox.insert(message("order-1", "placed"));
outbox.insert(message("order-1", "shipped"));
outbox.insert(message("order-2", "placed"));

WorkerPool relay = new WorkerPool(outbox, dispatcher,
    RelayConfig.builder().pollInterval(Duration.ofMillis(20)).workersPerInstance(2).build());
relay.start();
awaitUntil(() -> dispatcher.dispatchCount() == 3);
relay.stop();

assertThat(outbox.statusCounts().get(OutboxStatus.DONE)).isEqualTo(3);
```

`message(...)` above stands for a helper of yours that builds an `OutboxMessage`.
`dispatcher.dispatched()` returns the rows in the order they were sent. Events of `order-1` are always
`placed` then `shipped`; how they interleave with `order-2` is not defined, so do not assert it.
`awaitUntil` is a small polling helper of your own, since the relay works on its own threads.

**Making a delivery fail.** `RecordingDispatcher` can fail a chosen row, retriably or permanently, so
you can test the paths that matter most: a permanent failure blocks only its own aggregate.

```java
dispatcher.failRecord(1, false);         // row 1 fails permanently
// ... insert order-9 placed (row 1), order-9 shipped (row 2), order-8 placed (row 3), start the relay

// order-9 placed      -> FAILED
// order-9 shipped     -> PENDING, blocked behind it
// order-8 placed      -> DONE, unaffected
```

That is the guarantee of [Reliability](reliability.md#1-what-is-guaranteed-in-four-lines) in a test
you can run on every commit. `failRecord(id, retriable, times)` fails a row a set number of times and
then lets it through, to test retries, and `failAll(...)` and `succeedAll()` switch the whole
dispatcher. `manualCompletion()` lets you finish sends yourself, to test several in flight at once.

Two more helpers are worth knowing. `ControllableClock` lets a test move time forward with `advance(...)`, so backoff and
lease expiry are tested without waiting. `RecordingMetrics` is a `TandemMetrics` that keeps what it
was told, and is what you assert against to check that your own metrics adapter is reached.

### 1.4 End to end, with real PostgreSQL and Kafka

`TandemTestContainer` starts a real PostgreSQL and a real Kafka with Testcontainers, applies the
schema, and gives you the pieces to wire a full path. Docker (or Colima) must be running.

```java
try (TandemTestContainer tandem = new TandemTestContainer()) {
    tandem.start();

    OutboxRepository outbox = tandem.newRepository(256);
    tandem.createTopic("order-topic", 1);

    try (var consumer = tandem.newConsumer("test-group", "order-topic")) {
        consumer.poll(Duration.ofMillis(200));                      // join the group first

        WorkerPool relay = tandem.newRelay(
            RelayConfig.builder().bucketCount(256).build(),
            TopicRouter.kebabWithSuffix("-topic"),
            KafkaRelayConfig.of("/test"));
        relay.start();

        outbox.insert(message("order-1", "order.placed"));

        for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofSeconds(10))) {
            // record.key() is the aggregate id; the CloudEvents attributes are the ce_* headers
        }
        relay.stop();
    }
}
```

Integration tests are slow to start, so tag them (for example `@Tag("integration")`) and run them
separately from your unit tests. The PostgreSQL image is `postgres:16-alpine` by default; set the
`tandem.test.postgres.image` system property or the `TANDEM_TEST_POSTGRES_IMAGE` environment variable
to test against another version.

In a Spring application the usual route is a Testcontainers-backed `@SpringBootTest` with your own
schema migrations applied, so the test exercises your real configuration.

### 1.5 The tests only you can write

Tandem tests its own engine. Three properties depend on **your** code and deserve a test of their own:

- **Atomicity.** Force a failure after the outbox insert and check that nothing was left behind. This
  is the test that catches an insert that ran on a different connection from your transaction.

    ```java
    @Test
    void a_rolled_back_change_leaves_no_event() {
        assertThatThrownBy(() -> service.placeThenFail(command))
            .isInstanceOf(IllegalStateException.class);

        Integer rows = jdbc.queryForObject("SELECT count(*) FROM tandem_outbox", Integer.class);
        assertThat(rows).isZero();
    }
    ```

- **Ordering under concurrency.** Two transactions changing the same aggregate at once must publish in
  the order you intend. A green functional suite proves nothing here; the pattern is spelled out in the
  [Adoption Guide](adoption.md#1-the-one-precondition-that-can-actually-block-you).
- **Idempotent consumers.** Deliver the same event twice to each consumer and check the result is the
  same as delivering it once. At-least-once delivery makes duplicates a certainty over time.

---

## 2. Upgrades

### 2.1 Why a mixed-version window is safe

In a split deployment the write side, the relay and the Admin API can run at different Tandem versions
against the same database and the same topics, and even a single rolling deployment passes through such
a state. Tandem is designed for it: every contract it publishes evolves in one direction only.

| Contract | What changes | What never changes |
|---|---|---|
| **Database schema** (`tandem_*`) | New tables, and new columns that are optional or defaulted. | A column is never removed, renamed or retyped, and never made required. |
| **Kafka messages** (CloudEvents) | New optional headers and extension attributes. | The event `type`, the meaning of existing attributes, existing header names. |
| **Admin API** | New endpoints, fields, optional parameters. | Existing fields, and the `type` slug of a problem. |
| **CLI** | New commands and flags. | Command and flag names and meaning, and exit codes. |

Readers are tolerant in return: Tandem selects columns by name and ignores extra ones, and its Kafka
and REST readers ignore what they do not know. A change that would break either side waits for a new
major version (a versioned migration for the schema, `/v2` for the API).

This has a consequence for **your** consumers and clients: **ignore what you do not recognize**. A
consumer that rejects an unknown header, or a client that rejects an unknown JSON field, will break the
first time Tandem adds one.

### 2.2 Upgrading, step by step

1. **Read the release notes** on the
   [Releases](https://github.com/alirux/tandem-transactional-outbox-kafka/releases) page for every
   version you are skipping. A release that breaks something lists it under **Breaking**, with
   what you must do. While the library is `0.x`, a minor version can carry a breaking change, so
   read them even for a minor bump.
2. **Update the versions.** Change the BOM version once; every Tandem module in it follows. The
   exception is `tandem-rabbitmq`, which is outside the BOM with a version of its own: update it
   separately, and check that its release notes (tags `rabbitmq-v<version>`) accept the Tandem
   version you are moving to ([Publishing to RabbitMQ](rabbitmq.md#9-upgrading-the-connector)).
3. **Apply the schema first.** Migrations only add, so the schema of the new version is safe for the
   old code that is still running. The reverse is not true: new code may read a column that an old
   schema lacks. So the database goes first, then the components, in any order.

    - With **Liquibase**, `update` applies the changesets you do not have yet. Each release only
      appends a new one; a changeset that has shipped is never edited.
    - With **Flyway or plain SQL**, the changelog directory holds one file per schema version
      (`v1-baseline.sql`, `v2-...`). Apply the ones after your current version, as ordinary
      migrations.

4. **Roll out the components** (write side, relay, Admin API) one at a time, in any order. During the
   rollout, old and new versions work side by side.
5. **Check the relay is healthy**: it starts, `lag.age_seconds` stays low and `failed.count` stays at
   zero (see [Observability](observability.md#33-alerts-worth-having)).

### 2.3 A migration that builds an index

Some migrations create an index on `tandem_outbox`. An ordinary `CREATE INDEX` on PostgreSQL **blocks
writes to the table while it builds**, and the time it takes grows with the table. On a small table
you will not notice. On a large, busy one, that is a pause in every write path that touches the outbox.

Before applying a migration to a production database, open the file: the header comment says what it
does and why. If it builds an index, apply it in a quiet period. If you apply the migrations as plain
SQL, you can also run that statement yourself as `CREATE INDEX CONCURRENTLY` (outside a transaction),
which builds the same index without blocking writes.

### 2.4 Rolling back

Schema changes only add, and older components ignore what they do not know, so **rolling the
application code back to a previous version after a migration is safe**. Tandem provides no
down-migrations, so a migration itself is not reverted; with an additive schema you do not need to.

### 2.5 Spring Boot generations

`tandem-spring-producer` and `tandem-spring-relay` are one artifact for Spring Boot 3.x and 4.x. Moving
your application from Boot 3 to Boot 4 does not require a different Tandem artifact. The versions
Tandem is built and tested against are in the
[README](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/README.md#spring-boot-compatibility).
One thing does change between the generations, and it is not Tandem's: Boot 4 ships Jackson 3 by
default. The Admin API renders through whichever binding your application has, but the object-payload
serializer needs Jackson 2 (see [Advanced Integration](advanced-integration.md#5-object-payloads-and-the-serializer)).

### 2.6 The CLI

`tandem-cli` is versioned on its own, with tags of the form `cli-v<version>`, because what it depends
on is the Admin API's major version (`/v1`), not the library's release. A library upgrade that leaves
the API contract alone needs no CLI upgrade, and the reverse. `tandem-cli --version` prints its own
version and the Admin API version it speaks.
