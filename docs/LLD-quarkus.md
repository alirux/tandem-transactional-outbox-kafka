# Tandem LLD: Quarkus write-side extension (`quarkus-tandem-producer`)

**Version:** 0.1  
**Status:** Designed, not implemented. First release scope only: the plain and template tiers.  
**Implements:** [HLD-quarkus.md](HLD-quarkus.md) v0.4  
**Companion to:** [LLD-spring-producer.md](LLD-spring-producer.md); [LLD-spring-config.md](LLD-spring-config.md) §2;
[LLD-bucket-count-guard.md](LLD-bucket-count-guard.md); [LLD-rabbitmq.md](LLD-rabbitmq.md) §9;
[LLD-relay.md](LLD-relay.md); [HLD-tracing.md](HLD-tracing.md); [HLD-logging.md](HLD-logging.md)

This document specifies how the first release of the Quarkus extension is built: its repository and
modules, its classes and build steps, its startup sequence, its configuration, its exceptions and log
lines, its tests and CI, and how it is versioned and released on Quarkiverse. The extension's code lives
in its own repository in the Quarkiverse organization (`quarkiverse/quarkus-tandem`); its design lives
here, next to the HLD, and the extension's documentation links to both.

Every Quarkus API and build item named here exists in Quarkus 3.40.0 and was exercised on 3.40.x.

---

## 1. What the HLD already fixes

Not re-decided here; each point is implemented as the HLD states it.

- Write side only, two artifacts: `io.quarkiverse.tandem:quarkus-tandem-producer` (runtime) and
  `quarkus-tandem-producer-deployment` (HLD §4). The relay and the Admin API run as the `tandem-relay`
  image.
- The guarantee covers the business change and the outbox row on **one Agroal datasource with JTA
  transactions enabled** (HLD §5.1). One repository adapter carries every insert path and checks that
  startup has completed and that a JTA transaction is active (HLD §5.3). It never marks a transaction
  rollback-only (HLD §13).
- Startup order: migrations, then the bucket-count check outside any transaction, then writes; an
  earlier write is refused (HLD §5.2).
- First release: the plain and template tiers. The annotation and event tiers follow their own
  prototypes (HLD §5.4 to §5.6).
- Payload serialization: optional, on the application's `ObjectMapper` when `quarkus-jackson` is
  present; the application registers its payload types for native mode (HLD §5.7).
- Configuration: the shared `tandem.*` keys, the datasource key at build time and refused when it
  differs at runtime (HLD §6).
- The application owns its migrations: Liquibase includes Tandem's changelog from the jar, Flyway copies
  Tandem's files (HLD §6.3).
- Quarkiverse, versioned independently, floor on Tandem 1.0.0 (HLD §7). `main` on Quarkus 3.40 LTS with
  minimum 3.40.0 and Java 17; a `quarkus-4` branch (HLD §8).

---

## 2. Repository and modules

### 2.1 Layout

```
quarkus-tandem/
├── pom.xml                       quarkus-tandem-parent, on quarkiverse-parent
├── runtime/                      quarkus-tandem-producer
├── deployment/                   quarkus-tandem-producer-deployment
├── docs/                         quarkus-tandem-docs (Antora, docs.quarkiverse.io)
├── integration-tests/            quarkus-tandem-integration-tests
├── integration-tests-minimal/    quarkus-tandem-integration-tests-minimal
├── integration-tests-e2e/        quarkus-tandem-integration-tests-e2e
└── .github/
    ├── project.yml               release coordinates (§15.3)
    ├── dependabot.yml            §14.3
    └── workflows/                §14
```

Only `runtime` and `deployment` are deployed. The parent POM lists `docs` in a profile `docs` and the
three integration-test modules in a profile `it`, both active when `performRelease` is not `true`: the
hub's release runs with `-DperformRelease`, so they are built and tested on every ordinary build and
never published.

### 2.2 The parent POM

| Element | Value | Why |
|---|---|---|
| parent | `io.quarkiverse:quarkiverse-parent`, latest release | The hub's plugin configuration, formatting check and release profile |
| `maven.compiler.release` | `17` | `quarkiverse-parent` sets 21; Tandem's baseline and the 3.40 line's minimum are 17 (HLD §8). The `quarkus-4` branch removes the override |
| `quarkus.version` | the latest 3.40.x | What the build and the release are tested on; CI also runs the minimum (§14) |
| `tandem.version` | `1.0.0` | **The floor itself** (§15.2) |
| dependency management | `io.quarkus:quarkus-bom` imported; `com.codingful:tandem-core`, `tandem-jdbc`, `tandem-tracing-otel` at `${tandem.version}` | |
| plugin management | `quarkus-maven-plugin`, `quarkus-extension-maven-plugin` and the `quarkus-extension-processor` annotation processor at `${quarkus.version}`; surefire and failsafe with `java.util.logging.manager=org.jboss.logmanager.LogManager` | The extension conventions of Quarkus |

`tandem-bom` is not imported. Each Tandem coordinate is declared at the floor so that the POM the
extension publishes states the floor in plain words, and an application that imports `tandem-bom` at a
later version raises it without any exclusion.

### 2.3 Runtime module

| Dependency | Scope | Why |
|---|---|---|
| `com.codingful:tandem-jdbc` | compile | The JDBC adapter and the bucket-count guard; brings `tandem-core` |
| `io.quarkus:quarkus-arc` | compile | CDI |
| `io.quarkus:quarkus-agroal` | compile | The datasource |
| `io.quarkus:quarkus-narayana-jta` | compile | `@Transactional`, `QuarkusTransaction`, `TransactionManager` |
| `io.quarkus:quarkus-jackson` | **optional** | The default `PayloadSerializer` (§9) |
| `com.codingful:tandem-tracing-otel` | **optional** | The OpenTelemetry trace capture (§10); brings `opentelemetry-api` to the compile classpath only |

Optional dependencies do not reach the application: a service gets `quarkus-jackson` and
`tandem-tracing-otel` only by declaring them, and the extension then activates the matching beans
(§6.2). No JDBC driver is a dependency: the application declares `quarkus-jdbc-postgresql`, as for any
datasource.

The module carries `META-INF/quarkus-extension.yaml`: name *Tandem Producer*, description, keywords
(`outbox`, `transactional-outbox`, `kafka`, `postgresql`), categories `data` and `messaging`, the guide
URL on docs.quarkiverse.io, and status `stable` from 1.0.0. The `quarkus-extension-maven-plugin`
`extension-descriptor` goal binds it to the deployment artifact.

### 2.4 Deployment module

Compile: the runtime module, `quarkus-arc-deployment`, `quarkus-agroal-deployment` (which brings
`quarkus-agroal-spi` and its build items), `quarkus-narayana-jta-deployment`. Test: `quarkus-junit-internal`,
`assertj-core`, `quarkus-jdbc-postgresql-deployment`, `quarkus-liquibase-deployment`,
`quarkus-flyway-deployment` with `quarkus-flyway-postgresql`, `quarkus-jackson-deployment`, and
`org.testcontainers:testcontainers-postgresql` for the upgrade tests (§13.4).

### 2.5 Formatting

The hub's build checks formatting (Quarkus's formatter and import sorter) and fails on a file that does
not conform. The extension follows it rather than Tandem's own layout: it is the convention of every
repository in the organization, and the hub's CI enforces it.

---

## 3. Packages and public types

### 3.1 Packages

| Package | Contents | Promise |
|---|---|---|
| `io.quarkiverse.tandem.producer` | `TransactionalOutboxTemplate`, `OutboxCollector` | Public API, governed by the extension's semver (§15.1) |
| `io.quarkiverse.tandem.producer.runtime` | The recorder, the configuration mappings, the adapter and every bean implementation | **None.** Quarkus needs the recorder and the configuration mappings to be public; its `package-info` states that the package is internal and may change in any release, as `tandem-admin`'s `internal` package does (HLD-api-stability §3) |
| `io.quarkiverse.tandem.producer.deployment` | The build steps, the build-time configuration, the extension's build item | None; a deployment module never reaches application code |

The configuration **keys** are part of the contract (§8); the Java mapping interfaces that read them are
not. An application reads Tandem's settings through the keys, never through
`TandemOutboxRuntimeConfig`.

### 3.2 Public interfaces

Both mirror the signatures of `tandem-spring-producer` (LLD-spring-producer §3), so code written against
one reads the same against the other; they are separate types, since the Spring ones live in a module
built against Spring (HLD §5.4).

**`TransactionalOutboxTemplate`**, **provided**: the extension implements it, the application calls it.

```java
public interface TransactionalOutboxTemplate {
    <T> T execute(Function<OutboxCollector, T> work);
    default void executeWithoutResult(Consumer<OutboxCollector> work) { ... }
}
```

**`OutboxCollector`**, **provided**: handed to the work by the template, valid only during that
`execute` call, not thread-safe.

```java
public interface OutboxCollector {
    void add(OutboxMessage message);
    void record(String aggregateType, AggregateId aggregateId, long seq, Object payload);
    void record(String aggregateType, String aggregateId, long seq, Object payload);
    void record(String aggregateType, AggregateId aggregateId, Object payload);   // unsequenced()
    void record(String aggregateType, String aggregateId, Object payload);       // unsequenced()
}
```

Each interface's javadoc closes with the provided paragraph of `TransactionalOutboxTemplate` in
`tandem-spring-producer`: a minor version may add abstract methods, so an application implementing one
follows the extension's minor versions. Their methods carry `@param`, `@return` and `@throws` in full,
as ports do (AGENTS.md §Javadoc 5).

### 3.3 Beans the application sees

| Bean type | Provided by | The application may replace it |
|---|---|---|
| `OutboxRepository` | the adapter (§4), `@Singleton @Typed(OutboxRepository.class)` | **No.** It is the one insert path the checks of §4 depend on. An application bean of the same type makes injection ambiguous and fails the build, which is the intended outcome |
| `TransactionalOutboxTemplate` | `DefaultTransactionalOutboxTemplate`, `@Singleton @Typed(TransactionalOutboxTemplate.class)` | No; an application wanting another behaviour writes its own class |
| `PayloadSerializer` | `JacksonPayloadSerializer`, `@DefaultBean`, only with `quarkus-jackson` (§9) | **Yes**, by declaring a bean of that type |
| `TracePropagator` | the trace capture of §10, `@DefaultBean` | **Yes**, by declaring a bean of that type |

Both replaceable types are `tandem-core` SPIs (HLD-api-stability §4.1), so replacing them adds no
extension type to the contract. The adapter's `@Typed` hides its implementation class: the application
cannot inject `TandemOutboxRepository`, and the class stays internal.

---

## 4. The repository adapter

`io.quarkiverse.tandem.producer.runtime.TandemOutboxRepository`, package-private, `final`.

```java
@Singleton
@Typed(OutboxRepository.class)
final class TandemOutboxRepository implements OutboxRepository {
    private final TransactionManager transactionManager;
    private volatile OutboxRepository delegate;   // null until the startup step opens the outbox

    void openForWrites(OutboxRepository jdbcRepository);
    public void insert(OutboxMessage message);
    public void insertAll(Collection<OutboxMessage> messages);
}
```

**`insert` and `insertAll` run the same two checks, in this order, then delegate** to the
`JdbcOutboxRepository` the startup step built (§7):

1. **Open.** `delegate` is set. Otherwise `OutboxInsertException` (message M1, §11.2): the write arrived
   before the bucket count was checked.
2. **Active transaction.** `TransactionManager.getStatus()` is `Status.STATUS_ACTIVE`. Otherwise
   `OutboxInsertException` (M2) carrying the status code. This refuses both a call with no transaction
   (the row would autocommit) and a call in a transaction already marked rollback-only (the row could
   never commit). A `SystemException` from the transaction manager becomes `OutboxInsertException` (M3)
   with the cause.

The checks run before anything else, so a refused call issues no SQL and leaves the transaction as it
was. They apply to an empty batch too: the rule is about where the call is made, not what it carries.

**Delegation is direct.** The delegate receives the `AgroalDataSource` itself: inside an active
transaction Agroal hands out the connection enlisted in it for that datasource (HLD §5.1). No proxy is
added.

**Failures are rethrown as they are.** The adapter catches nothing from the delegate: a
`DuplicateSeqException`, an `OutboxInsertException` from SQL, a `PayloadSerializationException` reach
the caller unchanged, and the transaction is not marked (HLD §5.3).

**Opened once.** `openForWrites` is called by the startup step only. The field is `volatile` because the
startup thread sets it and request threads read it; it is never reset. In dev mode a live reload builds
a new application with a new adapter, so a reload re-runs the whole sequence of §7.

---

## 5. Template and collector

### 5.1 `DefaultTransactionalOutboxTemplate`

```java
@Singleton
@Typed(TransactionalOutboxTemplate.class)
final class DefaultTransactionalOutboxTemplate implements TransactionalOutboxTemplate {
    DefaultTransactionalOutboxTemplate(OutboxRepository outboxRepository,
                                       Instance<PayloadSerializer> payloadSerializer);

    public <T> T execute(Function<OutboxCollector, T> work) {
        Objects.requireNonNull(work, "work");
        return QuarkusTransaction.joiningExisting().call(() -> {
            CollectingOutboxCollector collector = new CollectingOutboxCollector(serializer());
            T result = work.apply(collector);
            outboxRepository.insertAll(collector.collected());
            return result;
        });
    }
}
```

- **Transaction.** `QuarkusTransaction.joiningExisting()` joins the caller's transaction or starts one,
  the meaning of `REQUIRED`. On a throwable it rolls back a transaction it started, or calls
  `setRollbackOnly()` on a joined one, and rethrows a `RuntimeException` as it is. That handling is
  Narayana's; the template adds none of its own.
- **Insert.** The collected messages go through the injected `OutboxRepository`, which is the adapter of
  §4, in one `insertAll` call, in collection order. Nothing is inserted if the work throws.
- **Serializer.** Resolved on each call from the `Instance`: unsatisfied gives a collector without a
  serializer; one bean gives that bean; several application beans make `get()` throw CDI's
  `AmbiguousResolutionException`, which names them. Resolving per call rather than at construction keeps
  the template independent of whether Jackson is present.

### 5.2 `CollectingOutboxCollector`

Package-private, `final`, one instance per `execute` call, an `ArrayList` of `OutboxMessage`.

- `add(message)` appends the message, refusing `null` with `NullPointerException("message")`.
- `record(type, id, seq, payload)` builds `OutboxMessage.builder().aggregateType(type).aggregateId(id)
  .payload(serializer.serialize(payload)).contentType(serializer.contentType()).seq(seq)`; the
  overloads without `seq` call `unsequenced()` instead. The `String` id overloads go through
  `AggregateId.of`.
- Without a serializer, every `record` throws `PayloadSerializationException` (M4) at the call, before
  anything is collected. `add` keeps working: a pre-built message needs no serializer (HLD §5.7).

A managed number (`managedSeq()`) and the writer lock (`lockedWrite()`) are reached by building the
message and passing it to `add`, as with the Spring collector.

---

## 6. Build steps

All in `io.quarkiverse.tandem.producer.deployment.TandemProducerProcessor`, package-private.

### 6.1 Build items

| Item | Kind | Produced by | Consumed by |
|---|---|---|---|
| `TandemDataSourceBuildItem` (the extension's own, package-private) | `SimpleBuildItem` carrying the datasource name | `outboxDataSource` | `openOutbox` |
| `FeatureBuildItem("tandem-producer")` | Quarkus | `feature` | the startup banner |
| `AdditionalBeanBuildItem` | ArC | `beans` | ArC |
| `ServiceStartBuildItem("tandem-producer")` | Quarkus | `openOutbox` | `StartupEvent` (ArC's lifecycle step) |
| `JdbcDataSourceBuildItem` (list) | Agroal SPI | Agroal | `outboxDataSource` |
| `JdbcDataSourceSchemaReadyBuildItem` (list) | Agroal SPI | `quarkus-flyway`, `quarkus-liquibase` | `openOutbox` |
| `BeanContainerBuildItem`, `SyntheticBeansRuntimeInitBuildItem` | ArC | ArC | `openOutbox` (`@Consume`) |
| `Capabilities` | Quarkus | Quarkus | `beans`, `openOutbox` |

### 6.2 `beans`

```java
@BuildStep
void beans(Capabilities capabilities, BuildProducer<AdditionalBeanBuildItem> beans)
```

Registers, by class name (the deployment module never loads the runtime classes):

| Class | When | Unremovable |
|---|---|---|
| `TandemOutboxRepository` | always | yes: the recorder looks it up programmatically |
| `DefaultTransactionalOutboxTemplate` | always | no: removed if the application never injects it |
| `TracePropagatorProducer` (§10) | always | yes: the recorder looks the `TracePropagator` up programmatically |
| `JacksonPayloadSerializerProducer` (§9) | `capabilities.isPresent(Capability.JACKSON)` | no: the template's `Instance` injection point keeps it |
| `OtelTracePropagatorProducer` (§10) | `capabilities.isPresent(Capability.OPENTELEMETRY_TRACER)` **and** `QuarkusClassLoader.isClassPresentAtRuntime("com.codingful.tandem.tracing.otel.OtelTracePropagator")` | no: the trace capture's `Instance` injection point keeps it |

**A class whose signatures name an optional library is registered only when that library is present.**
ArC reads every registered bean class, so a class naming `ObjectMapper` or `OpenTelemetry` that is
registered without the library fails the build. This is why the Jackson and OpenTelemetry pieces each
live in a producer class of their own, and why the always-registered trace capture never names an
OpenTelemetry type (§10).

### 6.3 `outboxDataSource`

```java
@BuildStep
TandemDataSourceBuildItem outboxDataSource(TandemOutboxBuildTimeConfig config,
                                           List<JdbcDataSourceBuildItem> dataSources)
```

The name is `config.datasource()`, or `DataSourceUtil.DEFAULT_DATASOURCE_NAME` when absent. Three
refusals, each a Quarkus `ConfigurationException` that stops the build (§11.2):

| Condition | Message |
|---|---|
| No `JdbcDataSourceBuildItem` has that name | B1, with the names configured |
| `DatabaseKind.isPostgreSQL(dbKind)` is false | B2, with the db kind |
| `isTransactionIntegrationEnabled()` is false (`quarkus.datasource.jdbc.transactions=disabled`) | B3 |

All three are build-time facts in Quarkus, so the mistake stops the build rather than a deployment
(HLD §5.1). A datasource whose JDBC part is disabled (`quarkus.datasource.jdbc=false`) produces no
`JdbcDataSourceBuildItem` and falls in the first case.

### 6.4 `openOutbox`

```java
@BuildStep
@Record(ExecutionTime.RUNTIME_INIT)
@Consume(BeanContainerBuildItem.class)
@Consume(SyntheticBeansRuntimeInitBuildItem.class)
ServiceStartBuildItem openOutbox(TandemProducerRecorder recorder,
                                 TandemDataSourceBuildItem dataSource,
                                 Capabilities capabilities,
                                 List<JdbcDataSourceSchemaReadyBuildItem> schemaReady)
```

Records `recorder.openOutbox(dataSource.name(), otelAdapterMissing)`, where `otelAdapterMissing` is
`OPENTELEMETRY_TRACER` present and the adapter class absent (§10.3), and returns
`new ServiceStartBuildItem("tandem-producer")`.

Each parameter is there for its ordering effect:

- `List<JdbcDataSourceSchemaReadyBuildItem>`: the step runs after **every** producer of the item, which
  `quarkus-flyway` and `quarkus-liquibase` emit once their migrations at start are done, for every
  datasource. A list parameter is satisfied by zero items, so the step still runs with no migration
  extension. The value itself is not read: Tandem waits for all migrations, not only those of its own
  datasource, which costs nothing and survives a migration of the Tandem tables placed on another
  datasource's changelog by mistake. Removing this parameter makes the step run before the migrations;
  the startup-order tests of §13.2 fail when it is removed.
- `@Consume(SyntheticBeansRuntimeInitBuildItem)`: Agroal's datasources are synthetic beans initialised
  at runtime init; the recorder needs them.
- `@Consume(BeanContainerBuildItem)`: the recorder looks beans up through `Arc.container()`.
- The returned `ServiceStartBuildItem`: ArC's lifecycle step consumes all of them before it fires
  `StartupEvent`, so `@Startup` beans and `StartupEvent` observers find the outbox open.

---

## 7. Recorder and startup sequence

`io.quarkiverse.tandem.producer.runtime.TandemProducerRecorder`, public (`@Recorder`), with constructor
injection of `RuntimeValue<TandemOutboxRuntimeConfig>` and `RuntimeValue<TandemTracingRuntimeConfig>`.

The extension does nothing at static init. At runtime init, once every migration has run (§6.4),
`openOutbox(dataSourceName, otelAdapterMissing)` runs these steps in order; any exception stops the
application's startup:

1. **The datasource key has not changed.** The runtime value of `tandem.outbox.datasource`, read with
   `ConfigProvider.getConfig().getOptionalValue(...)`, is absent, equal to the build-time name, or empty
   while the build-time name is the default datasource. Otherwise `TandemConfigurationException` (S1)
   naming both values (HLD §6.1).
2. **No transaction is active.** `TransactionManager.getStatus()` is `STATUS_NO_TRANSACTION`; otherwise
   `TandemConfigurationException` (S2); a `SystemException` becomes S3. The guard commits a transaction
   of its own, so it must never run inside one (HLD §5.2). Recorder code at runtime init never runs in a
   JTA transaction, so no test reaches this branch: it is a guard against a change in Quarkus's startup,
   left uncovered on purpose (AGENTS.md, coverage of defensive guards on framework inputs).
3. **The datasource is active.** `AgroalDataSourceUtil.dataSourceIfActive(name)`; an inactive datasource
   (`quarkus.datasource."name".active=false`) gives `TandemConfigurationException` (S4).
4. **The bucket count is checked.** `BucketCountGuard.check(dataSource, bucketCount)`
   (LLD-bucket-count-guard): it seeds `tandem_meta` on a fresh schema and fails on a different recorded
   value. Its exceptions are the library's and pass through unchanged: a mismatch is the library's
   `TandemConfigurationException`, a missing `tandem_meta` is a `TandemException` whose cause names the
   table, which the guide quotes (HLD §5.2).
5. **The JDBC repository is built.** `JdbcOutboxRepository.builder(dataSource, bucketCount)
   .wakeup(wakeup).tracePropagator(tracePropagator).build()`, the trace propagator being the
   `TracePropagator` bean looked up through `Arc.container()` (§10).
6. **The adapter is opened.** The `OutboxRepository` bean, cast to `TandemOutboxRepository`, receives the
   repository through `openForWrites`. Log line L1 at `INFO`.
7. **The tracing warning**, when it applies (§10.3): log line L2 at `WARN`.

Then Quarkus completes the remaining service starts and fires `StartupEvent`.

**Without a migration extension** (migrations run by another tool, or by an init job before the
application starts) no schema-ready item exists, the step runs when the beans are ready, and step 4
checks whatever schema the database holds.

---

## 8. Configuration

### 8.1 Roots and phases

| Mapping (package `…runtime` or `…deployment`) | Prefix | Phase | Module |
|---|---|---|---|
| `TandemOutboxRuntimeConfig` | `tandem.outbox` | `RUN_TIME` | runtime |
| `TandemTracingRuntimeConfig` | `tandem.tracing` | `RUN_TIME` | runtime |
| `TandemOutboxBuildTimeConfig` | `tandem.outbox` | `BUILD_TIME` | deployment |

Each is a `@ConfigMapping(prefix = ...)` interface with `@ConfigRoot(phase = ...)`. Quarkus accepts a
`BUILD_TIME` root that shares a prefix outside `quarkus.` with runtime roots: its build-time reader
ignores `<prefix>.**` for the build-time mapping when a runtime mapping holds the same prefix.
`BUILD_AND_RUN_TIME_FIXED` is not used, for the reason in HLD §6.2.

### 8.2 Keys

| Key | Type | Default | Phase | Meaning |
|---|---|---|---|---|
| `tandem.outbox.datasource` | `Optional<String>` | the default datasource | build time | The Agroal datasource the outbox rows are written to; the business tables belong on the same one |
| `tandem.outbox.bucket-count` | `int` | `256` | runtime | LLD-spring-config §2.1; identical in every writer and in the relay |
| `tandem.outbox.wakeup` | `Wakeup` (`none`, `pg-notify`) | `none` | runtime | LLD-spring-config §2.1; a setting of the writer and relay pair |
| `tandem.tracing.enabled` | `boolean` | `false` | runtime | Whether inserts capture trace context and the correlation id (§10) |
| `tandem.tracing.correlation-id-mdc-key` | `String` | `correlationId` | runtime | The MDC key the correlation id is read from |

The enum values come from SmallRye's kebab-case conversion of `Wakeup`, which gives `none` and
`pg-notify`, the values the Spring modules and the relay image accept. Validation of `bucket-count` is
the library's: the guard and the builder refuse a non-positive value with their own message, which keeps
one rule and one text across every process.

**Keys of other roles start the application as they are** (HLD §6.1). `tandem.relay.*` and
`tandem.kafka.*` lie outside every prefix the extension maps, so nothing reads or validates them.
`tandem.tracing.publish-span`, the relay's span switch, lies inside the `tandem.tracing` prefix, where
SmallRye's check that a mapping covers every name under its prefix
(`quarkus.config.mapping.validate-unknown`) could refuse it. A test pins the behaviour
(`OtherRoleKeysTest`, §13.2); if the check applies, `TandemTracingRuntimeConfig` declares
`publishSpan()` as an `Optional<Boolean>` whose description says the write side ignores it. The
extension never asks the application to turn that check off.

### 8.3 Documentation

The `quarkus-extension-processor` generates the configuration model of the three roots
(`META-INF/quarkus-config-doc/quarkus-config-model.json`) and the AsciiDoc tables the `docs` module
includes, with each key's environment variable (`TANDEM_OUTBOX_BUCKET_COUNT`, …). The javadoc of each
mapping method is the key's description there, so it is written for an operator, not for a Java reader.

---

## 9. The Jackson payload serializer

Two package-private classes in `…runtime`, registered only with `Capability.JACKSON` (§6.2):

```java
final class JacksonPayloadSerializer implements PayloadSerializer {
    JacksonPayloadSerializer(ObjectMapper objectMapper);
    byte[] serialize(Object payload);   // writeValueAsBytes; JsonProcessingException → PayloadSerializationException (M5)
    String contentType();               // "application/json"
}

@Singleton
final class JacksonPayloadSerializerProducer {
    @Produces @Singleton @DefaultBean
    PayloadSerializer tandemJacksonPayloadSerializer(ObjectMapper objectMapper);
}
```

The `ObjectMapper` is the application's own bean, with every `ObjectMapperCustomizer` it registered: a
payload serializes the way the application's REST responses do. `@DefaultBean` steps aside as soon as the
application declares any `PayloadSerializer` bean.

On the 3.40 line `quarkus-jackson` is Jackson 2 (`com.fasterxml.jackson.databind`); the `quarkus-4`
branch moves the two classes to Jackson 3 (HLD §5.7). Nothing else in the extension names Jackson.

**Native mode.** The extension registers no payload type for reflection (HLD §5.7, §13). With Quarkus's
default `ObjectMapper` an unregistered type fails serialization with `PayloadSerializationException`
(`FAIL_ON_EMPTY_BEANS` is on); an application that turns that feature off gets `{}` instead. The guide
states both outcomes and the two ways out.

---

## 10. Tracing

Two independent identifiers, captured at the adapter's single insert path, merged with
`TracePropagator.composite` exactly as `tandem-spring-producer` does (LLD-spring-config §2.6): the
distributed trace context from OpenTelemetry, then the correlation id. On a key collision the earlier
one wins.

### 10.1 Classes

```java
@Qualifier @Retention(RUNTIME) @Target({METHOD, FIELD, PARAMETER, TYPE})
@interface TandemTraceCapture { }                  // internal qualifier, package-private

final class MdcCorrelationTracePropagator implements TracePropagator {
    MdcCorrelationTracePropagator(String mdcKey);
    // capture(): org.jboss.logging.MDC.get(mdcKey), toString(); else TandemContext.currentCorrelationId();
    //            Map.of(TandemHeaders.CORRELATION_ID, id), or Map.of() when neither is set
}

@Singleton
final class TracePropagatorProducer {
    @Produces @Singleton @DefaultBean
    TracePropagator tandemTracePropagator(TandemTracingRuntimeConfig config,
                                          @TandemTraceCapture Instance<TracePropagator> openTelemetry);
}

@Singleton
final class OtelTracePropagatorProducer {           // registered only when §6.2 allows
    @Produces @Singleton @TandemTraceCapture
    TracePropagator tandemOtelTracePropagator(OpenTelemetry openTelemetry);   // new OtelTracePropagator(openTelemetry)
}
```

`tandemTracePropagator` returns:

| `tandem.tracing.enabled` | OpenTelemetry capture resolvable | Result |
|---|---|---|
| `false` | any | `TracePropagator.NOOP` |
| `true` | no | `MdcCorrelationTracePropagator(correlationIdMdcKey)` |
| `true` | yes | `composite(openTelemetry.get(), new MdcCorrelationTracePropagator(correlationIdMdcKey))` |

The qualifier is what keeps OpenTelemetry types out of the always-registered producer (§6.2): it injects
a `TracePropagator`, a core type, and only the conditional producer names `OpenTelemetry`.

### 10.2 What is read, and from where

- **The MDC is JBoss Logging's.** Quarkus binds SLF4J to the JBoss LogManager, so a value an application
  puts with `org.slf4j.MDC` is the one `org.jboss.logging.MDC` returns. Its `get` returns `Object`; the
  propagator uses `toString()`.
- **The OpenTelemetry instance is the application's** `io.opentelemetry.api.OpenTelemetry` bean from
  `quarkus-opentelemetry`. `OtelTracePropagator` writes the current context through the propagators the
  application configured (`traceparent` with Quarkus's W3C default), and writes nothing when no span is
  active (HLD-tracing §5). With `quarkus.otel.enabled=false` the instance is a no-op and nothing is
  captured.
- **An application `TracePropagator` bean replaces all of this**, and is used whatever
  `tandem.tracing.enabled` says, as in the Spring module: the flag gates Tandem's own capture, not the
  application's.

### 10.3 OpenTelemetry present, adapter absent

An application with `quarkus-opentelemetry` that sets `tandem.tracing.enabled=true` and did not add
`com.codingful:tandem-tracing-otel` gets the correlation id only. That is not a failure: no event is lost,
the row lacks a trace context. The extension says so once, at startup, with log line L2 at `WARN`, when
all of these hold:

- at build time, `OPENTELEMETRY_TRACER` was present and the adapter class absent (`otelAdapterMissing`);
- at runtime, `tandem.tracing.enabled` is `true`;
- the `TracePropagator` in use is the extension's default bean (`InjectableBean.isDefaultBean()`), not
  one the application supplied.

**Rejected:** *failing the startup*, which would stop a service over a missing trace header; *a
non-optional dependency on `tandem-tracing-otel`*, which would put the OpenTelemetry API on every
application's classpath (HLD §1.3); *saying nothing*, which leaves the user looking for a trace that is
never written.

---

## 11. Exceptions and logging

### 11.1 Exception types

The extension defines **no exception type**. It throws:

| Where | Type | Why |
|---|---|---|
| Build steps | `io.quarkus.runtime.configuration.ConfigurationException` | Quarkus reports it as a configuration error of the build, naming the step |
| Startup (recorder) | `com.codingful.tandem.core.exception.TandemConfigurationException` | The type the library and the Spring modules use for the same kind of mistake; an operator searching for it finds one family |
| Insert path | `com.codingful.tandem.core.exception.OutboxInsertException`, `PayloadSerializationException` | What every `OutboxRepository` and collector throws; a caller catching `TandemException` catches them all |
| Library code called by the extension | the library's own, unchanged | |

A new type would add public surface to name a condition the existing types already describe.

### 11.2 Message catalogue

The texts follow AGENTS.md §Logging 3 and 7 for exception messages too: a capital letter, fixed text,
then a `name:value` tail. They are stable enough for the guide to quote but are **not** part of the
semver contract (§15.1).

| Id | Type | Text |
|---|---|---|
| B1 | `ConfigurationException` | `Tandem outbox datasource is not a configured JDBC datasource dataSource:<name>, configured:<names>` |
| B2 | `ConfigurationException` | `Tandem outbox datasource must be PostgreSQL dataSource:<name>, dbKind:<kind>` |
| B3 | `ConfigurationException` | `Tandem outbox datasource has JTA transactions disabled, every insert would autocommit apart from the business change dataSource:<name>` |
| S1 | `TandemConfigurationException` | `Tandem outbox datasource is fixed at build time and differs at runtime; rebuild the application to change it key:tandem.outbox.datasource, buildTimeValue:<name>, runtimeValue:<value>` |
| S2 | `TandemConfigurationException` | `A JTA transaction is active at Tandem startup; the bucket-count check must run outside any transaction dataSource:<name>, transactionStatus:<status>` |
| S3 | `TandemConfigurationException` | `JTA transaction status unavailable at Tandem startup dataSource:<name>` (cause attached) |
| S4 | `TandemConfigurationException` | `Tandem outbox datasource is not active dataSource:<name>` |
| M1 | `OutboxInsertException` | `Outbox not open for writes: Tandem startup has not completed, so the bucket count is not checked yet` |
| M2 | `OutboxInsertException` | `Outbox insert refused: no active JTA transaction, the row would not commit with the business change transactionStatus:<status>` |
| M3 | `OutboxInsertException` | `Outbox insert refused: JTA transaction status unavailable` (cause attached) |
| M4 | `PayloadSerializationException` | `No PayloadSerializer available; add quarkus-jackson, supply a PayloadSerializer bean, or record a pre-built OutboxMessage via add(...)` |
| M5 | `PayloadSerializationException` | `Failed to serialize payload to JSON` (cause attached) |

`<status>` is the `jakarta.transaction.Status` constant's numeric value, as the transaction manager
returns it.

### 11.3 Logging

**API: JBoss Logging** (`org.jboss.logging.Logger`). It is on every Quarkus application's classpath
through `quarkus-core`, so it adds nothing to the footprint, which is the test AGENTS.md §Logging applies
per module; it is the API every Quarkus extension logs through, and the extension already reads its MDC.
`System.Logger` would reach the same handlers through the JUL bridge, and SLF4J through its JBoss
binding, each one more indirection for no gain. The extension ships no logging configuration.

| Id | Level | Logger | Text |
|---|---|---|---|
| L1 | `INFO` | `TandemProducerRecorder` | `Tandem outbox open for writes dataSource:<name>, bucketCount:<n>, wakeup:<mode>, tracing:<enabled>` |
| L2 | `WARN` | `TandemProducerRecorder` | `Tandem trace capture records the correlation id only, the OpenTelemetry adapter is missing artifact:com.codingful:tandem-tracing-otel` |

Both are once per application start (§Logging 4). Nothing is logged per insert: `tandem-jdbc` already
logs at `TRACE` what it does per row. Exceptions are thrown, never logged and rethrown: Quarkus logs a
failed startup itself, and an insert failure belongs to the caller.

---

## 12. Migration routes

The application owns its migrations (HLD §6.3). The guide shows both routes; the extension contains no
code for either.

### 12.1 Liquibase

```properties
quarkus.liquibase.migrate-at-start=true
quarkus.liquibase.change-log=db/changelog.xml
```

```xml
<databaseChangeLog ...>
    <include file="tandem/schema/postgres/changelog/db.changelog-master.xml"/>
    <include file="db/app/changes.xml"/>
</databaseChangeLog>
```

Liquibase identifies each Tandem changeset by the classpath path of its SQL file
(`tandem/schema/postgres/changelog/v1-baseline.sql`, …), so a newer `tandem-jdbc` applies only the
changesets the database has not recorded, after everything already applied. The guide recommends
keeping the application's changes in a file of their own, included after Tandem's: their identity then
never depends on how Tandem's changelog is included.

For a named datasource the keys are `quarkus.liquibase."<name>".*`; the startup step waits for every
datasource's migrations (§6.4).

### 12.2 Flyway

The application copies Tandem's SQL files from `schema/postgres/changelog/` of the release it uses (or
from the `tandem-jdbc` jar, same content) into its own migrations, **byte for byte**, each as its next
version: `V<n>__tandem_v1_baseline.sql`, `V<n+1>__tandem_v2_replays.sql`, and so on. Upgrading Tandem
copies the files of the schema versions not yet applied, as the next versions after whatever the
application has applied since. The files carry Liquibase's formatted-SQL comments, which Flyway runs as
comments. No Flyway customizer, no location inside the jar: Flyway's defaults
(`quarkus.flyway.migrate-at-start=true`).

---

## 13. Testing

Tandem's rules apply (AGENTS.md §Testing): BDD names in business terms, no mocks, behaviour over
coverage, a real PostgreSQL. Every database is **`postgres:18`**, one constant in each test module's
support code; the integration-test applications pin Dev Services to the same image
(`quarkus.datasource.devservices.image-name=postgres:18`), so a Quarkus upgrade that changes its default
image does not change the database under the tests.

### 13.1 Where each kind of test runs

| Module | Harness | Database | JVM | Native |
|---|---|---|---|---|
| `deployment` | `QuarkusExtensionTest` (one application per test class, built in the test) | Dev Services; the upgrade tests their own container (§13.4) | yes | no |
| `integration-tests` | `@QuarkusTest` (`*Test`) and `@QuarkusIntegrationTest` (`*IT`) over HTTP | Dev Services | yes | yes |
| `integration-tests-minimal` | `@QuarkusTest` and `@QuarkusIntegrationTest` | a container started by a test resource, schema applied by plain JDBC | yes | yes |
| `integration-tests-e2e` | `@QuarkusTest` | its own Testcontainers network (§13.5) | yes | no |

**`integration-tests`** is an application with Hibernate ORM, Liquibase (including Tandem's changelog),
Quarkus REST, Jackson, `quarkus-opentelemetry` and `tandem-tracing-otel`. Each scenario is a REST
endpoint run in the application, so the same test class runs on the JVM and, as its `*IT` subclass,
against the native executable. **`integration-tests-minimal`** depends on the extension and
`quarkus-jdbc-postgresql` only.

### 13.2 HLD §12 mapped to tests

| HLD §12 behaviour | Module, test | Native |
|---|---|---|
| Business rollback removes the outbox rows, plain tier | `integration-tests`, `PlainTierTest` | yes |
| Business rollback removes rows the template inserted, joined and started transaction | `integration-tests`, `TemplateTierTest` | yes |
| A failing insert reaching the transaction boundary prevents the business commit, both tiers | `PlainTierTest`, `TemplateTierTest` | yes |
| An SQL failure caught inside the transaction: nothing commits, the method returns normally | `PlainTierTest` | yes |
| A serialization failure in the callback caught around `execute` prevents the commit, template starting and joining | `TemplateTierTest` | yes |
| A serialization failure caught inside the callback leaves the transaction committable | `TemplateTierTest` | yes |
| An insert without a transaction fails, no row | `PlainTierTest` | yes |
| A template call inside `REQUIRES_NEW` writes in the new transaction | `TemplateTierTest` | yes |
| Ordering, not serialized: the reorder | `integration-tests`, `OrderingTest` | yes |
| Ordering, `lockedWrite()`: the second writer waits on the advisory lock, then the order holds | `OrderingTest` | yes |
| A payload type used only by the outbox: registered serializes, unregistered fails | `integration-tests`, `PayloadRegistrationTest` | yes |
| Trace context and correlation id on the row with OpenTelemetry present | `integration-tests`, `TracingTest` | yes |
| An insert before startup completes is refused | `deployment`, `EarlyWriteTest` (a Flyway `AFTER_MIGRATE` callback writes) | no |
| The bucket-count check runs after the migrations, Liquibase and Flyway | `deployment`, `LiquibaseStartupOrderTest`, `FlywayStartupOrderTest` | no |
| A missing schema stops the startup with the guard's error | `deployment`, `MissingSchemaTest` | no |
| A named datasource is the one written to | `deployment`, `NamedDataSourceTest` | no |
| A different runtime datasource value stops the startup (S1) | `deployment`, `DataSourceChangedAtRuntimeTest` | no |
| A custom `PayloadSerializer` replaces the default | `deployment`, `CustomPayloadSerializerTest` | no |
| The correlation id from SLF4J's MDC reaches the row | `deployment`, `CorrelationIdTest` | no |
| OpenTelemetry present, adapter absent: correlation id only, L2 logged | `deployment`, `MissingOtelAdapterTest` | no |
| Keys of other roles (`tandem.relay.*`, `tandem.kafka.*`, `tandem.tracing.publish-span`) do not stop the startup | `deployment`, `OtherRoleKeysTest` | no |
| Missing, non-PostgreSQL and non-transactional datasources fail the build (B1 to B3) | `deployment`, one test class each | n/a |
| Startup and a write with no optional integration and no schema-ready item | `integration-tests-minimal`, `NoOptionalIntegrationsTest` | yes |
| An object payload with no serializer fails at the call (M4), a pre-built message is written | `integration-tests-minimal`, `NoOptionalIntegrationsTest` | yes |
| Upgrade of a migrated database, each route | `deployment`, `LiquibaseUpgradeTest`, `FlywayUpgradeTest` (§13.4) | no |
| End to end with the `tandem-relay` image | `integration-tests-e2e`, `RelayEndToEndTest` (§13.5) | no |

Class names above are the scenario groups; the methods carry the BDD names. The JVM-only rows need an
application configured differently from `integration-tests`, and leaving them out of native mode is the
risk HLD §12 accepts.

**What the tracing tests assert.** `TracingTest` runs an endpoint inside the server span
`quarkus-opentelemetry` opens, returns that span's trace id and the correlation id it put in the MDC, and
reads the row: its `traceparent` header carries the same trace id, and its `correlation-id` header the
same value. `MissingOtelAdapterTest` relies on the deployment module's test classpath, which has
`quarkus-opentelemetry` and not `tandem-tracing-otel` (optional dependencies do not propagate): the row
carries the correlation id and no `traceparent`, and the log has L2.

**Ordering tests** use the library's own relay claim (`JdbcOutboxStore.claimBatch`, then
`markDoneBatch`) on the aggregate's bucket between the two commits. The serialized scenario proves the
wait from the database, not from timing: it polls `pg_stat_activity` for a session waiting on an
`advisory` lock whose `pg_blocking_pids` include the first writer's backend pid, then checks that the
second writer has not returned, and releases the first writer in a `finally`.

### 13.3 Assertions on the database

Tests read rows with plain JDBC on the same datasource and compare against the inputs (aggregate id,
payload, headers by `TandemHeaders` constants), never against repeated literals (AGENTS.md §Testing 4).
The schema's own names (`tandem_outbox`, `tandem_meta`) appear in one support class per module.

### 13.4 Upgrade tests

Dev Services starts its database with the application, too late to hold a schema of an earlier Tandem
version, so these tests start `postgres:18` themselves (`testcontainers-postgresql`), migrate it before
Quarkus starts, and hand the URL to the application with Dev Services disabled. One database per
scenario, kept in system properties because a `QuarkusExtensionTest` class is initialised by two class
loaders.

| Route | Before Quarkus starts | Quarkus at start | Asserted |
|---|---|---|---|
| Liquibase | A changelog including Tandem's changesets up to the version **before** the floor's last one, plus the application's changes | The current changelog (§12.1) | `DATABASECHANGELOG` has no changeset twice and the floor's last changeset last; that version's schema objects exist; a write succeeds |
| Flyway | The application's first migration, copies of Tandem's files up to the version before the floor's last one, then a later application migration | The next copy as the next version (§12.2) | `flyway_schema_history` in version order with the new copy last; that version's schema objects exist; a write succeeds |

The schema objects are asserted explicitly, not only the write: a schema step that adds an index leaves
an older schema writable, and the write alone would not see the step missing. When the floor moves, the
two "before" points move with it.

### 13.5 End to end with the relay image

`integration-tests-e2e` proves HLD §3's topology: a row the extension writes is published by the
`tandem-relay` image. The test resource starts, on one Testcontainers network:

- `postgres:18`, migrated with Tandem's changelog;
- a Kafka broker (`apache/kafka`, KRaft);
- `ghcr.io/alirux/tandem-relay:<tag>`, the tag in the module's `tandem-relay.image.tag` property, with
  `SPRING_DATASOURCE_URL` on the network alias, `TANDEM_KAFKA_SOURCE`,
  `TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS`, and the same `TANDEM_OUTBOX_BUCKET_COUNT` the application
  sets, a value other than the default so that a mismatch would show. The test waits for
  `/actuator/health/readiness` on the management port.

The Quarkus application connects to PostgreSQL through the mapped port (Dev Services disabled), writes
one aggregate's events through the template, and a plain `KafkaConsumer` (a test dependency of this
module only) reads them: same count, same order, CloudEvents `id` equal to each outbox id. JVM only:
native mode adds nothing to a test whose subject is the row shape and the relay.

The tag is the latest `relay-v*` release when the extension releases; moving it is a step of the release
checklist (§15.4), not a Dependabot update, since a Maven property naming an image is invisible to it.

---

## 14. CI

### 14.1 Workflows

| Job | Runs | JDK | Quarkus | Required |
|---|---|---|---|---|
| `jvm` | `mvn -B verify` (every module, `it` and `docs` profiles on) | 17 | `3.40.0` (`-Dquarkus.version=3.40.0`) | yes |
| `jvm` | same | 17 | the pinned latest 3.40.x | yes |
| `jvm` | same | 25 | the pinned latest 3.40.x | yes |
| `native` | `mvn -B verify -Dnative -Dquarkus.native.container-build=true -pl integration-tests,integration-tests-minimal -am` | 17 | the pinned latest 3.40.x | yes |
| `latest-tandem` | `mvn -B verify -Dtandem.version=<latest>` | 17 | the pinned latest 3.40.x | no |

- **JDK 17 runs the build and the tests**, not only the compiler target: bytecode compiled for 17 does
  not prove it runs on 17 (HLD §8). JDK 25 is the newest runtime the line supports.
- **Native** builds in the line's default Mandrel builder image, so no GraalVM version is chosen by this
  repository, and runs the two applications' `*IT` classes against the executables. The minimum version
  does not get a native leg: a native difference between two patch releases of one LTS line is not worth
  a second build of several minutes.
- **`latest-tandem`** reads the latest release from Maven Central's
  `com/codingful/tandem-core/maven-metadata.xml` (`<release>`) and skips itself when it equals the floor.
  It is not required: its failure is a regression of a library release, reported on the library, not a
  defect of the pull request. It also runs weekly on a schedule.
- Every job that starts containers pulls Docker Hub images through `mirror.gcr.io`, as Tandem's own CI
  does, to keep anonymous pull limits from failing a build.

### 14.2 Ecosystem CI

The repository joins the hub's ecosystem CI (`quarkiverse-tandem/info.yaml` in
`quarkus-ecosystem-ci`), which builds it against Quarkus snapshots and reports to an issue in
`quarkiverse/quarkiverse`. It is an early signal for the next line, not a gate.

### 14.3 Dependabot

- **Maven**, weekly: `io.quarkus:*` limited to patch updates on `main` (the line is 3.40; a minor or major
  is a branch decision, HLD §8); `com.codingful:*` **ignored**, because the declared version is the floor
  and moves only by decision (§15.2); test dependencies freely.
- **GitHub Actions**, weekly.

---

## 15. Versioning and release

### 15.1 What the version governs

As HLD §7: the public types of §3.2, the keys of §8.2 with their meaning and defaults, the replaceable
beans of §3.3, the Tandem floor, and the supported Quarkus line with its minimum. **Not** governed: the
`runtime` and `deployment` internals, the build steps and their items, the message texts of §11.2, the
log lines.

| Change | Bump |
|---|---|
| A fix; a Quarkus patch within the line; a Tandem dependency left at the floor | patch |
| A new tier, key or optional integration; raising the Tandem floor; raising the Quarkus minimum within the line | minor |
| Removing or changing anything in §3.2 or §8.2; changing the Quarkus line or the Java baseline | major |

### 15.2 The floor

The parent declares `tandem.version` = **1.0.0**, and that declared version is the floor: what an
application gets unless it imports a later `tandem-bom`. The ordinary build therefore tests the floor
resolved from Maven Central, with no substitution step; `latest-tandem` (§14.1) tests the other end.

Raising the floor is a deliberate change of that property, in a minor release whose notes name the new
floor and why. A feature needing a newer library API waits for the library release that ships it. Until
Tandem 1.0.0 is on Maven Central the property holds the latest 1.0.0 release candidate, and no release of
the extension is made.

### 15.3 Branches and numbers

| Branch | Quarkus | Java | Versions |
|---|---|---|---|
| `main` | 3.40 LTS, minimum 3.40.0 | 17+ | 1.x, from **1.0.0** |
| `quarkus-4` | 4.x | 21+ | 2.0.0, released when it becomes `main` at Quarkus 4's general availability |
| `1.x` (created then) | 3.40 LTS | 17+ | 1.x patches, until 3.40's maintenance ends (2027-09-30) |

The first release is 1.0.0: its public API is two interfaces with the signatures of a released, stable
Spring API, and the floor it builds on is a 1.0 library. Development versions are `999-SNAPSHOT`, the
hub's convention.

### 15.4 Releasing

The hub's flow: a pull request from a branch of the repository itself (not a fork) sets
`release.current-version` and `release.next-version` in `.github/project.yml`; merging it runs the hub's
release workflow, which tags and deploys to Maven Central with the organization's credentials. The
GitHub Release is created by hand afterwards.

**Release notes are written once, in the release pull request's description**, and become the body of
the GitHub Release unchanged. Structure, as Tandem's own (AGENTS.md §Releases): first line
`quarkus-tandem <x.y.z>: <short summary>`, then only the sections that apply among **New**, **Fixes**,
**Breaking**, **Known limitations**. Every release states the **Tandem floor** and the **Quarkus line
and minimum** in its first lines, since neither is implied by the version. Links are absolute URLs.

Checklist of the release pull request:

1. `tandem.version` is the intended floor, and a final release (never an RC).
2. `tandem-relay.image.tag` is the latest `relay-v*` release (§13.5), and the e2e test passes on it.
3. The `docs` module's version attributes match the release.
4. The notes are in the description.

After the release, the hub opens the catalog pull request (`quarkus-extension-catalog`) by itself; the
first release is also announced on the Tandem site's guide, which links the extension's documentation.

---

## 16. Conventions of the extension repository

| Convention | Source |
|---|---|
| Tests: BDD names in business terms, no mocks, behaviour over coverage, no fragile literals | AGENTS.md §Testing 1 to 4 |
| Log and exception message style (§11) | AGENTS.md §Logging 3, 5, 7 |
| Javadoc: every public type, ports fully tagged, the provided/SPI paragraph | AGENTS.md §Javadoc |
| Commits: Conventional Commits `<type>(<scope>): <description>`, the same closed type list, scopes `producer`, `docs`, `build`, `ci`, `deps`; no AI co-author trailer | AGENTS.md §Commit messages |
| Formatting | the hub's (§2.5) |
| Releases | the hub's flow, with Tandem's notes structure (§15.4) |

A design change lands in this repository's `docs/HLD-quarkus.md` and `docs/LLD-quarkus.md` first, then in
the extension.

---

## 17. First release and later

| Item | First release | Later |
|---|---|---|
| Plain tier, template tier | yes | |
| Build-time datasource checks, startup ordering, runtime datasource refusal | yes | |
| Jackson serializer, MDC correlation id, OpenTelemetry capture | yes | |
| Native mode for everything above | yes | |
| Annotation tier (HLD §5.5) | | after its prototype; the interceptor inserts through the adapter of §4 |
| Event tier (HLD §5.6) | | after its prototype; the observers insert through the adapter of §4, and its build steps consume `TandemDataSourceBuildItem` and ArC's bean metadata |
| Quarkus 4 | | the `quarkus-4` branch (§15.3) |
| Embedded relay, Admin API on Quarkus REST (HLD §9) | | on demand, each a design of its own |

The adapter is the point every later tier plugs into, so its checks are written once. Each later tier
gets its section in a new version of this document once its prototype has shown the behaviour HLD §5.5
or §5.6 requires.

---

## 18. Open points

- **When to file the Quarkiverse extension proposal** (HLD §14). This document and the HLD are what it
  describes.
- **Whether `tandem.tracing.publish-span` needs a declaration** in the tracing mapping (§8.2). Settled
  by `OtherRoleKeysTest` in the first implementation step; either outcome is decided above.
