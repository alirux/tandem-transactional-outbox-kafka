# Tandem: Public API and Stability Rules

**Version:** 0.3  
**Status:** Accepted. Not implemented: §6 lists the changes due before `1.0.0-rc.1`.  
**Companion to:** [HLD.md](HLD.md) §1.1, §1.4; [HLD-admin-api.md](HLD-admin-api.md);
[HLD-cloudevents.md](HLD-cloudevents.md); [LLD-jdbc.md](LLD-jdbc.md) §6;
[LLD-spring-config.md](LLD-spring-config.md); [HLD-write-side-sdks.md](HLD-write-side-sdks.md) §4

This note decides what Tandem 1.0.0 freezes, how each frozen contract may change without a new major
version, and how the build checks it.

---

## 1. Summary

- From 1.0.0, semantic versioning covers **five contracts**: the Java API, the database schema, the
  published message, the Admin API and the Spring properties, plus a short list of **names** that
  applications and operators configure against (§2).
- **Supported platform versions are floors** that rise only by decision, in a minor version (§2).
- **One rule** decides what is Java API: a public type of a published library module, outside any
  `internal` package (§3).
- **Three rules** let the Java API grow without breaking it: how interfaces gain methods, how records
  and enums gain members, and when construction goes through a builder (§4).
- **Every contract has a gate** that compares the build with the latest release, or with a committed
  reference file, and fails on an incompatible change (§5).
- §6 applies the rules to the code as it stands: the changes due before `1.0.0-rc.1`.

---

## 2. What 1.0.0 freezes

| Contract | What it covers | Design owner |
|---|---|---|
| Java API | The public types of the published library modules, as defined in §3 | this note |
| Database schema | The `tandem_*` tables; the Liquibase changelog and its classpath location `tandem/schema/postgres/changelog/`; the write contract of [HLD-write-side-sdks.md](HLD-write-side-sdks.md) §4 | HLD §1.4, LLD-jdbc §6 |
| Published message | The CloudEvents envelope and the raw passthrough envelope: record key, headers and their names, body | HLD-cloudevents, HLD-alternative-envelope |
| Admin API | The `/v1` contract in [admin-api.openapi.yaml](admin-api.openapi.yaml), including every problem `type` slug | HLD-admin-api |
| Spring properties | Every `tandem.*` key, its type and its default | LLD-spring-config |
| Names | The class names of the Spring autoconfigurations (other autoconfigurations order against them with `afterName`, applications exclude them by name); logger names at package level (`com.codingful.tandem.jdbc`, `com.codingful.tandem.kafka`, …); the module name each published jar declares (`Automatic-Module-Name`, for example `com.codingful.tandem.core`); metric names and their tag keys, span names and their attribute keys | this note |

**Defaults are part of the contract.** A value Tandem applies when the application sets nothing (a
`RelayConfig` default, the default of a `tandem.*` property, a Kafka producer setting Tandem fills in)
changes only in a major version.

**Supported versions are floors, and moving one is a decision.** Tandem supports Java 17, Spring Boot
3.x and 4.x, and PostgreSQL 14 and later ([guide/getting-started.md](../guide/getting-started.md),
[guide/compatibility.md](../guide/compatibility.md)). Support is a promise: a defect on a supported
version is Tandem's to fix. Which supported versions CI exercises is a separate statement, made in
[guide/compatibility.md](../guide/compatibility.md) §2. A version reaching its end of life upstream
does not move the floor: the floor rises only by the maintainers' decision, in a minor version, never
a patch, and the release notes of that version say so and give the reason.

The delivery guarantees of HLD §4 (at-least-once, per-aggregate order) are what the contracts above
exist to carry. A change that weakens one is breaking, whatever the gates of §5 report.

**Not frozen:** the text of log messages and exception messages, `toString` output, health details,
timings and throughput, anything in an `internal` package, and the unpublished modules
(`tandem-sample`, `tandem-sample-spring`, `tandem-benchmark`, `tandem-coverage`). The separately
versioned artifacts keep contracts of their own: `tandem-rabbitmq` (LLD-rabbitmq §9), the
`tandem-relay` image (LLD-relay §7.3) and `tandem-cli` (LLD-cli §9.1).

---

## 3. What is Java API

**A type is API when it is `public`, belongs to a published library module, and no segment of its
package is `internal`.** Nothing else is API, whatever its visibility. The published library modules
are `tandem-core`, `tandem-jdbc`, `tandem-cloudevents`, `tandem-kafka`, `tandem-test`,
`tandem-spring-producer`, `tandem-spring-relay`, `tandem-micrometer`, `tandem-tracing-otel` and
`tandem-admin` (`tandem-bom` carries no code).

Three consequences:

- **A type an application does not need is not public.** When only its own package uses it, it is
  package-private. When another Tandem package needs it, it moves to an `internal` package, for
  example `com.codingful.tandem.admin.internal`. Today only `tandem-admin` needs one (§6).
- **A Spring autoconfiguration stays public** although no application calls it: its class name is a
  contract of §2.
- **The members of an `internal` type carry no promise.** The `package-info` of each `internal`
  package says so once.

An `internal` package keeps the logger contract of §2: its loggers stay under the module's package.

**An unbuilt feature publishes no API.** A type declared ahead of its implementation would freeze a
shape no implementation has validated; building the feature later is additive anyway
([HLD-causal-ordering.md](HLD-causal-ordering.md) §0.1).

**Rejected:**

- *A marker annotation on internal types.* It adds a type to `tandem-core`'s API, and the IDE still
  offers the annotated type in completion.
- *A list of internal types kept in the gate configuration and stated in javadoc.* Nothing at the
  point of use tells the reader.
- *JPMS module descriptors exporting only the API packages.* They enforce the boundary only for
  applications on the module path; Spring Boot applications, the main audience, run on the classpath,
  where descriptors are ignored. Every published module would carry a descriptor to keep in step with
  its dependencies, for a benefit that reaches a minority (HLD §1.1). The jars still declare a stable
  module name (§2), so an application on the module path requires each one by a name that does not
  change.

---

## 4. How the Java API grows

An additive change is a minor version; anything else is breaking. Java makes some additive changes
breaking by accident, so three rules apply.

### 4.1 Interfaces: SPI or provided

Every public interface is one of two kinds, and its javadoc says which:

- **SPI**: the application implements it and Tandem calls it. A new method is always `default`, so
  an existing implementation keeps compiling and running. `TandemMetrics` already grows this way.
- **Provided**: Tandem implements it and the application calls it. Where an application needs a
  stand-in, `tandem-test` supplies it: `InMemoryOutbox` for `OutboxRepository`, `OutboxStore`,
  `OutboxQuery`, `ReplayService` and `DiscardService`, `InMemoryRelayControl` for `RelayQuery` and
  `RelayControl`. A provided interface may gain abstract methods in a minor version; an application
  that implements one anyway follows Tandem's minor versions.

| Kind | Interfaces |
|---|---|
| SPI | `OutboxDispatcher`, `MessageEncoder`, `TopicRouter`, `PayloadSerializer`, `TracePropagator`, `TandemMetrics`, `TandemSpanRecorder` and its `Span`, `TandemAggregate`, `BucketCountReconciliation` (core); `BackoffStrategy`, `BucketSource`, `WakeupSource`, `RelayControlSource` (jdbc); `KafkaMessageEncoder` (kafka); `OutboxEventMapper` (spring-producer) |
| Provided | `OutboxRepository`, `OutboxStore`, `OutboxQuery`, `ReplayService`, `DiscardService`, `RelayQuery`, `RelayControl`, `BucketCountStore` (core); `WakeupSource.Listener` (jdbc); `TransactionalOutboxTemplate`, `OutboxCollector` (spring-producer) |

A nested interface has a kind of its own: `WakeupSource.Listener` is provided although `WakeupSource`
is SPI, because Tandem implements the listener and hands it to the source.

Without the split, every interface Tandem implements would keep its 1.0 shape until 2.0: a new filter
on `OutboxQuery` or a new relay operation on `OutboxStore` would be breaking.

**Rejected:** *a `default` for every new method, provided interfaces included.* It works for a query
that may answer "unknown" and for the batch form of an existing method, as `OutboxStore` shows, but not
for a new operation the relay must perform: there a default could only throw or pretend, which turns a
compile-time break into a runtime one.

### 4.2 Records and enums

A public record gains a component only together with an overload of the previous canonical
constructor, which supplies the new component's default. An accessor is never removed. A record that
applications build is also reachable through a builder or a factory (`OutboxSearchCriteria.builder()`,
`KafkaRelayConfig.of`), so callers do not depend on the order of its components. Deconstructing a
Tandem record with a record pattern is outside the promise: a new component changes the pattern.

A public enum may gain constants in a minor version, since the readers of every contract tolerate
values they do not know (HLD §1.4). Code that switches over a Tandem enum keeps a `default` branch,
and the enum's javadoc says so. The public enums are `OutboxStatus`, `SeqSource` and
`RelayCoordinationMode` (core), `Coordination` and `Wakeup` (jdbc).

### 4.3 Construction

A public class takes its required collaborators in a constructor and its optional ones through a
builder, so a new optional collaborator is a new builder method rather than another overload. The three
classes an application wires by hand without Spring (`JdbcOutboxRepository`, `KafkaRelay`,
`WorkerPool`) each keep one constructor for the common case, and everything else goes through their
builder (§6).

---

## 5. Gates

Most gates compare the build with a **baseline**: the latest release, resolved from Maven Central or
from its tag. The baseline version is one build property, moved after each release. A **reference
file** (below) compares it instead with a file committed in the repository.

The gates land before `1.0.0-rc.1`; those that read a baseline have none set yet, and skip. Run once
against `0.11.1` before the tag, they must report exactly the breaking rows of §6 and nothing else,
which checks the gates and the list against each other. Once `1.0.0-rc.1` is published it becomes the
first baseline.

| Contract | Gate | Exists |
|---|---|---|
| Java API | **japicmp** on every published library module: binary and source compatibility, `internal` packages excluded, provided interfaces (§4.1) allowed to gain methods | No |
| Database schema | The generated `tandem-baseline.sql` matches the changelog | Yes |
| | Every changeset shipped in the baseline's `tandem-jdbc` jar is byte-identical in the build | No |
| | **Schema compatibility test:** the baseline release's JDBC adapter, resolved from Maven Central, runs a write, relay and Admin API scenario (every `seq` mode, claim, done, retry, failed, reclaim, cleanup, search, replay, discard, relay control) against a database built from the current changelog. It catches what a new changeset does to the previous version sharing the database (HLD §1.4): a column removed, a `NOT NULL` without a default, a narrower `CHECK` | No |
| Published message | **Reference file:** a reference record through each built-in encoder (CloudEvents on Kafka, raw passthrough) matches a committed file of key, header names and values, and body | No |
| Admin API | **Specmatic** backward-compatibility check of the OpenAPI document against the baseline tag | No |
| | Responses validated against the document in the integration tests | Yes |
| Spring properties | Diff of `META-INF/spring-configuration-metadata.json` against the baseline jars: no key removed, no type changed, no declared default changed | No |
| Effective defaults | **Reference file:** for every `tandem.*` key of the three Spring modules, the value in force when the application sets nothing, wherever it is applied (`RelayConfig`, `MicrometerTandemMetrics`, the producer settings `KafkaProducerConfig` fills in, the Admin API). A derived default, such as the instance id, is recorded as its rule. An application without Spring gets the same values, since the Spring tier delegates to the same classes | No |
| Metric and span names | **Reference file:** the meters the Micrometer adapter registers (name, tag keys) and the spans both span recorders emit (name, attribute keys) | No |
| Other names | Review | n/a |

The Spring metadata reports a default only where the property record declares one: all 5 in
`tandem-spring-producer` and 6 of 27 in `tandem-spring-relay`, where the rest come from `RelayConfig`,
`MicrometerTandemMetrics` or nowhere (a property with no default). The 2 of `tandem-admin` come from
metadata written by hand, which states what the code is meant to do rather than what it does. The
metadata diff therefore guards keys and types, and the effective defaults have a gate of their own.

**A reference file** is committed next to its test, which renders what the code actually produces and
fails on any difference, so a change reaches the file only through an edit the review sees. A line
added is a minor version; a line changed or removed is breaking. A reference file writes wire names as
literals on purpose: built from the constants, it could not notice a constant whose value changed.
That makes reference files the one exception to the rule of AGENTS.md (Testing §4) that test code
names a header through its constant.

**An intentional break** (in a major version, or between two release candidates) is declared in the
gate's configuration next to its release-notes entry. The declaration is removed when the baseline
moves past it.

---

## 6. Applying the rules before 1.0.0-rc.1

Every row marked breaking goes under **Breaking** in the `1.0.0-rc.1` release notes.

| Change | Module | Rule | Breaking |
|---|---|---|---|
| Remove the reserved causal-ordering types and constants (done; the names stay reserved in [HLD-causal-ordering.md](HLD-causal-ordering.md) §0.1) | core | §3, unbuilt feature | Yes |
| `FullJitterBackoff`, `JdbcBucketCountStore`, `BucketLeaseManager`, `PgNotifyWakeup` become package-private: applications reach them through `BackoffStrategy.fullJitter()`, `BucketCountGuard.check`, `BucketSource.forCoordination` and `WakeupSource.forWakeup` | jdbc | §3 | Yes |
| `BucketCountGuard`'s constructor and its instance `check` become package-private: applications use the static `check`, and with `JdbcBucketCountStore` hidden the constructor could only take a store the application writes itself. `BucketCountStore` stays in the core (LLD-bucket-count-guard §2) | jdbc | §3 | Yes |
| `ErrorClassifier` and `DefaultErrorClassifier` become package-private: no public constructor accepts them | kafka | §3 | Yes |
| `CloudEventEncoder` becomes package-private behind a new `KafkaMessageEncoder.cloudEvents(TopicRouter, KafkaRelayConfig)` | kafka | §3 | Yes |
| `JacksonPayloadSerializer`, `MicrometerTracePropagator`, `MdcCorrelationTracePropagator`, `MicrometerTandemSpanRecorder` become package-private: their beans are exposed as the port type, and Jackson 2's `ObjectMapper` leaves the API | spring-producer, spring-relay | §3 | Yes |
| The seven `@ConfigurationProperties` records become package-private: the keys are the contract (§2), and a public record would break binary compatibility with every property added (§4.2) | spring-producer, spring-relay | §3 | Yes |
| Every type except `TandemAdminAutoConfiguration` moves under `com.codingful.tandem.admin.internal`, keeping its feature packages (`internal.outbox`, `internal.relay` and their `dto`): the module's contract is the HTTP API and the autoconfiguration's name. The `basePackages` of the two feature advices follow the move; the module-wide advice already covers it | admin | §3 | Yes |
| `JdbcOutboxRepository.builder(dataSource, bucketCount)` with `tracePropagator` (default no-op) and `wakeup` (default none); the two-argument constructor stays, the three- and four-argument ones go | jdbc | §4.3 | Yes |
| `KafkaRelay.builder(producerConfig, encoder)` with `spanRecorder` (default no-op), which with `KafkaMessageEncoder.cloudEvents` also covers the CloudEvents envelope with a span; the `(producerConfig, router, cfg)` constructor stays, the other two go | kafka | §4.3 | Yes |
| `WorkerPool.builder(store, dispatcher, cfg)`, whose defaults are those of the three-argument constructor (no-op metrics, system clock, full-jitter backoff, every bucket embedded, no pause, no wakeup); the three-argument constructor stays, the three longer ones go | jdbc | §4.3 | Yes |
| Every published library jar declares `Automatic-Module-Name`, named after the module's root package (`com.codingful.tandem.core`, `com.codingful.tandem.spring.relay`, …) | all | §2, names | Only on the module path, where an application required the name derived from the file (`tandem.core`) |
| `ReplayCriteria.builder()`, and the guide builds it that way | core | §4.2 | No |
| Every public interface states its kind (§4.1), and every public enum asks for a `default` branch (§4.2) | all | §4 | No |
| `RelayQuery`, `RelayControl` and their views stay API as provided interfaces, documented in [guide/advanced-integration.md](../guide/advanced-integration.md) §6 next to the other Admin API ports: built from a `DataSource` like them, per-bucket operations meaningful only under `LEASE`, no audit line when called from code | core, jdbc | §4.1 | No |
| The guides that wire Tandem by hand follow the changes above (`reliability.md`, `advanced-integration.md`, `observability.md`, `configuration.md`, `message-format.md`), and so do LLD-jdbc, LLD-spring-config and LLD-alternative-envelope; the release checklist in AGENTS.md refers to the gates of §5, and its Testing §4 states the reference-file exception; the package layout recorded in IMPLEMENTATION-PLAN-admin-api §7 gets a note on the `internal` move | docs | | |

The gates of §5 land in the same pass, before the tag.
