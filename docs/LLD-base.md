# Tandem — Base LLD (Shared Foundations)

**Version:** 1.0  
**Status:** Draft  
**Applies to:** all Tandem modules

This document captures technical conventions shared across all module-level LLDs.
Each module LLD references this document rather than repeating these details.

---

## 1. Build & Publication Coordinates

**Build system:** Gradle (multi-project build), **Kotlin DSL** (`*.gradle.kts`, `settings.gradle.kts`) —
type-safe, best IDE support, and consistent with the `build.gradle.kts` assumed by the release workflow.  
**Publication target:** Maven Central

| Property | Value |
|---|---|
| `group` | `com.codingful` |
| Root project name | `tandem` |
| Root Java package | `com.codingful.tandem` |
| License | Apache 2.0 |
| Java version | 17+ |
| Gradle DSL | Kotlin (`*.gradle.kts`) |

### Subproject names and published artifact IDs

| Subproject | Published `artifactId` | Notes |
|---|---|---|
| `tandem-bom` | `tandem-bom` | |
| `tandem-core` | `tandem-core` | |
| `tandem-jdbc` | `tandem-jdbc` | |
| `tandem-cloudevents` | `tandem-cloudevents` | The CloudEvents envelope; every transport adapter binds it to its own wire format |
| `tandem-kafka` | `tandem-kafka` | |
| `tandem-spring-producer` | `tandem-spring-producer` | Write-side Spring autoconfig (JDBC, **no Kafka**) — used by the client (§3.2 HLD). Split by role, no aggregator (Q21, LLD-spring-config §1) |
| `tandem-spring-relay` | `tandem-spring-relay` | Relay Spring autoconfig (JDBC + Kafka) |
| `tandem-relay` | `tandem-relay` | Prebuilt **standalone runnable** relay (Spring Boot app over `tandem-spring-relay`) — split topology (§3.2 HLD) |
| `tandem-test` | `tandem-test` | |
| `tandem-kafka-streams` | `tandem-kafka-streams` | Optional — causal-ordering adapter (§9 HLD) |
| `tandem-flink` | `tandem-flink` | Optional — causal-ordering adapter (§9 HLD) |
| `tandem-micrometer` | `tandem-micrometer` | Optional — relay-side Micrometer adapter for the `TandemMetrics` port (§7 HLD) |
| `tandem-tracing-otel` | `tandem-tracing-otel` | Optional — OpenTelemetry trace-capture and relay-span adapter, for applications outside Spring (§7.1 HLD, HLD-tracing.md §5) |
| `tandem-admin` | `tandem-admin` | Optional — REST admin API, API-first (§7.2 HLD, admin-api.openapi.yaml) |
| `tandem-benchmark` | *(not published)* | Internal — load/performance harness (see HLD-load-testing.md) |
| `tandem-sample` | *(not published)* | Runnable end-to-end tutorial (plain Java, no Spring) |
| `tandem-sample-spring` | *(not published)* | Runnable Spring Boot tutorial — the write-side tiers plus the autoconfigured relay |
| `tandem-coverage` | *(not published)* | Build-only — the project-wide aggregated JaCoCo report |

### Dependency graph and vulnerability alerts

CI submits a GitHub dependency graph on every push to `main` (`gradle/actions/dependency-submission`),
and that graph is what Dependabot raises security alerts against. It is deliberately **scoped to the
redistributed footprint**: only the published modules, and within them only `runtimeClasspath`, the
closest Gradle equivalent of the published POM's compile + runtime scopes. An alert therefore means
something an adopter inherits, which is the same surface [THIRD-PARTY-NOTICES.md](../THIRD-PARTY-NOTICES.md)
tracks by hand.

Left out on purpose: test and build-time tooling, `compileOnly` dependencies (Spring, Jackson,
Micrometer Tracing: none of them reaches a published POM), the two extra Boot lines of the
compatibility matrix, and the unpublished sample and benchmark apps, whose own server runtime would
otherwise dominate the alert list without any adopter ever seeing it. Those classpaths are still kept
off known-vulnerable versions where it costs nothing, through version floors in the catalog and
`constraints` in the module that pulls the coordinate, but they no longer generate alerts. The Go
modules under `tandem-cli/` are unaffected: GitHub reads their `go.mod` directly rather than through
this submission.

---

## 2. Key Gradle Plugins

| Plugin | ID | Purpose |
|---|---|---|
| Maven Central publishing | `com.vanniktech.maven.publish` | Publishes all modules to Maven Central; provides `publishToMavenCentral` task and handles GPG signing |

---

## 3. Database Object Naming

All Tandem-managed database objects are **prefixed `tandem_`** to keep them clearly separate
from the client's own tables:

| Object | Name |
|---|---|
| Outbox table | `tandem_outbox` |
| Attempt archive (designed, not built — HLD-attempt-archive §3) | `tandem_outbox_attempt` |
| Lamport clock store (reserved, HLD-causal-ordering §3.1) | `tandem_aggregate_clock` |
| Bucket ownership (standalone, §4.3) | `tandem_bucket_lease` |
| Relay control flag (§4.1 admin) | `tandem_relay_control` |
| Inbox reorderer (future, HLD-causal-ordering §7) | `tandem_inbox` |
| Indexes | `idx_tandem_…` |

Column names are **not** prefixed (they are already scoped by their table). The prefix is
fixed for now; a configurable prefix/schema could be added later if multi-tenancy in one DB
is required.

---

## 4. Package Naming Convention

```
com.codingful.tandem.<module>[.<sub-package>]
```

| Module | Root package |
|---|---|
| tandem-core | `com.codingful.tandem.core` |
| tandem-jdbc | `com.codingful.tandem.jdbc` |
| tandem-cloudevents | `com.codingful.tandem.cloudevents` |
| tandem-kafka | `com.codingful.tandem.kafka` |
| tandem-spring-producer | `com.codingful.tandem.spring.producer` |
| tandem-spring-relay | `com.codingful.tandem.spring.relay` |
| tandem-relay | `com.codingful.tandem.relay` |
| tandem-test | `com.codingful.tandem.test` |
| tandem-kafka-streams | `com.codingful.tandem.kafkastreams` |
| tandem-flink | `com.codingful.tandem.flink` |
| tandem-micrometer | `com.codingful.tandem.micrometer` |
| tandem-tracing-otel | `com.codingful.tandem.tracing.otel` |
| tandem-admin | `com.codingful.tandem.admin` |
| tandem-benchmark | `com.codingful.tandem.benchmark` |
| tandem-sample | `com.codingful.tandem.sample` |
| tandem-sample-spring | `com.codingful.tandem.sample.spring` |
