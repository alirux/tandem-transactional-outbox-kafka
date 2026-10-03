# Tandem — `tandem-relay` LLD

**Version:** 1.4 (Implemented)
**Module:** `tandem-relay` · Gradle subproject, package `com.codingful.tandem.relay`
**Depends on:** [`tandem-spring-relay`](LLD-spring-config.md) (the relay autoconfiguration it runs),
[`tandem-kafka`](LLD-kafka.md) (the one transport the image carries, §3.2),
[`tandem-micrometer`](LLD-micrometer.md) (the metrics adapter, §4.3),
[`tandem-admin`](HLD-admin-api.md) (optional second role, §4)
**Companion to:** [HLD.md](HLD.md) §3.2 (deployment topology — the split relay this module ships),
[HLD-admin-api.md](HLD-admin-api.md) §4.1 (the standalone Admin API deployment, delivered by the same image)
**Resolves:** Q23 in [open-questions-lld.md](open-questions-lld.md) §D
**Published:** **Not** to Maven Central. Distributed as an OCI container image and an executable
jar attached to the GitHub Release (§7).
**Versioned independently of the library**, tagged `relay-v<semver>`, and built from one exact
library release that every image names (§7.3).

This document specifies `tandem-relay`, a prebuilt runnable application that hosts the Tandem
relay — and, optionally, the Admin API — as its own deployable. It is the **split topology**
of [HLD.md](HLD.md) §3.2 delivered as an artifact instead of as instructions.

---

## 1. Purpose & scope

The split topology is a documented, supported deployment: the relay runs as its own process
pointed at the client's outbox database and Kafka, so the client application depends only on the
write-side and never on the Kafka client. HLD §3.2 promises it, the README lists it, and
`tandem-spring-relay` implements every moving part of it — but an adopter who wants it today must
still create a Spring Boot project, add the dependency, write a main class, and build their own
image. This module removes that step.

**The gap is small and it is not in the engine.** `tandem-spring-relay`'s autoconfigurations already
contribute the topic router, the Kafka dispatcher, the outbox store, the bucket source, the
relay control source and the `WorkerPool`; `RelayLifecycle` already starts it after the context is
built and drains it before teardown; `tandem-relay-reference.yml` already documents every bound
key. What is missing is an application around them, and a way to ship it.

**In scope:**

- a Spring Boot application whose entire job is to host those beans (§3);
- a health contribution over the relay's in-process state, which the library deliberately does not
  ship (§5);
- the deployment defaults a library has no business setting: ports, probes, shutdown (§6);
- packaging as a container image and an executable jar, and their release path (§7);
- tests that run the application as shipped, started as a process against a real database and
  broker (§8).

**Out of scope:**

- **Any new relay capability.** This module contributes no engine behaviour, no new port, and no
  new `tandem.*` key bound by the library. Every relay tunable stays exactly where it is, in
  `tandem-spring-relay`. If this module ever needs a knob the library does not have, that is a
  signal to add it to the library, not here.
- **Any transport other than Kafka.** The image publishes to Kafka and to nothing else (§3.2).
- **Applying the schema.** The image does not create, migrate or verify the `tandem_*` tables
  (§6.4).
- **Authentication for the Admin API.** Tandem ships endpoints, not an auth policy
  (HLD-admin-api §3); the image inherits that position and defends it with defaults (§4.2).
- **A native image / GraalVM build.** A relay process starts once and runs for weeks; startup
  time and RSS are not what an outbox deployment is limited by, so the second toolchain buys
  nothing (Pareto, AGENTS.md).

### 1.1 Q23's third question is already answered

Q23 asks for "main class, config binding, packaging (JAR/Docker), how it receives N / shard
assignment (ties to Q8)". The first three are this document. **The fourth no longer exists:**
under `LEASE` there is no `N` to configure and no assignment to receive. An instance registers
itself in `tandem_relay_member`, the fair-share divisor counts live members, and buckets are
claimed and rebalanced dynamically (HLD §3.2, LLD-jdbc §3.2). Scaling the deployment up or down
needs no configuration change on any instance. That part of Q23 is recorded as resolved, not
designed.

---

## 2. JSON binding: stock Boot 4, Jackson 3

The image is a **stock Spring Boot 4 application**: `spring-boot-starter-webmvc` brings Jackson 3
(`tools.jackson`) and no Jackson 2 databind, and the image does not add `spring-boot-jackson2` to
opt back.

That is the classpath `tandem-admin` is built for. The module names Jackson's annotations only,
never either generation's databind, and renders the stored `payload` as raw JSON text
(LLD-spring-config §1.3), so the Admin API role (§4) needs nothing from this module to work on
Jackson 3. `tandem-spring-relay` touches no Jackson type at all.

This module therefore declares **no Jackson dependency of its own** and configures no mapper. A
change here that needs one is a signal that JSON handling is leaking out of `tandem-admin`.

---

## 3. Module layout

`tandem-relay/` is a Gradle subproject like any other, but it is an **application, not a library**,
which changes three things: it is listed in the root build's `unpublishedModules` (so it opts out
of the shared java-library/publishing convention and configures its own toolchain and tasks), it
carries **concrete** Spring dependencies rather than `compileOnly` ones, and it pins a Java version,
a Spring Boot version and a Tandem library version of its own.

```
tandem-relay/
├── build.gradle.kts
├── Dockerfile
├── .dockerignore
├── docker-compose.example.yml
└── src/
    ├── main/java/com/codingful/tandem/relay/
    │   ├── TandemRelayApplication.java     @SpringBootApplication, and the scan for the one properties class
    │   ├── RelayVerdict.java               §5.1, the verdict: a pure function of one reading
    │   ├── RelayHealthIndicator.java       §5.1, fetches the reading and asks the verdict
    │   ├── RelayHealthProperties.java      §5.2
    │   ├── RoleCheck.java                  §4, refuses a process with no role, warns on an open Admin API
    │   └── package-info.java
    ├── main/resources/
    │   └── application.yml                 §6.1, the image's defaults
    ├── test/java/com/codingful/tandem/relay/            §8.1, no Docker
    │   ├── RelayVerdictTest.java
    │   ├── RelayHealthPropertiesTest.java
    │   ├── RelayHealthIndicatorTest.java
    │   ├── RoleCheckTest.java
    │   └── ShippedConfigurationTest.java
    └── integrationTest/java/com/codingful/tandem/relay/  §8.2, the jar as a process
        ├── RelayProcess.java
        └── TandemRelayApplicationIT.java
```

Five small classes, one of which is empty, and no autoconfiguration, no post-processor, no
`spring.factories`. That ratio is the point: anything that grows here beyond wiring and packaging
belongs in `tandem-spring-relay` instead.

### 3.1 Runtime baseline: Java 25, Spring Boot 4.1

The library modules compile against the Boot 3.x baseline with Spring `compileOnly`, so that one
artifact serves both generations (LLD-spring-config §1.1). **An application cannot do that** — it
must resolve a concrete Spring Boot version and run on it.

**Spring Boot 4.1, Java 25.** Nothing about this choice reaches an adopter: the image is a
deployable, so its Boot version and its JVM are implementation details that constrain no
consumer's classpath and appear in no published POM. That freedom is what makes the newer line the
better pick — the longest support runway, and, as a side effect, the project's **first real
end-to-end validation of the single-artifact bet**: a real Boot 4 application, assembled by the
Boot plugin and started as a process, where the library modules' `bootFourTest` and
`jacksonThreeTest` gates re-run unit tests on a swapped classpath.

Java 25 over 17 for the same reason (an application, not a library): it is the current LTS, so
the longest support runway, with no consumer to constrain. It is also the JVM `tandem-benchmark`
runs on, so the image ships on the JVM the relay's published figures were measured on (the JVM
only: the Kafka client is another matter, §3.2). The toolchain provisions it for this module alone; the library modules stay on 17, and so does the JDK
the CI and release workflows install.

### 3.2 Dependencies

| Dependency | Why |
|---|---|
| The Spring Boot BOM, at the application's own catalog version (§7.1) | Version alignment for everything below |
| `spring-boot-starter-webmvc` | The web container the Admin API and Actuator need. Boot 4's name for it: `spring-boot-starter-web` still resolves and is deprecated |
| `spring-boot-starter-jdbc` | `DataSourceAutoConfiguration` + HikariCP |
| `spring-boot-starter-actuator` | Health endpoint and probe groups (§5). On Boot 4 it also brings the Micrometer metrics autoconfiguration, which the Tandem adapter orders itself after |
| `com.codingful:tandem-bom` at the pinned library version (§7.3) | The application consumes Tandem the way an adopter does: by published coordinate, versions from the BOM. The working tree stands in for them during development |
| `tandem-spring-relay` | The relay itself |
| `tandem-kafka` | The transport. `tandem-spring-relay` carries no adapter and wires whichever one is on the classpath, so the application must supply it; without it the context has no `OutboxDispatcher` |
| `tandem-admin` | The optional second role (§4) |
| `tandem-micrometer` + a registry | The `TandemMetrics` adapter; Prometheus registry (§4.3) |
| `libs.postgresql` | The JDBC driver — the application must supply it, `tandem-jdbc` never does (§9) |
| `logback-classic` (via the starter) | A concrete backend: this is a leaf app, not a library (AGENTS.md §Logging) |

`tandem-tracing-otel` is deliberately **absent**. Instrumented-mode span emission on the relay side
comes from `tandem-spring-relay`'s own Micrometer Tracing adapter when the application runs
Micrometer Tracing; adding the OTel module too would put two span recorders in one image for a
feature that is off by default. An adopter who wants OTel wiring instead can extend the image.

**The Kafka client is the one Spring Boot manages, not the one `tandem-kafka` declares.** The library
declares `kafka-clients` 3.9.2, the line its CloudEvents binding targets and the one every
Docker-bound test and every benchmark runs on. The Boot 4 BOM manages the 4.x line (4.2.1 with
Boot 4.1.1), and in this application the BOM wins, exactly as it does in any Boot 4 application
that adds `tandem-kafka`. The image takes it rather than forcing 3.9.2 back: an image that differed
from what a Boot 4 adopter runs would test nothing they care about. The consequence is stated, not
hidden: this module's integration test (§8.2) is the project's end-to-end gate for the relay on
the 4.x client, and the published benchmark figures are for 3.9.2. Micrometer moves the same way,
from the library's 1.13 baseline to the BOM's 1.17, within the range LLD-micrometer §1 already
declares.

`tandem-rabbitmq` is **absent** too, and the image is Kafka-only by decision. The connector has no
autoconfiguration: an application relaying to RabbitMQ contributes its own `OutboxDispatcher` bean
(guide/rabbitmq.md §4.1), which a prebuilt image has no place to take from. It is also versioned
independently of the library (LLD-rabbitmq §9), so bundling it would tie one image tag to two
version lines. Relaying to RabbitMQ stays the assemble-it-yourself path over
`tandem-spring-relay`.

---

## 4. Roles — one image, chosen by property

The image is a single artifact that can run as a relay, as an Admin API, or as both. The gates
already exist in the library and neither is invented here:

| Role | `tandem.relay.enabled` | `tandem.admin.enabled` | Notes |
|---|---|---|---|
| **Relay** (default) | `true` (default) | `false` (default) | The Pareto case: the split relay of HLD §3.2 |
| **Admin API** | `false` | `true` | The standalone deployment HLD-admin-api §4.1 promises and never shipped |
| **Both** | `true` | `true` | One process, one DB connection pool; convenient for small deployments |

The two roles genuinely compose: the Admin API acts on the outbox through the database, not
through the relay process, and relay control (pause/resume) is DB-mediated through `tandem_meta`
and `tandem_bucket_lease` (HLD-admin-api §4.1). Hosted together they share one `OutboxStore`, the
relay's, so `tandem.relay.max-attempts` means the same thing in every role. The one thing that
does change in the same JVM is readiness, which then speaks for both (§5.2).

**A process with neither role is refused.** With `tandem.relay.enabled=false` and
`tandem.admin.enabled` left at its default the application would start, report ready and do
nothing, which is the silent kind of misconfiguration. `RoleCheck` fails startup instead, with a
message naming both keys.

A `tandem-admin` deployable also unblocks `tandem-cli`'s integration test against a live server,
deferred in LLD-cli.md §10 for exactly this reason.

### 4.1 Ports

The Admin API and the management endpoints listen on **separate ports**, so they can be exposed
and firewalled independently:

| Port | Default | Serves |
|---|---|---|
| `server.port` | `8080` | The Admin API (`/tandem/admin/v1/**`), only when enabled |
| `management.server.port` | `8081` | Actuator: health, probes, Prometheus scrape |

In the default relay-only role the application port listens and answers 404 to everything: no
route is mapped on it. The process still starts a web container because `management.server.port`
needs one. That is the cost of making probes and the
metrics scrape available at all, and it is the reason `spring-boot-starter-webmvc` is unconditional
in §3.2.

### 4.2 Security posture

`tandem.admin.enabled` defaults to **false** and the image does not change that. When an operator
turns it on they get an unauthenticated management surface, exactly as documented in
HLD-admin-api §3 — Tandem ships endpoints, not an auth policy. The image's obligations are to
default it off, to keep it on its own port so it can be bound to an internal network, and to say
so unmissably in the image documentation and in a startup `WARN` emitted when the Admin API is
enabled (`RoleCheck`, the same class that refuses a process with no role). The warning lives here
and not in `tandem-admin`: an application that embeds the Admin API chose its own security around
it, while this image is the one place where it can be switched on with nothing in front. It does
not ship a default credential, since a default credential is worse than none.

### 4.3 Metrics

`tandem-micrometer` plus a Prometheus registry are in the image because a standalone relay with no
metrics endpoint is not operable, and the adapter is inert without a `MeterRegistry`
(LLD-micrometer §5). The scrape endpoint is exposed on the management port and, unlike the Admin
API, is **on by default** — it is a read-only surface that exposes counts and timings, never
payloads (AGENTS.md §Logging rule 5 applies to metric tags too).

---

## 5. Health — where Tandem is finally allowed to have an opinion

The library deliberately ships **no** health verdict: `WorkerPool.status()` returns a
database-free `RelayStatus` reading (LLD-jdbc §3.8) and `RelayStatus`'s own javadoc states that
deciding what an acceptable worker deficit or cycle age is belongs to the embedding application,
exactly like routing logs or exporting meters.

**In this module Tandem *is* the embedding application.** That is not a reversal of the earlier
decision, it is the half of it that had no home before: the threshold has to be written somewhere,
and a prebuilt image with no readiness signal is not deployable on any orchestrator. The library
keeps shipping no verdict; the deployable ships one.

### 5.1 The verdict

The verdict is a **pure function** of one `RelayStatus` reading, the current instant and the stall
threshold (`RelayVerdict`). `RelayHealthIndicator` only fetches the reading and asks it. Nothing on
this path touches the database, which is what makes it safe at probe frequency on every instance
forever, and what lets every row below be tested without a database and without a mock (§8.1).

| Condition | Verdict |
|---|---|
| No relay in this process (Admin-API-only role) | `UNKNOWN` |
| `state == STOPPING` | `OUT_OF_SERVICE` |
| `state == STOPPED` | `DOWN` |
| `workersAlive == 0` | `DOWN` |
| `oldestWorkerCycle` older than the stall threshold | `DOWN` |
| `workersAlive < workersConfigured` | `UP` with the deficit in the details |
| otherwise | `UP` |

`STOPPING` is kept apart from `DOWN` for the reason `RelayStatus.State` keeps it apart from
`STOPPED`: a relay draining at shutdown is leaving on purpose, and a log or a dashboard should be
able to tell that from a failure. Both answer the probe with 503.

A **worker deficit is not DOWN**, and that is deliberate: a died worker is restarted automatically
(LLD-jdbc §3.1), so a transient gap is normal operation. A *persistent* gap is what matters, and
the signals for it are the deficit in the details and the `tandem.outbox.workers.active` gauge,
not a probe that flaps.

The **stall threshold is the load-bearing condition**, because it is the only one that separates a
relay that is running from one that is merely started. A cycle is stamped only when it completes
without throwing, so the reading ages in every case where the relay is not making progress,
whatever the cause:

- the database is unreachable, and every cycle fails;
- the broker's metadata is unavailable, and each send of the batch blocks the worker for the
  producer's `max.block.ms` before failing;
- a worker is blocked in a call that never returns.

`DOWN` therefore means "this relay is not delivering", not "this process is broken". The details
and the logs say which of the three it is.

**Pause is `UP`.** An operator who paused the relay through the Admin API intended it; reporting
`DOWN` would make an orchestrator act on a deliberate state. The verdict needs no special case: a
paused worker claims nothing but keeps completing cycles, so the reading stays fresh. The details
carry `paused`, read from `RelayControlSource.wholeRelayPaused()`, which is a cached in-memory
value by contract and so costs no database access here either.

Details always carry `instanceId`, `state`, `coordination`, `paused`, `workersConfigured`,
`workersAlive` and `cycleAgeSeconds`: the identifiers needed to find the affected instance without
re-running anything, the same rule AGENTS.md applies to `ERROR`/`WARN` logs. Its alerting twin is
the `tandem.outbox.workers.cycle_age_seconds` gauge, which reports the same age continuously
instead of at probe time.

**The indicator carries no bean condition.** It takes `ObjectProvider<WorkerPool>` and answers
`UNKNOWN` when there is none, instead of being `@ConditionalOnBean(WorkerPool.class)`. Two reasons,
both about failing silently. A bean condition on a component-scanned class is evaluated before any
autoconfiguration has registered the `WorkerPool`, so it would never match and the indicator would
be absent in every role (the ordering rule of AGENTS.md, seen from the application side). And the
readiness group of §5.2 names this contributor: Actuator validates group membership at startup, so
a contributor that exists in one role and not in another would stop the Admin-API-only role from
starting. `UNKNOWN` does not lower a group's aggregate status.

### 5.2 Readiness, and what it is for

The indicator joins the **readiness** group. A custom indicator is not in that group by itself, so
`application.yml` (§6.1) includes it by name. The names line up as follows, and all three are part
of what an operator sees:

| What | Name |
|---|---|
| Bean | `tandemRelayHealthIndicator` |
| Health contributor, the name used in group membership and in the response | `tandemRelay` |
| Properties | `management.health.tandem-relay.*` |

**Probes use the group paths, never the root.** `/actuator/health/readiness` and
`/actuator/health/liveness` on the management port. The root `/actuator/health` also carries Spring
Boot's own `db` indicator, which runs a query on every call: useful to a person, wrong for a probe.
The compose example's healthcheck points at the readiness path.

What readiness does depends on the role, and the image documentation says so plainly:

- **Relay-only.** The pod serves no traffic and usually has no Service, so an unready relay is not
  "taken out of service": nothing routes to it in the first place. Readiness there gates a rollout
  (a new version that cannot deliver never replaces the old one) and feeds alerting.
- **Both roles.** A relay that is not delivering makes the pod unready, which removes the Admin API
  from its Service at the moment an operator wants it. That is a limit of running both roles in one
  process. A deployment that relies on the Admin API in production runs the two roles as two
  deployments of the same image.

One property, `management.health.tandem-relay.stalled-after`, default **60s**. It sits under
Actuator's own `management.health.<name>` convention rather than under `tandem.*`, because it
configures a health contributor, not the relay, and because `tandem.relay.*` is bound by the
library's `TandemRelayProperties` and must not acquire keys the library does not know about.

**The threshold is checked against the relay's own idle wait at startup.** An idle worker waits up
to `tandem.relay.poll-interval` plus 20% of jitter between two cycles, and with the `pg-notify`
wakeup a long `poll-interval` is the point of the feature (dispatch-latency.md), so a fixed 60s
would report a healthy, idle relay as `DOWN`. The application therefore refuses to start unless
`stalled-after` exceeds 2.4 times `poll-interval` (two consecutive waits, each at its longest),
with a message naming both keys and both values. Same shape as the library's own check of
`row-lease` against `delivery.timeout.ms`, and it reads `RelayConfig`, which is already a bean.
With the defaults the margin is 60s against 240ms.

One knob, not four: a threshold per condition would be configuration surface nobody tunes.

**Liveness stays Spring's own "the process responds"**, and the indicator is not part of it. A
liveness failure restarts the container, and a restart cures exactly one of the three causes of a
stall listed in §5.1: a worker blocked in a call that never returns. For the other two it is noise
at best. A liveness threshold would have to stay above the longest stall a restart does *not* cure,
and that one is long: with broker metadata unavailable a single cycle lasts up to `batch-size`
times the producer's `max.block.ms`, 100 minutes at the defaults. Below that the orchestrator
would restart a healthy process in a loop for as long as a broker, or one topic, is missing.

The blocked call is handled at its cause instead, with a socket timeout on the image's datasource
(§6.1): the call fails, the cycle throws, and the worker carries on with a fresh connection.

---

## 6. Configuration

### 6.1 What the image sets, and what it refuses to set

The application binds **no `tandem.*` key of its own**. Every relay tunable comes from
`tandem-spring-relay`'s existing property classes, documented in `tandem-relay-reference.yml`,
which stays the single reference. The image's `application.yml` only sets deployment defaults that
have no meaning inside a library:

```yaml
server:
  port: 8080
  shutdown: graceful
management:
  server:
    port: 8081
  endpoints:
    web:
      exposure:
        include: health,info,prometheus
  endpoint:
    health:
      probes:
        enabled: true
      show-details: always
      group:
        readiness:
          include: readinessState,tandemRelay
spring:
  application:
    name: tandem-relay
  datasource:
    hikari:
      data-source-properties:
        socketTimeout: 30
```

`show-details: always` is what puts the details of §5.1 in the response. They are structural
identifiers and counts, never data, and they are served on the management port only.

**A socket timeout on the datasource, 30 seconds.** Nothing in `tandem-jdbc` bounds a database
call, by design: a timeout is a property of the connection, and the connection is the
application's. Here Tandem is the application. Without one, a connection that dies without closing
(a failover, a dropped NAT entry) leaves a worker waiting forever, and under `LEASE` that instance
keeps renewing its bucket leases from another thread, so no other instance ever takes those
buckets over. With one, the call fails after 30 seconds, the cycle throws, the pool discards the
connection and the worker continues. 30 seconds sits below the stall threshold of §5.2, so the
relay recovers before it is reported down, and far above any statement the relay or the Admin API
issues. The wakeup listener is unaffected: it waits for notifications with a timeout of its own.

It is set through Hikari's driver properties rather than in the URL because the URL is the
operator's. An operator who wants another value puts `socketTimeout` in the URL, which the driver
reads in preference to this default. The embedded relay gets no such default: there the datasource
belongs to the host application, which is the reason this lives in the image and not in the
library (§6.1's own rule, a deployment default with no meaning inside a library).

**Shutdown is bounded by the container runtime, not by a Spring property.** On `SIGTERM` the
context closes and the relay stops in two steps:

1. `WorkerPool.stop()` stops scheduling, interrupts the workers, waits for each to finish its
   current cycle (at most 10s per worker), records what has already been acknowledged and releases
   the instance's buckets. It does not wait for sends still in flight.
2. The Kafka producer is closed afterwards, with the other beans, and that waits for in-flight
   sends for up to `delivery.timeout.ms` (30s by default). Their acknowledgements are no longer
   recorded: those rows stay `IN_FLIGHT` until the row lease expires and are delivered again.

So a clean stop already costs a few duplicates, which at-least-once delivery allows, and never a
loss or a reordering. What the operator must size is the **grace period of the runtime**, because
that is the only thing that cuts the sequence short: `docker stop` waits 10s and Kubernetes 30s
before `SIGKILL`, and both are shorter than the worst case above. The compose example sets
`stop_grace_period: 60s`, and the image documentation gives `terminationGracePeriodSeconds: 60` and
the rule behind it: the worker wait plus `delivery.timeout.ms`. A kill before that costs more
duplicates and, under `LEASE`, a wait for the bucket leases to expire before another instance
takes the buckets over.

**Two required values with no default**, both failing fast and by name: `spring.datasource.url`
(plus credentials) and `tandem.kafka.source`, the CloudEvents source URI. The latter already fails
with a message naming the property (`TandemKafkaAutoConfiguration`, which is gated on
`tandem.relay.enabled` and so demands nothing in the Admin-API-only role); the former is Boot's
own error.

**`tandem.relay.coordination` keeps the library default, `SINGLE`.** A container image invites
scaling, and scaling a `SINGLE` deployment to two replicas is a misconfiguration — not a
corruption (ordering and single-claim exclusivity are carried at the row by `IN_FLIGHT` +
`SKIP LOCKED`, HLD §3.2), but every instance then re-scans every bucket for no gain. The
temptation is to default the image to `LEASE`. **Rejected:** the same configuration would then
mean different things embedded and standalone, which is a worse trap than the one it avoids. The
`docker-compose` example ships with `LEASE` set and two replicas, and the image documentation
states the rule where an operator meets it, including its Kubernetes form: a rolling update of a
`SINGLE` deployment runs two instances for a moment, so `SINGLE` goes with `strategy: Recreate`.

**`tandem.outbox.wakeup` keeps the library default, `none`**, for the same reason. With
`pg-notify` the relay holds one extra connection listening on the `tandem_wakeup` channel, and the
write side must emit the same mechanism or nothing is ever signalled (LLD-jdbc §3.10): it is a
setting of the pair, not of the image. The `docker-compose` example has no write side of its own;
it turns the wakeup on for its relays, with a comment that the write side must emit the same.
The image documentation carries the one deployment caveat that belongs to a container: a
connection pooler in transaction-pooling mode silently breaks `LISTEN`, so the relay's datasource
must reach PostgreSQL directly or through session pooling. A mismatch costs latency only, since
the relay keeps polling.

### 6.2 Configuring from the environment

Environment variables are the primary configuration channel of a container, and **every setting
the image needs is reachable through one, with no code in this module**. That includes the one
that looks as if it would not be: `tandem.kafka.producer` is a `Map<String, String>` handed to the
Kafka producer, so its keys are Kafka's own dotted names (`bootstrap.servers`, `security.protocol`,
`sasl.jaas.config`). When the target is a map, Spring's binder joins the remaining words of the
variable name with dots, so `TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS` reaches the producer as
`bootstrap.servers`. Kafka's setting names are lowercase dotted words, which is exactly the shape
this produces.

Because the image's documentation rests on that behaviour, it is pinned where it lives: a test in
`tandem-spring-relay` binds the producer map from environment variables alone and compares the
keys with Kafka's own constants, on all three Spring Boot lines. The integration test of this
module then configures the real process through its environment only (§8.2).

A setting that cannot be spelled as a variable name stays reachable through a mounted YAML file or
`SPRING_APPLICATION_JSON`. **Secrets** (`sasl.jaas.config`, the datasource password) are better
mounted as files and imported with `spring.config.import=configtree:`, which the image
documentation shows. Only `health`, `info` and `prometheus` are exposed, so no Actuator endpoint
can print the environment back.

### 6.3 Instance identity

`tandem.relay.instance-id` defaults to a derived `tandem-<host>-<pid>-<rand>`: the first 30
characters of the hostname, the process id, and 16 random bits. That derivation is weak in a
container. The hostname is the pod name, and a Deployment's pod names carry what distinguishes them
at the **end** (`<release>-tandem-relay-<replicaset>-<pod>`), so a long release name leaves every
replica with the same 30 characters. The process id is the same in every container. Uniqueness then
rests on the 16 random bits alone, and under `LEASE` two instances sharing an id would both believe
they own the same buckets.

The image documentation therefore gives `TANDEM_RELAY_INSTANCE_ID` set from the pod name (the
downward API on Kubernetes; on compose, which has no such mechanism, a value per service, the
service's name in the example) as **the** configuration for more than one replica, not as a nicety. The value is capped at 64 characters by the library. It also makes
logs, the lease table and Admin API output correlate. A vanished owner needs no cleanup either
way: its lease expires and is reclaimed.

### 6.4 The schema is not the image's job

The image **does not create, migrate or verify** the `tandem_*` tables. The schema is delivered
as files under `schema/postgres/`: a Liquibase changelog (`changelog/db.changelog-master.xml`) for
whoever migrates an existing database, and `tandem-baseline.sql`, the same schema flattened into
one script, for a fresh one. Applying either is the job of whoever owns the database, and the
image bundles neither Liquibase nor the scripts. The `docker-compose` example mounts
`tandem-baseline.sql` into PostgreSQL's `docker-entrypoint-initdb.d`, so the database container
applies it, not the relay. Two reasons to keep it that way: applying DDL requires privileges a
relay should not hold, and the relay is not the only writer to that schema (the client's write-side
is), so a relay that migrates on startup would be one participant unilaterally changing a contract
the other depends on.

The startup guards that do exist stay, and they run **in the relay roles only**:

- `BucketCountGuard` fails startup on a bucket count that differs from the stored one. On the very
  first start against a database it stores the configured value, so the relay's database user
  needs `INSERT` on `tandem_meta` besides the row-level access the engine uses. That is the only
  write the guards make, and it is data, not DDL.
- Under `LEASE`, `BucketLeaseManager` fails with a message naming the missing lease tables.

Neither covers a missing `tandem_outbox`. Under `SINGLE` the relay then starts, logs an `ERROR` per
cycle and turns `DOWN` at the stall threshold, which is the readiness signal doing its job. In the
Admin-API-only role no guard runs at all, and a missing schema surfaces as a failed first request.

---

## 7. Packaging & distribution

### 7.1 Executable jar

The Spring Boot Gradle plugin is applied to this module only, producing a layered executable jar.
It is the first use of that plugin in the project, which is why it is scoped to the one module
that is an application, and why the version catalog gains its first `[plugins]` entry.

**The application has its own Spring Boot version in the catalog**, separate from the
`spring-boot-v4` entry of the compatibility matrix. The two will usually hold the same value and
mean different things: the matrix entry states which Boot 4 line the library is verified against
and moves as a compatibility decision, while this one is the runtime the image ships and moves
whenever that runtime needs a fix. Sharing one entry would turn a security update of the image
into a change of the library's verified line.

**Two jars are built from the same application classes, and they differ only in where Tandem comes
from** (§7.3):

| Task | Tandem modules | Jar | Used by |
|---|---|---|---|
| `bootJar` | The working tree | `tandem-relay-<version>-worktree.jar` | `integrationTest`: the application against the library as it stands on `main` |
| `pinnedBootJar` | The pinned release, resolved from Maven Central | `tandem-relay-<version>.jar` | `pinnedTest`, the image, the release |

The pinned jar carries the plain name because it is the one that ships.

### 7.2 Container image

A hand-written multi-stage `Dockerfile` over `eclipse-temurin:25-jre`.

**Chosen over `bootBuildImage`/buildpacks**, which would produce an image with no Dockerfile at
all. Buildpacks hide the base image behind a builder image, which makes "which base am I running,
and has it been patched" an indirect question; they need a Docker daemon and a large builder pull
at build time; and they are a second, unfamiliar mechanism in a project that has no other
container tooling. A short Dockerfile is legible, patchable and explicit, the same preference
that made this project hand-write its Admin API server rather than generate it.

What the Dockerfile does, each line of it a decision:

- **It packages the jar Gradle built**, the pinned one of §7.1, copied in from `build/libs`, and
  compiles nothing. The image and the jar attached to the release are then the same bits. The build
  context is the module directory, and its `.dockerignore` admits `build/libs/tandem-relay-*.jar`
  except the `-worktree` one, and nothing else:

  ```
  ./gradlew :tandem-relay:pinnedBootJar
  docker build $(./gradlew -q :tandem-relay:imageBuildArgs) --tag tandem-relay:local tandem-relay
  ```
- **The version and the pin reach the image as two build arguments**, `RELAY_VERSION` and
  `TANDEM_VERSION`, because a label can take its value from nothing else. They have one source: the
  `imageBuildArgs` task prints the project version and `tandemPin`, the same two values the jar's
  `build-info.properties` carries, and every build (local, CI, release) takes them from it. The first
  stage then checks both against that `build-info` and fails on a mismatch, so a stale jar or a
  hand-typed value cannot produce a label naming another release than the one inside. The version also
  names the jar the stage copies, so a missing argument fails the build at once.
- **Layers are extracted with Boot's `tools` jar mode** (`java -Djarmode=tools -jar … extract
  --layers`), so the dependency layers cache independently of the application layer. That stage
  runs on the build platform (`FROM --platform=$BUILDPLATFORM`): the extracted layers are the same
  on every architecture, and running a JVM under emulation for it would only slow each build.
- **`ENTRYPOINT` is in exec form** (`["java", "-jar", "application.jar"]`), so the JVM is the
  container's first process and receives `SIGTERM` itself. A shell in front of it would swallow
  the signal and turn every stop into the kill that §6.1 sizes the grace period to avoid.
- **JVM flags travel in `JAVA_TOOL_OPTIONS`**, set to `-XX:MaxRAMPercentage=75` so the heap follows
  the container limit. An operator changes or extends it with one environment variable, without
  rewriting the entrypoint.
- **A numeric, non-root `USER`**, `10001:10001`, created in the image as `tandem`. Numeric because
  Kubernetes' `runAsNonRoot` can verify a number and cannot verify a name; a user of its own rather
  than the base image's `ubuntu` (1000), so the id does not depend on what the base happens to ship.
- **The licence travels with the redistribution.** `LICENSE` and `THIRD-PARTY-NOTICES.md` (§10)
  are copied into the jar's `META-INF` by `pinnedBootJar`, and the image takes them **from the jar**,
  into `/licenses`: a second extraction (`extract --launcher --layers application`) unpacks the
  application layer as plain files, which is also where the `build-info` checked above lives. The
  jar and the image therefore carry the same two files by construction, and the build context needs
  nothing but the jar. The image redistributes the whole Spring Boot runtime, and a notice that
  exists only in the repository does not accompany it.
- OCI `org.opencontainers.image.*` labels for title, description, source, version and licence,
  plus `com.codingful.tandem.library.version` carrying the library version inside (§7.3). The
  release adds `revision` and `created`, both taken from the tagged commit. **No `HEALTHCHECK`**
  instruction, since orchestrators use the probes of §5.2 and the compose example wires one
  explicitly.

The base image carries **no `curl`, no `wget` and no `nc`**, and this image adds none: a package
installed only for a probe is attack surface every deployment pays for, and Kubernetes' `httpGet`
probes run outside the container anyway. The compose example's healthcheck therefore sends its request
through `bash`'s `/dev/tcp`, which the base image already has, and checks for a `200` status line.

The image is **multi-architecture**, `linux/amd64` and `linux/arm64`, since arm64 is both a common
development machine and a common production instance type.

### 7.3 Versioning and release

The image goes to **GHCR** as `ghcr.io/alirux/tandem-relay`, and the executable jar is attached to
a GitHub Release. Nothing of this module goes to Maven Central.

**The image has its own version, on its own tag: `relay-v<semver>`**, released by its own workflow,
`relay-release.yml`. The globs are anchored like the other schemes': the library's `v*` does not
match `relay-v*`, nor the reverse, so neither release path fires on the other's tag and
`release.yml` is untouched.

It is the library packaged, and still not the library's version, because the package is more than
the library: a Spring Boot runtime, a JDK, a base image and a deployment contract, all of which
change on a cadence of their own. A vulnerability fixed in Tomcat or in the base image needs a new
image and nothing else. Under a shared version that fix would be a release of every library module
with not one line changed, staged and published by hand; under its own, it is a patch of the image
and one tag.

#### The pin

**An image contains exactly one library release, and it comes from Maven Central.** The module
depends on Tandem by published coordinate, through `tandem-bom` at the version in `tandemPin`
(`tandem-relay/build.gradle.kts`). During development the root build substitutes the working tree
for those coordinates, as it does for every independently versioned module, so the repository
stays one buildable unit. The pinned classpath is the one configuration exempt from that
substitution, the same mechanism `tandem-rabbitmq` uses for its floor. One detail is specific to
consuming the BOM: it is requested as a platform and the root build substitutes it as one, since a
plain project substitution asks `tandem-bom` for a library variant it does not have.

The difference from that connector is what the number means. A connector is used *with* a range of
library versions and declares the oldest, a floor. An image *contains* one, so the pin is exact.
Building from the working tree at tag time instead would put unreleased library code in an image,
and "which Tandem is running here" would have no answer, on a deployment where the relay, the
write side and the Admin API may each run a different version against the same database
(HLD §1.4).

Two gates, both in `check`, both running the integration test of §8.2:

- `integrationTest`, on the jar built from the working tree. It is what tells the library's next
  release apart from one that would break the application.
- `pinnedTest`, on the pinned jar. It is what ships, tested as it ships.

A change to this module that needs a library change waits for the library release that carries it,
and then moves the pin. **The first pin is the first library release in which every relay
autoconfiguration orders itself after the beans it depends on** (LLD-spring-config §4.4): earlier
releases do not start as a relay-only application at all.

#### What the version means

The image's contract is what an operator's manifests depend on, and it is not the Java API:

| Part of the contract | Not part of it |
|---|---|
| The image name | The Spring Boot, JDK and base image versions |
| The two default ports, and which one serves what | The exact set of health details |
| The probe paths | The layout inside the container |
| The default role | The rendering of log lines |
| The user id the process runs as | |
| Being configured by the library's `tandem.*` keys and Spring's own | |

Breaking a row on the left is a breaking change of the image. The bump rules, with the usual `0.x`
reading where a minor signals a break:

| Change | Bump |
|---|---|
| Rebuild on a newer base image, Spring Boot or dependency patch, a fix in this module | Patch |
| The pin moves to a library patch | Patch |
| The pin moves to a library minor, or this module gains a capability | Minor, never a patch |
| A contract row above changes, or the pinned library release itself breaks an operator (a schema migration to apply first, a change in the published envelope) | Breaking |

**The library version is stated wherever the image version is**, since nothing in the number
implies it: in the release notes, in a label on the image, on the `info` endpoint the image already
exposes, and in one `INFO` line at startup. The user guide keeps the table of image versions and
the library release each contains.

#### The release

Pushing a `relay-v*` tag runs `relay-release.yml`, which is fully automatic, because nothing in it
is irreversible the way a Maven Central publication is:

1. `pinnedTest` and the notices check (§10), so the tagged commit is verified against the pinned
   release before anything is pushed (Docker is available on the runner). `RELAY_VERSION` is taken
   from the tag first, refused unless it is `<major>.<minor>.<patch>` with an optional pre-release
   suffix (an image tag cannot carry semver build metadata), and set for every later step, so the jar
   under test is the jar that ships.
2. Build the pinned jar with that `RELAY_VERSION`. Outside a release the variable is unset and the
   build is a snapshot; the workflow is the only place that pushes.
3. Build the multi-architecture image and push it to GHCR under the version, and under `latest`
   **only when the version has no pre-release suffix**. Version tags are never moved: a rebuild is
   a new patch, and when the version tag already exists on GHCR the step pushes nothing.
4. Create the GitHub Release from the annotated tag, with the jar attached and `--latest=false`,
   so the repository's "Latest" badge stays the library's (same as the CLI and the connector).

A failed run is simply run again; there is no staging to repeat. Because an existing version tag is
left alone, a run repeated after step 4 failed publishes the release without moving the image.

Because the image is built from artifacts already on Maven Central, **an image release follows a
library release, it never accompanies one**: the library is tagged, published by hand, and only
then is the pin moved and the image tagged. Skipping that last step breaks nothing, the image just
stays on the previous library release, which is why it joins `tools/javadoc-io-sync.sh` in the
list of what follows a library release.

A rebuild for a vulnerability in the base image alone changes no file. It is still a new patch
tag, on the same commit, whose notes say what was rebuilt and why.

**`ci.yml` builds the image and runs it on every change, without pushing** (§8.3), so a broken
Dockerfile fails at PR time rather than at release time.

**The package is public with the repository.** The image is pushed by the workflow with its
`GITHUB_TOKEN`, and the `org.opencontainers.image.source` label (§7.2) links the package to this
repository, so it takes the repository's visibility: the first push, `relay-v0.1.0`, was pullable
without authentication straight away. A package that ever came out private, pushed some other way,
would be switched to public once from its settings on GitHub.

Before tagging, the breaking-change check is scoped to the contract above, and the notes state the
pin. Same annotated-tag-as-release-notes convention and the same "ask before tagging" rule as the
other schemes.

---

## 8. Testing

Three levels, each covering what the one below cannot.

The module is unpublished, so — like `tandem-sample-spring` — it declares its own JUnit/AssertJ
dependencies and its own `integrationTest` task rather than inheriting the shared convention, and
it is **not** added to `tandem-coverage`'s aggregation (published, tested modules only).

### 8.1 Unit tests: the verdict and the startup checks

No Docker, no Spring context. `RelayStatus` is a public record, so the verdict is exercised by
building readings and asking it (§5.1), with a fixed instant in place of a clock. One scenario per
test, in the project's `GIVEN_WHEN_THEN` form:

- each row of the verdict table, including a reading with no worker alive and therefore no cycle
  timestamp at all;
- the boundary of the stall threshold, on both sides of it;
- a worker deficit and a paused relay both report `UP`, with the deficit and the pause in the
  details;
- a process with no relay reports `UNKNOWN`, and one whose relay polls more slowly than the
  threshold allows is refused when the indicator is created;
- a `stalled-after` too close to `poll-interval` is refused, naming both keys, and one with enough
  margin is accepted;
- a process configured with neither role is refused, naming both keys (§4);
- the shipped `application.yml`, bound the way Spring Boot binds it, hands the socket timeout to
  the pool as a driver property under the name the PostgreSQL driver reads. A key one level off in
  that file is ignored without a word, and the default it was meant to set would simply not exist.

### 8.2 Integration test: the assembled application, started as a process

`TandemRelayApplicationIT`, tagged `integration` and wired into `check` like every other
Docker-bound test in the project, where it runs twice: on the jar built from the working tree and
on the pinned one (§7.3). It does **not** start a Spring context inside the test JVM: it launches
the jar with a real `java` process (the Java 25 launcher of the
module's toolchain, handed to the test task by Gradle), configured **through environment variables
only**, against `TandemTestContainer`'s real PostgreSQL and Kafka on their host-mapped ports.

**The test has a source set of its own, `integrationTest`**, with nothing of the application on its
classpath: it observes the process from outside, over HTTP, JDBC and Kafka. That is not only
tidiness. The main and test classpaths carry the Spring Boot 4 BOM, which manages a Testcontainers
generation (2.x) and a Jackson generation (3) that `tandem-test`'s container helper is not built
against; a test sharing that classpath would start its containers on a mix of the two.

Every relay in the test runs under `LEASE`: several of these processes are alive at once against
one database, which is exactly the case that mode is for.

That is what makes the test worth its runtime. A context started in the test JVM would exercise
neither the executable jar and its launcher, nor configuration from the environment, which a JVM
cannot set for itself. And the addresses of the database and the broker are known only once the
containers are up, so Gradle cannot supply them either.

With both roles enabled:

1. a row inserted into the outbox is delivered to Kafka, with `TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS`
   as the only source of the broker address (§6.2). The delivered body is compared with the written
   payload as a JSON document, not byte for byte: PostgreSQL stores it as `jsonb` and renders it in
   its own spacing;
2. the readiness path on the management port answers `UP` and carries the `tandemRelay`
   contributor with its details (§5);
3. the Prometheus scrape contains a Tandem meter, the proof that the metrics adapter found the
   registry Spring Boot contributes in an assembled application;
4. `GET /tandem/admin/v1/outbox/summary` answers 200, and `GET /outbox/messages/{id}` renders
   `payload` as **real JSON**, the stored document and not a rendering of the binding's own tree
   type (§2);
5. the application port serves no Actuator path and the management port no Admin API path (§4.1);
6. the `info` endpoint names the library release the application contains (§7.3);
7. asked to stop, the process exits by itself within the grace period (§6.1).

With one role, or a broken configuration:

8. Admin-API-only: the process starts with no Kafka setting at all, serves the Admin API, and
   reports the relay contributor as `UNKNOWN`;
9. relay-only: no Admin API route is served, and a correctly configured start logs nothing at `WARN`;
   a warning at every healthy start teaches operators to ignore warnings;
10. without `tandem.kafka.source` the process exits, and its output names the key;
11. with neither role enabled the process exits, and its output names both keys.

Assertion 4 overlaps `tandem-admin`'s `jacksonThreeTest` on purpose, and assertions 1 and 3
overlap the wiring tests of `tandem-spring-relay`. Those gates run a module's tests on a chosen
classpath; this one runs the application as shipped, with what Boot itself configured, which is
the only place a mismatch between the two would show.

**Not covered here:** readiness turning `DOWN` when the database or the broker goes away, and the
socket timeout cutting a dead connection. Both need fault injection that `TandemTestContainer`
does not offer. The logic behind the first is the verdict, covered row by row in §8.1. The second
is the driver's own behaviour; what this module can get wrong is the key, and §8.1 pins that.

### 8.3 Image smoke test, in CI

The integration test runs the jar, not the image, so nothing above exercises the Dockerfile. CI
builds the image from the pinned jar, exactly as a release would, and brings up `docker-compose.example.yml` with it, waiting on the compose
healthcheck, which is the readiness path. That one step proves the layer extraction, the
entrypoint, the non-root user, and the example file itself, which is otherwise a document that
nothing runs.

It is a step of the existing `build` job, after `check`, so it reuses the pinned jar `check` has
already built (for `pinnedTest`) and adds an image build and a compose start, not a second Gradle
build. The image is built for the runner's architecture only and never pushed. Beyond `--wait`, the
step reads readiness on both replicas from the host (the example publishes each replica's management
port on loopback) and fails if the relays' logs contain a `WARN` line, the same rule §8.2 applies to
the jar. On failure it prints the compose logs; it always tears the example down.

---

## 9. Database engines — PostgreSQL now, a second engine without restructuring

`tandem-jdbc` declares no JDBC driver at all (only a test-scoped one), by design: the driver is
the application's to choose. In this module Tandem is the application, so the image must carry
one.

**The image bundles one driver per supported engine, and lets the URL select it.** Spring Boot's
`DataSourceAutoConfiguration` already derives the driver class from `spring.datasource.url`, so
nothing needs to be configured and no dialect key is introduced here. Today that means PostgreSQL
alone — which already covers every Postgres-wire-compatible managed service, since those need no
driver of their own. When a second engine lands, adding it is one dependency line and one line of
documentation, with no restructuring and no second image. This design is deliberately indifferent
to *which* engine that turns out to be.

**Per-engine images were considered and rejected**: they double the build matrix, the release
artifacts and the documentation to save an amount of image size that does not matter, and they
force an operator to pick an image variant for a fact the connection URL already states.

Two other things stay untouched when a second engine arrives. **The image still applies no DDL**
(§6.4) — which is also what keeps another engine from doubling anything here. And **this module
still binds no `tandem.*` key of its own** (§6.1): if the JDBC layer ends up needing an explicit
engine or dialect property rather than detecting it, that key is the library's, appears in
`tandem-relay-reference.yml`, and flows through this image with no code change.

---

## 10. Module registration checklist

Per AGENTS.md, each omission below fails silently:

| List | Action |
|---|---|
| `settings.gradle.kts` | Add `tandem-relay` |
| `unpublishedModules` (root `build.gradle.kts`) | Add: **not** published to Central, and it needs its own Java 25 toolchain and its own test tasks |
| `tandem-bom` | **No** — not a Maven artifact |
| The root build's substitution exemption | The pinned classpath joins the connector's floor classpath as a configuration the working tree must **not** be substituted into (§7.3). Forgetting it leaves `pinnedTest` green while testing the working tree |
| `.github/workflows/relay-release.yml` | New, on `relay-v*` (§7.3). `release.yml` needs no exclusion: an unpublished module has no publishing task |
| `tandem-coverage`'s `coveredProjects` | **No** — published, tested modules only |
| `dependency-graph-exclude-projects` (`.github/workflows/ci.yml`) | **No, and this is the one unpublished module left out of that regex on purpose.** The exclusion keeps demo and benchmark dependencies out of the Dependabot alerts because nobody inherits them; this module's runtime classpath is shipped to operators inside the image, so an alert on it is a real one. Record the reason as a comment beside the regex |
| README API reference table | **No**: its rows link a javadoc.io page, which exists only for an artifact on Maven Central. The image is documented in the README's usage section and in the user guide instead |
| CONTRIBUTING project layout · LLD-base.md | Add; **and correct LLD-base.md**, which lists `tandem-relay` with a published `artifactId` |
| User guide (`guide/`, `mkdocs.yml`) | Add the page an operator reads: roles, the two ports, the probe paths, configuration from the environment, the grace period, the instance id, the `SINGLE`/`LEASE` rule, and the table of image versions with the library release each contains |
| README "Future work" | No `tandem-relay` bullet: the image is released, and the README's usage section points at it and at the user guide |
| AGENTS.md | A fourth release scheme joins the library's, the CLI's and the connector's: the `relay-v*` tag, the pin, the contract the breaking-change check is scoped to, and the image release as a step that follows a library release. Four existing statements are also written for a repository with no deployable and must be brought in line in the same change: the `dependency-graph-exclude-projects` row ("only for modules that must not be published"), the `THIRD-PARTY-NOTICES.md` row and rule ("published modules only"), the Logging paragraph on leaf apps (this one takes Logback from the starter, not `slf4j-simple`), and the paragraph on independently versioned modules, which today reads as if all of them were published libraries |
| `ci.yml` and LLD-base.md "Dependency graph and vulnerability alerts" | Both describe the submitted graph as the published runtime footprint; this module widens that to "what is redistributed", image included |
| THIRD-PARTY-NOTICES.md | **Yes**, despite not being on Central: the image and the jar redistribute the whole Spring Boot runtime, so the licence footprint is real. Follow the `tandem-cli` precedent and derive it from the **actual jar contents**, not from the dependency graph: the list is generated from the jar's `BOOT-INF/lib` and a check fails on drift, since a hand-kept list of a hundred-odd jars would be wrong within a release. The same file is what the image carries (§7.2). How, below |
| open-questions-lld.md | Mark Q23 resolved, including §1.1's already-answered fourth part |

### 10.1 The generated notices

The `tandem-relay` section of the root `THIRD-PARTY-NOTICES.md` holds a list between two marker lines
that only the build writes. Two tasks of the module share one generator:

- `updateThirdPartyNotices` rewrites the list from the pinned jar;
- `checkThirdPartyNotices`, part of `check`, regenerates it and fails when it differs from the committed
  one, naming the task to run.

The generator reads the jar, not a configuration: every entry of `BOOT-INF/lib` is a row, matched to its
Maven coordinates through the resolved `pinnedRuntimeClasspath`. The one jar no configuration resolves,
`spring-boot-jarmode-tools`, which the Boot plugin adds by itself, is matched by name; any other jar it
cannot attribute fails the task. Tandem's own modules are left out of the rows (they are the release the
list names above the table). Each library's licence comes from its POM, resolved like any artifact, or
from the nearest parent POM declaring one, which is how Maven inherits it; several licences in one POM
are alternatives by Maven's definition of the element, and are joined with `OR`.

Licence names are spelled differently in every POM, so the build maps each spelling to an SPDX
identifier, and **a spelling it does not know fails the generator**. That is deliberate: a licence new to
the image enters it only once a person has read it, and the same change adds its text section to the
file. It costs one line per new spelling and no plugin; a licence-report plugin would add a dependency to
the build for a list this module can derive in a few dozen lines.

The whole file, not only this section, is what `pinnedBootJar` puts in `META-INF` and the image copies
to `/licenses`: the image also redistributes the library's own modules, whose footprint the rest of the
file describes.

---

## 11. Decisions

### 11.1 Resolved

| Decision | Rationale |
|---|---|
| One image, roles by property | The gates already exist and default correctly; delivers the standalone Admin API of HLD-admin-api §4.1 at no extra cost (§4) |
| Spring Boot 4.1 + Java 25 | An application constrains no consumer; longest runway, and real validation of the dual-generation bet (§3.1) |
| Health indicator here, not in the library | The library ships no verdict because the embedding app owns the threshold — here Tandem is that app (§5) |
| The indicator joins readiness | A relay that is not delivering must stop a rollout and raise an alert; in the relay-only role that is all readiness does (§5.2) |
| The verdict is a pure function, the indicator has no bean condition | Testable row by row without a database; and a condition on a scanned class would never match, while a contributor missing in one role would fail group validation (§5.1) |
| `stalled-after` checked against `poll-interval` at startup | A fixed threshold reports an idle relay with a long poll interval as down (§5.2) |
| A process with neither role is refused | It would start, report ready and do nothing (§4) |
| Not in the liveness group; a socket timeout on the datasource instead | A restart cures only a blocked database call, and a liveness threshold safe against broker stalls would be over an hour and a half. The timeout removes the cause (§5.2, §6.1) |
| Dockerfile over buildpacks | Explicit, patchable base image; no second toolchain (§7.2) |
| Versioned on its own, `relay-v<semver>` | The image is the library plus a runtime, a base image and a deployment contract, which change on their own cadence; a shared version would turn every runtime fix into a release of every library module (§7.3) |
| One exact library release per image, resolved from Maven Central | An image built from the working tree could contain unreleased library code, and the question "which Tandem runs here" must have an answer (§7.3) |
| `latest` only for versions without a pre-release suffix; version tags never moved | A release candidate must not become what `latest` pulls, and a pinned tag must keep meaning the same bits (§7.3) |
| A fully automatic release workflow | Nothing in it is irreversible, unlike a Maven Central publication (§7.3) |
| One driver per supported engine, one image | The URL already selects the engine; per-engine images cost more than they save (§9) |
| No DDL application | Requires privileges a relay should not hold, and the relay is not the schema's only writer (§6.4) |
| No `tandem.*` key of its own | A knob this module needs is a knob the library is missing (§6.1) |
| No code for configuration from the environment | Spring's binder already maps a variable to a dotted map key; the behaviour is pinned by a test in `tandem-spring-relay` (§6.2) |
| Grace period sized by the runtime, not by a Spring property | The stop sequence is synchronous and is only ever cut short by `SIGKILL` (§6.1) |
| An explicit instance id for more than one replica | The derived one keeps the wrong end of a pod name (§6.3) |
| The application's own Spring Boot version in the catalog | A security update of the image must not move the library's verified Boot 4 line (§7.1) |
| The Kafka client Spring Boot manages (4.x) | It is what a Boot 4 adopter runs; this module's integration test is its end-to-end gate (§3.2) |
| The integration test starts the jar as a process | Only that exercises the executable jar and configuration from the environment (§8.2) |
| CI runs the image, not just builds it | Nothing else exercises the Dockerfile or the compose example (§8.3) |
| `coordination` stays `SINGLE`, `wakeup` stays `none` | Divergent defaults between embedded and standalone would be a worse trap than the one it avoids (§6.1) |
| Kafka-only image | `tandem-rabbitmq` has no autoconfiguration to switch on by property and carries its own version line (§3.2) |
| Stock Boot 4 classpath, no Jackson dependency here | `tandem-admin` is indifferent to the Jackson generation; the image adds no opt-back to Jackson 2 (§2) |
| Pause is `UP`, and shown in the details | A deliberate state must not trigger the orchestrator; the flag is a cached in-memory read (§5.1) |
| Kept in the dependency graph CI submits | The image redistributes its runtime classpath, so its alerts are real (§10) |
| The image's version and pin as build arguments printed by Gradle, checked against the jar's `build-info` | A label can only take a build argument; one source for both values, and a mismatch fails the build (§7.2) |
| The licence and the notices taken from the jar, not from the build context | The jar and the image carry the same files by construction, and the context holds the jar alone (§7.2) |
| No `curl`/`wget` in the image; the compose healthcheck uses `bash`'s `/dev/tcp` | A probe-only package is attack surface for every deployment; orchestrators probe from outside (§7.2) |
| The compose example leaves the Admin API off | It warns at every start by design (§4.2), and a healthy example start must log no `WARN` (§8.3) |
| The image smoke test is a step of the existing CI job | It reuses the pinned jar `check` built, instead of a second Gradle build (§8.3) |
| Notices generated from `BOOT-INF/lib`, an unknown licence spelling fails the generator | A list of eighty jars kept by hand would drift; a new licence needs a person to read it (§10.1) |

### 11.2 Open

- **Sends that block the worker.** `producer.send` waits for metadata on the worker thread, once
  per row of the batch, so one missing topic stalls a worker's whole slice and not only the rows
  routed to it. This is engine behaviour, tracked in the backlog on its own, and nothing in this
  module depends on its outcome: until it changes, readiness reports such a relay as not
  delivering, which is accurate.
- **Supply-chain extras for the image**: the base pinned by digest and who moves it, an SBOM and
  provenance attestation, a vulnerability scan in CI.
- **The derived instance id in containers** (§6.3) is a library matter: keeping the end of the
  hostname rather than its start, and more random bits, would make the default safe.
- **Kubernetes manifests / a Helm chart.** The compose example covers "try it"; a chart is a
  distribution surface with its own release cadence and its own compatibility promises. Deferred
  until there is demand — the image plus the documented probes is what a chart would wrap.
- **Exposing the Admin API and the relay from separate images.** Only worth revisiting if the
  combined image's dependency surface becomes an obstacle in practice.
- **`tandem-tracing-otel` as an opt-in variant image**, for adopters wanting OTel span emission
  without Micrometer Tracing (§3.2).
