# Tandem — Implementation Plan: RabbitMQ connector

**Version:** 1.0
**Status:** Implemented. The connector floors on Tandem 0.10.0 and releases on `rabbitmq-v*`.
**Scope:** a second transport adapter on the `OutboxDispatcher` port, `tandem-rabbitmq`, carrying its
own version and its own release cadence; plus the guide page on extending the published envelope,
deferred here from the message-format port. Spans `tandem-core` (documentation only), the new module,
`tandem-spring-relay` (one wiring defect), the root build, the release workflows and `guide/`.
**Design:** [LLD-rabbitmq.md](LLD-rabbitmq.md), written in Phase 1 and not yet existing. Inherits
[HLD.md](HLD.md) §1.2 (ports and adapters), §1.4 (compatibility of every contract),
[LLD-core.md](LLD-core.md) §2.4 (the message-format port) and [LLD-cli.md](LLD-cli.md) §9.1 (the
precedent for an independently versioned component).

This document orders the work, fences the scope and defines done-ness per phase. The design is fixed
in the LLD; nothing here restates it.

**Standing assumption:** the library is in `0.x`, but this connector is not. Once `rabbitmq-v0.1.0`
is published its public surface and its wire behaviour are a contract in their own right, so the
freedom the library still takes with breaking changes does not extend to this module.

---

## 0. Decisions taken before the work starts

| # | Decision | Why |
|---|---|---|
| D1 | RabbitMQ over AMQP 0.9.1, `com.rabbitmq:amqp-client`. Module `tandem-rabbitmq`, package `com.codingful.tandem.rabbitmq` | It is the broker most of the non-Kafka population already runs, and the one that stresses the port hardest: no partitions, no delivery timeout, a two-valued route |
| D2 | Ship a CloudEvents binary binding, written by hand, reusing `CloudEventFactory` | The CloudEvents SDK publishes no AMQP 0.9.1 binding (its AMQP binding is Proton, that is AMQP 1.0), so the binding is ours to write. Shipping it is what validates the `tandem-cloudevents` split: a second transport reuses the envelope and writes only its own mapping |
| D3 | No RabbitMQ autoconfiguration in this plan (no `tandem.rabbitmq.*` properties) | A transport's autoconfiguration is a separate adoption surface with its own properties and its own dual-generation gate. A Rabbit application contributes its own `OutboxDispatcher` bean, which the relay autoconfiguration already honours |
| D4 | The RabbitMQ container lives in this module's own test sources, never in `tandem-test` | `tandem-test` is published and already exposes Kafka and Testcontainers as `api`. Adding RabbitMQ there would impose it on every consumer of the test helpers |
| D5 | Independent version, tagged `rabbitmq-v<semver>`, starting at `rabbitmq-v0.1.0` | The connector tracks the port (`OutboxDispatcher`, `MessageEncoder`), not the implementation behind it. A library release that leaves those interfaces alone cannot break it, and it can fix bugs with no library release at all. Same shape as `cli-v*`, whose glob separation already works |
| D6 | The connector is not in `tandem-bom` | The BOM promises one aligned version for every module. Pinning an independently versioned module there would force a BOM release on every connector release, recreating the coupling the independent version removes |
| D7 | Build the independent release path last (§8), not first | Its centrepiece is a floor verified against a published coordinate, and the coordinate the connector can actually floor on is not published yet (see D8). Built earlier the gate would have been born disabled. Three guard rails (§1) cover the interval |
| D8 | Develop now, release after the next library release | `v0.9.0` put `tandem-core` and `tandem-cloudevents` on Central, but the connector reads `CloudEventsHeaders.AMQP_BINARY_PREFIX`, added to the core *after* that tag. The floor is therefore the next library release, which is also the one carrying the §6 breaking change. Nothing else blocks the work |
| D9 | `tandem-spring-relay` stops redistributing `tandem-kafka` (§6) | With the transport a port, an autoconfiguration module must wire whichever adapter is present, not carry one. Today every Spring relay application inherits the Kafka client whether it publishes to Kafka or not. Breaking for consumers, so it lands in the same library release the connector floors on |

---

## 1. Phase 0 — Guard rails and pins · **S** · **done**

No connector code. Three changes that are cheap now and expensive later, plus the version pins.

**The publication guard.** `release.yml` publishes every module that applies the publishing plugin,
so a `v*` tag cut while `tandem-rabbitmq` exists in the tree would publish it versioned as the
library. A version on Maven Central can never be deleted or overwritten, and that one would have
announced to consumers exactly the coupling D5 denies. Add a second set beside `unpublishedModules`
in the root build, used only by the two lines that apply the publishing plugin
(`build.gradle.kts:26` and `build.gradle.kts:133`) and not by the one that applies the java-library
convention. The module stays an ordinary citizen of the build (it compiles, tests, and enters
coverage aggregation) and is simply not publishable until Phase 6 gives it a release path. A flag in
the workflow would do the same job and can be forgotten at tag time; this cannot.

**The BOM omission**, from the commit that creates the module, per D6.

**The discipline rule**, recorded in the LLD in Phase 1 and binding from now: the connector uses only
`tandem-core` and `tandem-cloudevents` API that exists in, or will ship with, the release it floors
on. If it needs a core change, that change is a separate commit that lands in that release, never an
incidental edit pulled along by the adapter. Phase 6's `floorTest` turns this rule into a check;
until then it is a review obligation.

**Version catalog:** `com.rabbitmq:amqp-client` 5.25.0 and `org.testcontainers:rabbitmq` through the
Testcontainers BOM already pinned.

**Done-ness:** `./gradlew check` green; `./gradlew :tandem-rabbitmq:tasks --all` lists no
`publishToMavenCentral`; the module is absent from `tandem-bom`.

---

## 2. Phase 1 — The LLD, before any code · **S/M** · **done**

`docs/LLD-rabbitmq.md`, on the shape of [LLD-kafka.md](LLD-kafka.md). What it must settle, because
each item is a contract a consumer or an operator depends on:

- **The mandated safe configuration**, and what is rejected: publisher confirms on, `deliveryMode`
  persistent, the `mandatory` flag with a return listener. Same fail-fast posture as
  `KafkaProducerConfig.harden`.
- **Failure semantics.** Unroutable return is permanent (no queue is bound; retrying cannot help and
  would block the aggregate for the whole backoff ladder). Broker `basic.nack`, channel and
  connection exceptions are retriable. An encode failure is permanent, classified at the encode site
  and never routed through the broker classifier, exactly as in `KafkaRelay`.
- **The confirm timeout, and why it is not optional.** `RelayWorker` wires the dispatch future with
  `whenComplete` and imposes no deadline of its own: on Kafka the deadline is the producer's
  `delivery.timeout.ms`, which `deliveryTimeoutMillis()` reports so the `rowLease` invariant can be
  checked against the real value. AMQP has no equivalent, so a lost confirm would leave the future
  pending forever, holding an in-flight slot while the row's lease expires and another worker
  redelivers it. The adapter therefore enforces its own confirm timeout, fails it as retriable, and
  reports it through `deliveryTimeoutMillis()`.
- **The route.** `TopicRouter` returns one string; AMQP needs an exchange and a routing key. A
  routing port local to this module returns both. `TopicRouter`'s javadoc, which today says "Kafka
  topic", is generalised additively.
- **The CloudEvents binding**: attributes as `cloudEvents_` prefixed headers, the payload as the
  body, stored headers passed through, `CloudEventFactory` deciding every attribute a consumer reads.
- **The ordering contract, stated plainly.** The relay publishes one head per aggregate, so publish
  order per aggregate holds. RabbitMQ has no partitions, so consumer-observed order does not follow
  from it: competing consumers on one queue reorder. Preserving it requires a queue with
  single-active-consumer, or the consistent-hash exchange keyed on the aggregate id. This is the one
  place where the connector is weaker than `tandem-kafka` at the guarantee Tandem exists to provide,
  and it is documented as such rather than implied.
- **Versioning**, in the form of LLD-cli §9.1: what the connector's semver governs (its public types,
  its default route, the headers it puts on the wire, the floor it declares) and what it does not
  (the AMQP client version, the broker version, which belong to a compatibility matrix).

Also in this phase: `HLD.md` §3.1's module graph, and §4.4 and §4.5, which today describe the
publication guarantees in Kafka's terms alone.

**Done-ness:** the LLD reviewed and merged. No production code in this phase.

---

## 3. Phase 2 — The adapter · **M** · **done**

| File | Change |
|---|---|
| `tandem-rabbitmq/build.gradle.kts` *(new)* | `api(project(...))` on core and cloudevents for now; Phase 6 swaps these for published coordinates. `amqp-client` brings `slf4j-api` transitively, as `kafka-clients` does, so this module logs through SLF4J at no extra footprint cost (AGENTS Logging) |
| `RabbitRelay` *(new)* | `OutboxDispatcher, AutoCloseable`. Publisher confirms mapped to the future, the confirm timeout, the span recorder wired as in `KafkaRelay`, `deliveryTimeoutMillis()` reporting the confirm timeout |
| `RabbitMessageEncoder` *(new)* | The transport-specific encoder port, with `from(MessageEncoder)` lifting a neutral encoder onto AMQP. Mirrors `KafkaMessageEncoder` |
| `CloudEventAmqpEncoder` *(new)* | The default format: `CloudEventFactory` builds the event, this class writes the `cloudEvents_` binding and copies the passthrough headers |
| `RabbitRelayConfig`, the routing port, the error classifier *(new)* | Per the LLD |

**Tests.** Classical school, no mock framework: a hand-written in-memory `Channel` double, the
counterpart of the `MockProducer` the Kafka tests use. Behaviours to pin: confirm completes the
future; nack fails it retriably; an unroutable return fails it permanently; an encode failure fails
it permanently without consulting the classifier; the confirm timeout fires and is retriable; the
neutral encoder lifted through `from` produces the same headers as a directly written one. Any new
type reachable from a log statement gets the `toString` test the logging convention requires.

**Done-ness:** `./gradlew :tandem-rabbitmq:test` green; the JaCoCo report read line by line for
genuine gaps, per the pre-commit rule.

---

## 4. Phase 3 — Integration · **M** · **done**

`RabbitRelayIT`, tagged `integration`, on a real broker through `RabbitMQContainer`, held in this
module's test sources (D4). What it must prove, beyond the unit suite:

- End to end: rows written, relay started, messages consumed, CloudEvents attributes asserted through
  typed constants rather than repeated literals.
- Per-aggregate publish order, with a single consumer so the assertion measures the relay and not the
  broker's dispatch policy.
- An unroutable message marks the row FAILED and does not block the bucket's other aggregates.
- A confirm that never arrives is retried, not lost, and the row's lease is never the thing that
  rescues it.
- A broker restart mid-run is retriable and the run drains.

**Scope, decided while writing it.** The IT covers the adapter against a real broker, which is the
same scope `KafkaRelayIT` has; the database-to-broker end-to-end stays in `tandem-test`'s `EndToEndIT`
and remains Kafka's, because extending it would mean putting RabbitMQ into `tandem-test` and D4 rules
that out. Two cases named above are unit tests instead, where they are deterministic rather than
timing-dependent: the lost confirm (a real broker confirms, so the deadline cannot be provoked without
a network proxy) and the dropped connection.

**Done-ness:** `./gradlew :tandem-rabbitmq:check` green with Docker available.

---

## 5. Phase 4 — Registration · **S** · **done**

The checklist from AGENTS.md, with the two variations D5 and D6 introduce:

| List | Action |
|---|---|
| `settings.gradle.kts` | Add |
| `tandem-bom` | **Do not add** (D6) |
| `tandem-coverage` `coveredProjects` | Add. The module is built and tested in this repository like any other |
| `README.md` API reference | **Deferred to the first release**: each row links a javadoc.io page, which exists only for a published artifact. The module is instead named in the README's feature list and its documentation section |
| `CONTRIBUTING.md` layout, `docs/LLD-base.md` | Add |
| `THIRD-PARTY-NOTICES.md` | Add the per-module table: `amqp-client`, `slf4j-api` |
| AGENTS.md registration checklist | Add the rule for independently versioned modules: outside the BOM, inside coverage, version stated explicitly wherever the module is documented |

**Done-ness:** every list names the module, or deliberately does not, and the deliberate omission is
written down where a future contributor will read it.

---

## 6. Phase 4b — `tandem-spring-relay` stops redistributing Kafka · **M** · **done**

`tandem-spring-relay` declares `api(project(":tandem-kafka"))`, so every Spring application using the
relay autoconfiguration inherits the Kafka client whether it publishes to Kafka or not. Now that the
transport is a port, this is the relay-side counterpart of the minimal-footprint rule (HLD §1.3): an
autoconfiguration module wires whichever adapter is present, it does not carry one.

**The pattern is already in this module.** `TandemMicrometerAutoConfiguration` wires an optional
Tandem module (`tandem-micrometer`, declared `compileOnly`) from a separate top-level
`@AutoConfiguration` class with a class-level `@ConditionalOnClass`, ordered `before` the relay class
so that the `@ConditionalOnMissingBean` back-off is one Spring actually guarantees: it guarantees it
across explicitly ordered autoconfiguration classes, never across two `@Bean` methods declared in one
class. Kafka takes the same shape.

- `tandem-kafka` moves from `api` to `compileOnly`, and onto the test and three-line matrix runtime
  classpaths, exactly as `tandem-micrometer` already is.
- A new `TandemKafkaAutoConfiguration`, `before = TandemRelayAutoConfiguration.class`, class-level
  `@ConditionalOnClass` on the Kafka producer and `KafkaRelay`, carrying `tandemMessageEncoder`,
  `tandemOutboxDispatcher` and `tandemTopicRouter` (which reads `tandem.kafka.topic-suffix`), plus
  `@EnableConfigurationProperties(TandemKafkaProperties.class)`, all of which leave the relay class.
- **A class-level gate, not per-method conditions.** The Kafka types appear in those methods'
  signatures, and an optional type in a `@Bean` signature throws `NoClassDefFoundError` while Spring
  introspects the configuration class, before any method-level condition is evaluated. A gated class
  is never loaded at all, which is why the Micrometer wiring can safely take a bare `MeterRegistry`
  parameter (LLD-spring-config §1.1 rule 2).
- `WorkerPool` then asks for an `OutboxDispatcher` that nothing contributes when neither Kafka nor an
  application bean is present. That case must fail with a message naming the fix (declare
  `tandem-kafka`, or contribute a dispatcher), not with Spring's generic unsatisfied-dependency report.

**This is breaking for consumers.** An application on `tandem-spring-relay` alone stops receiving the
Kafka adapter and must declare `tandem-kafka` itself. It belongs in the same library release that
publishes `tandem-cloudevents`, which is the release the connector floors on (D8), and it is the one
entry those release notes cannot omit. The README's Spring snippets and `tandem-sample-spring` change
with it.

**Verification.** A `FilteredClassLoader` context-runner test proves the back-off but not the absence
case: the configuration class stays loaded by the parent loader, so a signature that would fail on a
genuinely Kafka-free classpath stays green. The real check is a classpath that genuinely lacks the
module, in the shape this project already uses for such claims: a `noKafkaTest` task with a runtime
classpath excluding `tandem-kafka`, wired into `check`, sibling of `bootFourTest` and of
`tandem-admin`'s `jacksonThreeTest`.

**Done-ness:** `./gradlew :tandem-spring-relay:check` green including `noKafkaTest`; the sample Spring
application still boots with `tandem-kafka` declared explicitly.

---

## 7. Phase 5 — The guide · **S/M** · **done**

`guide/message-format.md`, plus its entry in `mkdocs.yml`. This is the half of backlog item 42 that
was deferred from the message-format port on purpose: it is written better with a second real
transport in hand, because it can show one neutral encoder running on two adapters instead of
describing a hypothetical one.

Content: what `MessageEncoder` and `EncodedMessage` are; when a neutral encoder suffices and when a
format genuinely needs the transport in its signature; how to keep the CloudEvents envelope and
change only parts of it; the Spring wiring, which is one bean; what the ordering key means on each
transport; and the compatibility rules that apply to a published envelope (additive only, tolerant
readers, never remove or retype).

Positioning: the README and the site describe Kafka throughout. This phase adds the minimum honest
line saying the transport is a port. The wider repositioning is backlog item 47 and stays there.

**Done-ness:** the guide builds under `mkdocs` and the new page is reachable from the nav.

---

## 8. Phase 6 — Independent version and release path · **M** · **done**

Runs last, immediately before the first connector release, once a library release has published the
core the connector floors on (D8: `v0.9.0` is not it, since the AMQP binding prefix landed after that
tag).

- **Published coordinates with an explicit floor** replace the project dependencies. A project
  dependency would stamp the connector's own version into the POM as the core's version, which is the
  single mechanical reason an independently versioned module cannot depend on its siblings by project.
- **`dependencySubstitution`** puts the local projects back during development, so the repository
  stays one buildable unit while the POM keeps declaring the floor.
- **`floorTest`**, wired into `check`: re-runs the connector's tests with the substitution off and the
  coordinates resolved from Central. Without it, "works with core x.y.z" is an unverified claim,
  since ordinary CI would be testing the working tree. Same role the dual-generation gate plays for
  the Spring modules.
- **Version from the module's own environment variable**, with a guard that fails the publish when it
  is unset, so a manual run cannot push a snapshot to Central.
- **`rabbitmq-release.yml`** on `rabbitmq-v*`, publishing this module alone; **`release.yml`** excludes
  it, replacing the Phase 0 guard. The globs are anchored, so `v*` never matches `rabbitmq-v*` and the
  two paths need no guard against each other.
- **AGENTS.md Releases** gains the third scheme, beside the library's `v*` and the CLI's `cli-v*`.
- **The LLD's versioning section** is completed with the floor actually declared.

**Done-ness:** `floorTest` green against the published floor; a dry run of the release workflow
produces a staged deployment containing this module and nothing else.

---

## 9. Out of scope

- RabbitMQ autoconfiguration and `tandem.rabbitmq.*` properties (D3). A separate item once the
  adapter has users.
- AMQP 1.0, JMS, and any second transport beyond this one.
- A performance comparison with Kafka. The published numbers are Kafka's and stay that way. The
  benchmark harness *can* run every scenario against RabbitMQ (`--broker=rabbit`, LLD-benchmark §3.1),
  because each scenario's verdict is correctness-only, and that is what it is for: gating this adapter
  under the real relay loop. Its throughput and latency figures describe the harness's single-queue
  topology, not the broker.
- A dedicated `tandem-spring-rabbitmq` autoconfiguration. It would itself need an independent version, since it would depend on the connector.
- Renaming the repository, whose name still says Kafka.

---

## 10. Verification gate

Before the commit that closes each phase: `./gradlew check` green including `integrationTest`, the
JaCoCo report read for genuine gaps rather than for its percentage, and the documentation updated in
the same change. Before the first release: `floorTest` green, and the release notes written in the
annotated tag rather than on the release page.

---

## 11. Release notes for `rabbitmq-v0.1.0`

Title: `tandem-rabbitmq v0.1.0 — RabbitMQ connector`. The body needs **New** (the adapter, the
CloudEvents AMQP binding, the guide page) and **Known limitations** (consumer-observed ordering
requires single-active-consumer or the consistent-hash exchange; no Spring autoconfiguration yet). It
must state the `tandem-core` floor, since nothing in the version number implies it.

The **library** release that precedes it carries its own **Breaking** entry for §6: an application on
`tandem-spring-relay` must now declare `tandem-kafka` explicitly to keep publishing to Kafka.
