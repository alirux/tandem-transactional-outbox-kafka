# Tandem: an Agent Skill for Adopting Tandem

**Version:** 0.1  
**Status:** Proposed. Designed, not implemented.  
**Companion to:** [HLD.md](HLD.md) §1.1, §1.3; [../guide/getting-started.md](../guide/getting-started.md);
[../guide/adoption.md](../guide/adoption.md); [../guide/configuration.md](../guide/configuration.md);
[HLD-api-stability.md](HLD-api-stability.md)

This note decides whether and how Tandem ships an **agent skill**: a packaged set of instructions a
coding agent (Claude Code and any other agent that reads the Agent Skills format) loads to integrate
Tandem into an application, whether the application is new (greenfield) or already in production
(brownfield).

---

## 1. Summary

- The knowledge an adopter needs already exists, in the user guide: [Getting Started](../guide/getting-started.md)
  is the greenfield path, the [Adoption Guide](../guide/adoption.md) the brownfield one. A guide is
  read by a person, who then has to map it onto their own code. A skill does that mapping: it
  **inspects the application**, takes the decisions the guides describe from what it finds, applies
  the integration, and verifies it.
- Decided: **one skill** (`tandem-adoption`) with a shared discovery phase and two branches (§3),
  not two skills.
- Decided: the skill is a **thin procedure over generated references**. `SKILL.md` is written by hand
  and holds the procedure and the decision tree; everything it cites is generated from `guide/` and
  from the Spring configuration metadata, and committed, with a build check that fails when the
  generated copy is stale (§5). The guide stays the single source.
- Decided: the skill **never acts on a running system**. It writes code, configuration and migration
  files; it never applies a migration, starts a relay against a shared database, or removes an
  existing producer in the same change (§4).
- Decided: it ships from this repository as a **Claude Code plugin** in a marketplace file, with the
  library's version, and is first released against **1.0.0** (§7).
- Nothing of it reaches the library: no module, no dependency, no contract. It is outside the core
  path by construction (HLD §1.1, §1.3).

---

## 2. Goals and non-goals

**Goals.**

- An adopter gets from "no Tandem" to "an event written in my transaction and published, with a
  test proving it" in one guided session, on the common stack (HLD §1.1): Spring Boot 3 or 4, or
  plain Java, on PostgreSQL, publishing to Kafka.
- The decisions a brownfield adopter gets wrong most often are **surfaced, not guessed**: the
  ordering precondition, the `@Version` flush timing, the `seq` mode, applying the schema to a
  database that already holds data, and the cutover from a hand-rolled producer.
- The skill stays correct across releases without anyone keeping a second copy of the guide in step.

**Non-goals.**

- Operating Tandem in production: sizing, alert triage, replaying `FAILED` rows. That is the
  Admin API, the CLI and the [troubleshooting guide](../guide/troubleshooting.md); a separate skill
  may cover it later (§9).
- Writing the consumers. The skill states the idempotency requirement and points to
  [Consuming events](../guide/consuming-events.md); the consumer usually lives in another service.
- Non-Java writers, until the SDKs of [HLD-write-side-sdks.md](HLD-write-side-sdks.md) exist; MySQL,
  until the engine port exists.
- Migrating from another outbox tool. That path is not in the guide yet; the skill follows the guide.

---

## 3. The procedure

The skill runs five phases in order. Each phase ends with a short report to the user, and the
decisions of phase 3 are confirmed by the user before phase 4 writes anything.

### 3.1 Discovery

The skill reads the application and records what it finds, without changing anything:

| Fact | Why the skill needs it |
|---|---|
| Build tool (Gradle Kotlin/Groovy, Maven) | Where and how the BOM and the modules are declared |
| Spring Boot line (none, 3.x, 4.x) and Jackson generation | Which write-side tier is available, and which Spring modules apply (LLD-spring-config §1.2, §1.3) |
| Database engine and version | PostgreSQL 14+ is supported; anything else stops the skill with the compatibility guide |
| Migration tool (Liquibase, Flyway, none) | How the `tandem_*` schema enters the application's own migration history |
| Existing Kafka or RabbitMQ producer, and its call sites | Whether the application is brownfield, and what the cutover replaces |
| Transaction boundaries (`@Transactional`, `TransactionTemplate`, manual JDBC) | Where the outbox write goes |
| Aggregate roots and their locking (`@Version`, pessimistic locks, none) | The ordering precondition (§3.3) |
| Existing domain event mechanism (`ApplicationEventPublisher`, aggregate-held events) | Which write-side tier fits with the least change |
| Where the relay will run (in the application, or the `tandem-relay` image) | Which relay module or which deployment file the skill produces |

### 3.2 Classification

**Greenfield** when there is no existing producer to replace and the outbox tables are going into an
application whose events are new. **Brownfield** otherwise. The classification is reported with its
evidence, and the user may override it. Brownfield is greenfield plus constraints, so the two
branches share every step and differ only in what phase 3 must check and phase 4 must preserve.

### 3.3 Decisions

The skill takes the decisions of the [Adoption Guide](../guide/adoption.md) §1 and §3, in that order:

1. **Ordering precondition** (brownfield, and greenfield when concurrent writers to one aggregate
   exist). The skill classifies each aggregate into one of the guide's four shapes from the code it
   read. Shape 1 proceeds. Shape 2 (`@Version`) proceeds only with the flush handled or with
   `managedSeq()`, and the skill says which it chose. Shapes 3 and 4 **stop the skill for that
   aggregate**: it reports the shape and the guide's options, and does not pick one, because the fix
   is a domain decision.
2. **Write-side tier**, from the guide's decision tree: application events when the application
   already publishes them, otherwise the least invasive tier that fits.
3. **`seq` mode and the write lock**, per aggregate type, with the reason stated.
4. **Relay placement**: in-process (`tandem-spring-relay`) or the image; the skill proposes
   in-process for a single-service greenfield application and the image when several services share
   the outbox (HLD §3.2).

### 3.4 Application

The skill writes, in the application's own idioms:

- the BOM and the modules the decisions require, and nothing else (HLD §1.3);
- the schema as a migration in the application's own tool, never applied by the skill;
- the outbox write at each call site the decisions cover;
- the minimal configuration: the keys [Getting Started](../guide/getting-started.md) §6.1 names, at
  their defaults unless discovery gave a reason to change one;
- in brownfield, the cutover of the [Adoption Guide](../guide/adoption.md) §5, one aggregate at a
  time and in its rollout order (§8): the new write path is added, the old producer stays until the
  user removes it.

### 3.5 Verification

The skill adds a test that writes an event inside the business transaction and asserts it on the
outbox (`InMemoryOutbox` for a unit test, `TandemTestContainer` when Docker is available), runs the
application's own test suite, and ends with the guide's "before you go to production" checklist,
each item marked as done, not applicable, or left to the user.

---

## 4. What the skill never does

These rules are part of `SKILL.md`, stated as hard stops, because each one is an action whose
damage a test cannot show:

- **Never applies a migration or connects to a database other than a test container.** A brownfield
  database holds data; the [Adoption Guide](../guide/adoption.md) §4 steps are for the user to run.
- **Never removes an existing producer.** The cutover leaves the old path in place; removing it is a
  separate change the user makes once the new path is observed in production.
- **Never decides an ordering precondition it cannot see satisfied** (§3.3, shapes 3 and 4).
- **Never changes the Kafka producer's protected settings** or the partitioner
  ([Advanced Integration](../guide/advanced-integration.md) §1.1): "one aggregate, one partition"
  is what consumer ordering rests on.
- **Never writes a payload, credential or connection string into a log statement or a test
  fixture** it creates (AGENTS.md, Logging §5 applies to the code the skill generates).

---

## 5. Content: one source, generated references

A skill that restates the guide by hand becomes a second documentation set that drifts from the
first. So:

- **`SKILL.md` is hand-written and short**: the phases of §3, the hard stops of §4, and pointers to
  the references, loaded only when a phase needs them (progressive disclosure).
- **`references/` is generated** by a build task from the guide pages the procedure cites (Getting
  Started, Adoption, Configuration, Advanced Integration, Testing and Upgrades, Troubleshooting) and
  from the `additional-spring-configuration-metadata.json` of the Spring modules (the configuration
  key table, with defaults). Links inside them are rewritten to absolute URLs, since the skill runs
  in another repository.
- **The generated files are committed**, because a plugin is installed from the repository's files,
  and a check task wired into `check` fails when they differ from what the task would produce now.
  The pattern is the one of `checkThirdPartyNotices` for the relay (LLD-relay §10): regenerate,
  review the diff, commit.

A change to the guide therefore reaches the skill in the same change, and a reviewer sees both.

---

## 6. Evaluation

Whether an agent follows the procedure is not deterministic, so it is not a CI gate. It is checked
before each skill release on two fixtures kept beside the skill:

- **greenfield**: a Spring Boot application with one aggregate and no messaging;
- **brownfield**: the same application with a `KafkaTemplate` producer called after commit, an
  aggregate with `@Version`, and one aggregate that only ever writes a child row (shape 3).

A run passes when the fixture builds, its tests pass, the brownfield producer is still there, the
`@Version` aggregate uses the flushed version or `managedSeq()`, and the shape 3 aggregate was
reported and left untouched. The deterministic part (references up to date, links resolving) is in
CI through §5.

---

## 7. Packaging, versioning, releases

- **Location**: `plugins/tandem/`, holding `.claude-plugin/plugin.json`, `skills/tandem-adoption/`
  (`SKILL.md`, `references/`) and the evaluation fixtures; a `.claude-plugin/marketplace.json` at the
  repository root lists it, so the repository itself is the marketplace. The `SKILL.md` format is the
  open Agent Skills one, so the same directory serves agents other than Claude Code.
- **Version**: the library's. The skill describes the library's API, so a skill version means "the
  procedure for Tandem x.y". It is stated in `plugin.json` and in the first lines of `SKILL.md`.
- **Release**: the skill goes out with the library tag that carries it; there is no fourth release
  scheme. What the marketplace serves must be the skill of the **latest library release**, not of
  `main`, which may describe unreleased features.
- **First release**: against **1.0.0**, the first frozen API (HLD-api-stability), so the procedure is
  not rewritten at each release candidate.
- **Registration**: a commit scope `skill` in `commitlint.config.mjs` and in the AGENTS.md list; a
  line in `.github/CONTRIBUTING.md` (project layout and design documents); a mention in the README
  next to the guide. The plugin is not a Gradle module, so the module checklist does not apply, apart
  from the generation task of §5, which lives in the root build.

---

## 8. Footprint and compatibility

The skill adds nothing to any artifact an application imports or an operator deploys: no module, no
dependency, no class, no configuration key. It is not one of the five contracts of
HLD-api-stability §2, and a change to it is never a breaking library change. It **depends on**
those contracts: the code it generates uses only the published API, so the API stability rules
already keep a released skill valid for every later minor.

---

## 9. Rejected alternatives

- **Two skills, one greenfield and one brownfield.** Rejected because the discovery phase is the same,
  the adopter often cannot tell which case they are in until it has run, and the brownfield path is
  the greenfield one with added checks.
- **A hand-written skill.** Rejected because it is a second guide that nothing keeps in step (§5).
- **A skill that reads the published guide over the network at run time.** Rejected because it needs
  network access the agent may not have, and serves the guide of `main`, not of the release the
  application uses.
- **A project generator (`tandem init`, a Maven archetype, a Spring Initializr entry).**
  Deterministic, and useful for greenfield, but it cannot read an existing application, which is
  where adoption goes wrong. It may complement the skill later; it does not replace it.
- **An MCP server.** Rejected: the skill needs no live data and no tool of its own, only knowledge
  and a procedure, and a server is a runtime component to install and keep running.
- **An operations skill in the same package.** Deferred, not rejected: operating Tandem (Admin API,
  CLI, triage) is a different audience and moment, and gets its own skill when this one has users.

---

## 10. Open points

- How the marketplace entry is pinned to the latest release tag rather than `main`, with the
  mechanism the Claude Code marketplace format offers for a git reference.
- Whether the Getting Started plain-Java path is part of the first release or follows it: the
  Spring path covers most adopters (HLD §1.1).
- Whether the brownfield fixture also exercises Flyway, or only the Liquibase changelog that Tandem
  itself ships.
