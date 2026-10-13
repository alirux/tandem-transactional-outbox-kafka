# Tandem: Compatibility Gates (LLD)

**Version:** 0.1  
**Status:** Draft. The gates are being built: the baseline (§2), the declared breaks (§3), the Java
API gate with its interface kinds (§4), the two schema gates (§5), the Admin API gate (§6) and the
Spring metadata gate (§7) exist.  
**Implements:** [HLD-api-stability.md](HLD-api-stability.md) §5  
**Companion to:** [LLD-jdbc.md](LLD-jdbc.md) §6 (the changelog); [LLD-spring-config.md](LLD-spring-config.md)
§2.4 (the configuration metadata); [HLD-admin-api.md](HLD-admin-api.md) §5 (contract testing);
[LLD-relay.md](LLD-relay.md) §7.3 and [LLD-rabbitmq.md](LLD-rabbitmq.md) §9 (resolving a published
Tandem release while the working tree substitutes its coordinates)

This note specifies the build gates that check each contract HLD-api-stability §2 freezes: what each
gate compares, where its baseline comes from, what makes it fail, how an intentional break is declared,
and how the gates run locally and in CI.

---

## 1. Summary

- **One baseline**: the Gradle property `tandemBaseline`, a library release. The gates that read it
  resolve that release from Maven Central, or from its tag for the OpenAPI document. While it is
  empty, those gates skip (§2). A module the baseline release does not contain is new, and its
  baseline gates skip too (§2.4).
- **One file declares intentional breaks**, bound to the baseline it was written against
  (`gradle/accepted-breaks.txt`, §3). A declaration accepts an incompatibility a gate found, never an
  error that kept the gate from comparing.
- **Reference files** need no baseline: each is compared with what the code renders today (§8).
- The gates' build logic lives in **`buildSrc`** (§9.1). Every gate is part of `check`, so it runs
  in the CI build job like any other verification.

| Contract | Gate | Task | Module | Baseline | Docker |
|---|---|---|---|---|---|
| Java API | japicmp (§4) | `checkJavaApi` | every published library module | yes | no |
| Java API | interface kinds (§4.3) | `checkInterfaceKinds` | every published library module | no | no |
| Published modules | baseline BOM's modules still managed (§2.4) | `checkBomModules` | `tandem-bom` | yes | no |
| Database schema | shipped changesets byte-identical (§5.1) | `checkShippedChangesets` | `tandem-jdbc` | yes | no |
| Database schema | schema compatibility test (§5.2) | `schemaCompatTest` | `tandem-jdbc` | yes | yes |
| Published message | reference file (§8.2) | `test` | `tandem-kafka` | no | no |
| Admin API | oasdiff: validity, compatibility, frozen names (§6) | `checkOpenApi`, `checkOpenApiCases` | `tandem-admin` | validity and cases: no; the rest: yes (tag) | yes |
| Spring properties | metadata diff (§7) | `checkSpringMetadata` | `tandem-spring-producer`, `tandem-spring-relay`, `tandem-admin` | yes | no |
| Effective defaults | reference files (§8.3) | `test` | the three Spring modules, `tandem-jdbc`, `tandem-kafka`, `tandem-micrometer` | no | no |
| Metric and span names | reference files (§8.4) | `test` | `tandem-micrometer`, `tandem-spring-relay`, `tandem-tracing-otel` | no | no |

The published library modules are those of HLD-api-stability §3. `tandem-bom` carries no code, so its
only gate is the list of modules it manages; `tandem-rabbitmq` keeps its own version and its own
contract (LLD-rabbitmq §9), so it has no gate here. The root build names the independently versioned
modules in a set, `independentlyVersionedModules` (today `tandem-rabbitmq`), and its convention gives the
library gates to every other published module but `tandem-bom`.

---

## 2. The baseline

### 2.1 The property

`tandemBaseline` is declared in `gradle.properties`. Its value is a library release published on
Maven Central, such as `1.0.0-rc.1`. When it is empty, every gate that reads a baseline is skipped
with the reason "no compatibility baseline", and so is the compilation of the schema compatibility
test (§5.2). A command line overrides it without an edit: `-PtandemBaseline=0.11.1` is the dry run of
§10.

A gate of Tandem's own task types reports a skip as a log line, not as a skipped task, because it still
runs the part that needs no baseline: the declared-breaks file (§3). It prints `Baseline comparison skipped: <reason>`, the reason being "no compatibility
baseline" or "new since <baseline>" (§2.4), at Gradle's lifecycle level, so a plain run shows it. The
line appears whenever the task runs, not when Gradle finds it up to date or takes it from the build
cache.

The property moves in its own commit, after a release has reached Maven Central (§11). A baseline
artifact that does not resolve fails the build, for every module the baseline release contains (§2.4):
a gate that cannot read its baseline must not pass.

**Rejected:** *a `val` in the root build script*, the way `tandemPin` and `tandemFloor` are declared.
Those two are claims one module makes; the baseline belongs to the whole build, and the dry run would
need an edit to the script.

### 2.2 Resolution from Maven Central

The root build substitutes the working tree for every published Tandem coordinate, so a configuration
that must resolve the baseline is exempted by name through `centralClasspaths`, the mechanism
`floorTest` and `pinnedTest` already use. The gates add these configurations:

| Configuration | Holds | Read by |
|---|---|---|
| `compatibilityBaselineBom` | `com.codingful:tandem-bom:<baseline>`, its POM alone | every baseline gate (§2.4), `checkBomModules` |
| `compatibilityBaselineArchive` | `com.codingful:<module>:<baseline>`, the jar alone | `checkJavaApi`, `checkSpringMetadata`, `checkShippedChangesets` |
| `compatibilityBaselineClasspath` | the same coordinate with its runtime dependencies | `checkJavaApi` |
| `schemaCompatTestCompileClasspath`, `schemaCompatTestRuntimeClasspath` | `tandem-jdbc` at the baseline, its `tandem-core`, and the test libraries | `schemaCompatTest` |

A configuration missing from `centralClasspaths` is substituted silently, and its gate then compares
the working tree with itself. Every gate therefore checks that each Tandem component it resolved is a
published module at the baseline version, not a project, and fails otherwise.

### 2.3 Resolution from the tag

The OpenAPI document is not in any published jar, so `checkOpenApi` reads it from the tag, the object
`v<baseline>:docs/admin-api.openapi.yaml`. A missing tag fails the task with a message
asking for `git fetch --tags`; it never skips. CI fetches tags in its checkout (§9.3).

**Rejected:** *shipping the document in the `tandem-admin` jar* so that it resolves from Maven
Central like the rest. A new resource path in a published jar is a new frozen name, added for a file
git already serves.

### 2.4 Modules the baseline does not have

The baseline release's own BOM says which modules it contains: every published library module of a
release is managed by that release's `tandem-bom` (the BOM of 0.5.0, for example, manages neither
`tandem-cloudevents` nor `tandem-tracing-otel`, added later). Every baseline gate reads that list
first:

- **A module the baseline BOM does not manage is new.** Its baseline gates are skipped with the reason
  "new since <baseline>", and the checks that need no baseline (`checkInterfaceKinds`, its reference
  files) keep running. Once the baseline moves to a release that contains it, it is compared like any
  other.
- **A module the baseline BOM manages must resolve.** An artifact that cannot be fetched fails the
  build, as §2.1 says: it is a missing baseline, not a new module.

The same list catches a module that goes away. `checkBomModules` (`tandem-bom`) fails when a module
the baseline BOM manages is no longer managed by the current one (the POM the build generates for its
publication, read with the same parser), because a module removed, or moved to a version of its own,
breaks every application that declared it without a version. No other gate could notice it: the project
that would run the module's gates is the one that is gone. The declaration names the module:
`bom | tandem-bom | <module> | <release-notes line>`.

**Rejected:** *a list in the build of the modules added since the baseline*, emptied when the
baseline moves. A manual copy of what the baseline's published BOM already states.

---

## 3. Declared breaks

An intentional break is legitimate in a major version and between two release candidates
(HLD-api-stability §5). It is declared in one committed file, `gradle/accepted-breaks.txt`:

```text
# Breaks accepted against the baseline below (LLD-api-stability §3).
baseline 1.0.0-rc.1
japicmp | tandem-core | <the member, as the japicmp report names it> | <release-notes line>
spring-metadata | tandem-spring-relay | tandem.relay.some-key | <release-notes line>
bom | tandem-bom | tandem-some-module | <release-notes line>
openapi | tandem-admin | finding <fingerprint> | <release-notes line>
schema-compatibility | tandem-jdbc | * | <release-notes line>
```

1. **Entries are bound to their baseline.** When the file holds entries, its `baseline` line must equal
   `tandemBaseline`, or every gate fails. A file without entries is always valid. Moving the baseline
   therefore forces the file to be emptied, which is the rule "the declaration is removed when the
   baseline moves past it". While `tandemBaseline` is empty, the line is `baseline` alone and the file
   holds no entries: no gate compares, so an entry could match nothing, and it fails every gate.
2. **An entry must match a break the gate reports in this run.** One that matches nothing fails the
   gate: the break was reverted, or the entry is misspelt, and either way the release notes would lie.
   An entry naming a module where its gate does not run fails the same way, since no run could ever
   match it.
3. **Each gate accepts its own kind of item.** `japicmp` names a member, `spring-metadata` a property key, `bom` a module, `openapi` an oasdiff
   finding (its `fingerprint`, §6.2) or a frozen-name item (`problem-type <slug>` or
   `server <template>`, §6.3). `schema-compatibility` accepts only `*`: the test reports no
   item with a stable identity, so an entry accepts every incompatibility it finds against this
   baseline, including one that arrives after the entry. Nothing accepts a changed changeset (§5.1) or a
   finding of the OpenAPI validation (§6.1).
4. **A declaration accepts an incompatibility, never an error.** An entry applies only to an
   incompatibility the gate found by comparing. A failure that kept the gate from comparing (a
   baseline that does not resolve, a document oasdiff cannot load, a container that does not start, a
   scenario that does not compile) fails the gate whatever the file declares. §5.2 and §6.2 say where
   each of those two gates draws the line.
5. **The fourth field is the release-notes line**, written once, where the break is declared. At
   release time the entries are the **Breaking** section (§11).

The file is invalid, and every gate fails, when a line is neither a comment, the `baseline` line nor an
entry of four non-empty fields, when a second `baseline` line appears, or when an entry names a gate other
than the six above or repeats another entry.

The build cannot tell whether the next version is a major or a release candidate: that is known only
when the tag is pushed. Review and the release checklist hold that rule.

**Rejected:**

- *Declarations in each module's build script.* A break spreads over modules, its release-notes line
  sits apart from the others, and the binding to the baseline would be repeated per gate.
- *japicmp's exclusion lists.* An excluded member leaves the comparison altogether, so a later and
  different break of the same member would pass unseen.

---

## 4. Java API: japicmp

### 4.1 What is compared

`checkJavaApi` is a `JapicmpTask` of the plugin `me.champeau.gradle.japicmp`, in every published
library module. Its task type, `CheckJavaApi` in `buildSrc` (§9.1), extends the plugin's, so that it
validates the declared breaks and reports a skip the way the other gates do (§2.1):

- **Old side.** The archive is `compatibilityBaselineArchive`. The classpath is
  `compatibilityBaselineClasspath` plus the third-party part of the module's compile classpath: a
  Spring module declares Spring `compileOnly`, so its POM never lists it, yet japicmp needs it to
  resolve supertypes.
- **New side.** The archive is the module's `jar`; the classpath is its compile classpath.
- `ignoreMissingClasses` stays off: a type japicmp cannot resolve fails the task instead of hiding a
  supertype change.
- Packages excluded: `*.internal` and `*.internal.*` (HLD-api-stability §3).
- Access level: `protected`, since a protected member of a public non-final class is API to a
  subclass.
- Only modified elements are reported. Every comparison writes three files, whether or not the task
  fails: the rich report `build/reports/japicmp/checkJavaApi.html`, each break with its verdict;
  japicmp's markdown report `build/reports/japicmp/checkJavaApi.md`, every modified element; and the
  outcome `build/compatibility/java-api.txt`, the declarations used and the problems found (§4.2).

**The markdown report applies the filters.** The plugin hands the rich report's rules the comparison
as japicmp's report filter leaves it, and that filter runs, in place, when the markdown report is
written: it removes what is less accessible than `protected`, synthetic members and unmodified
elements. Without the markdown report, the rules would judge package-private types and members too.

### 4.2 What fails

The plugin's own failure switches stay off. The task fails through the rules of its rich report,
which live in `buildSrc` (§4.4):

1. Every change japicmp marks binary incompatible or source incompatible is an error, except:
2. a method added to a **provided** interface (japicmp's `METHOD_ADDED_TO_INTERFACE` on an interface
   §4.3 lists as provided), which
   HLD-api-stability §4.1 allows in a minor version;
3. a change that matches an entry of the declared breaks (§3), reported with its release-notes line.
4. After the comparison, an entry of the declared breaks that no change used is an error (§3, rule 2).

**A break is named by its item**, the third field of a `japicmp` declaration, written the way the
report names the element:

| Element | Item |
|---|---|
| Type | Its binary name: `com.codingful.tandem.core.port.CausalContext`, `com.codingful.tandem.core.OutboxRecord$Builder` |
| Method | Its type, name and parameter types: `com.codingful.tandem.core.OutboxRecord$Builder.lamport(java.lang.Long)` |
| Constructor | Its type and parameter types: `com.codingful.tandem.kafka.KafkaRelay(java.util.Map,com.codingful.tandem.kafka.KafkaMessageEncoder,com.codingful.tandem.core.port.TandemSpanRecorder)` |
| Field | Its type and name: `com.codingful.tandem.core.TandemHeaders.CAUSATION_ID` |
| Implemented interface | `<type> implements <interface>` |

A declaration accepts every incompatible change of the element it names. The failure lists each
undeclared break with japicmp's change types and the declaration that would accept it, its
release-notes line left to write.

**The rules judge each element on its own changes**, including those japicmp records on the parts the
rich report does not visit apart: a type's superclass, a method's return type and parameters (a change
of type arguments alone is source incompatible). Two cases are decided for a type as a whole:

- **A type japicmp marks incompatible only through its members** has no item of its own: it is
  accepted when every incompatible member it contains is accepted by rule 2 or 3. A test of the rules
  covers that case: an abstract method added to a provided interface passes, the same method added to
  an SPI interface fails, and so does its type.
- **A type removed, or made less accessible than `protected`, is one break**, named by the type. Its
  members go with it and are not judged apart, since none of them is API any longer: a public
  constructor of a class that is no longer public is not a second break. A declaration naming such a
  member matches nothing, and is stale.

**The outcome.** japicmp and the rules run in the plugin's worker process. An error in the rich report
fails the worker; the last rule writes the outcome first. The task reads it once the worker ends: it
logs each declaration used, with its release-notes line, and fails with the undeclared breaks and the
stale declarations, pointing to the rich report. A comparison that fails before the rules conclude,
such as one with a class japicmp cannot resolve, leaves no outcome, and fails the task whatever the
file declares (§3, rule 4).

Everything japicmp marks compatible passes: a new type, a new method, a new `default` method on any
interface, a new enum constant, a new record accessor.

The gate sees what Java's linkage and compilation see: a removed or less accessible type or member, a
class made final, a changed signature, a record component added without the overload of the previous
canonical constructor (HLD-api-stability §4.2). It does not see behaviour, record patterns (outside
the promise), annotations' meaning, or the jar manifest: `Automatic-Module-Name` stays with review
(HLD-api-stability §5, "Other names").

### 4.3 SPI or provided, from the javadoc

`checkInterfaceKinds` reads the module's `src/main/java`. Every public interface outside an `internal`
package, top-level or nested in a public type, must carry in its own javadoc exactly one of the two
paragraphs the Javadoc rules prescribe: `<b>SPI:</b>` or `<b>Provided:</b>`. An interface with
neither, or with both, fails the task. Annotation types are not interfaces for this rule. This part
needs no baseline, so it guards the Javadoc rule from the day it lands.

The kind is the one the current sources state. The task writes the binary names of the interfaces that
state they are provided (`com.codingful.tandem.jdbc.WakeupSource$Listener`, for example) to
`build/compatibility/provided-interfaces.txt`, the file the rule of §4.2 reads. The javadoc is what an
application developer reads, so the gate enforces what it promises, and adds no third list beside the
javadoc and the table of HLD-api-stability §4.1.

One change the task does not see: a paragraph rewritten from SPI to provided in the same change that gives
the interface an abstract method, which breaks every application implementing it. That rewrite is a line
of javadoc in the diff, and the release checklist reviews it (§11): every `<b>SPI:</b>` paragraph removed
since the baseline tag is a break unless the interface was not released.

**Rejected:**

- *An explicit list of provided interfaces in the build.* A third copy to keep in step, and an
  interface missing from it would silently take whichever kind the list implies by omission.
- *A committed file recording each interface's kind, updated when the baseline moves.* Another copy
  of what the baseline's published sources already state.
- *Reading the baseline's kind from its sources jar on Maven Central*, so that a reclassification from SPI
  to provided fails until declared. One more artifact per module and a table of cases, for a change that
  can only be made by editing the paragraph review reads.
- *An annotation marking provided interfaces.* It adds a type to `tandem-core`'s API, the reason
  HLD-api-stability §3 rejects a marker annotation for internal types.

### 4.4 Why the rules live in `buildSrc`

The plugin runs japicmp in a worker process and loads each rich-report rule there from the jar its
class comes from. A rule written in a build script has no such jar, so the rules are classes in
`buildSrc` (§9.1).

**Rejected:** *build scripts only.* Without a custom rule, "a provided interface may gain methods"
takes two japicmp tasks per module (one excluding the provided interfaces, one comparing only them
with `METHOD_ADDED_TO_INTERFACE` excluded), and declared breaks become exclusion patterns (§3).

---

## 5. Database schema

The generated `tandem-baseline.sql` already has its gate (`checkBaselineSql`, LLD-jdbc §6). Two gates
are added.

### 5.1 Shipped changesets stay byte-identical

`checkShippedChangesets` (`tandem-jdbc`) reads `tandem/schema/postgres/changelog/` from the baseline
jar:

- every changeset file (`v<n>-*.sql`) directly in that directory exists under
  `schema/postgres/changelog/` with the same name and the same bytes;
- `db.changelog-master.xml` grows with every schema version, so it is compared by content: the
  baseline's `<include>` and `<includeAll>` entries, in order, are the first entries of the current
  master. An entry is compared with all its attributes, since `relativeToChangelogFile` decides the path
  Liquibase records for the changesets it includes.

A baseline jar without the master fails the gate: the path is published (LLD-jdbc §6). The repository
checks out every text file with LF on every OS, so the bytes compare as they are. The task writes its
verdict to `build/compatibility/shipped-changesets.txt`.

No declaration accepts a difference, in a major version either: an operator's `DATABASECHANGELOG`
holds the checksum of every changeset it applied, so a shipped changeset never changes (LLD-jdbc §6).
A schema change is a new changeset. The declared breaks have no gate kind for this gate, so no entry can
name it; the task still reads the file, so an invalid one fails it like every other gate (§3).

### 5.2 Schema compatibility test

`schemaCompatTest` answers two questions about a database the baseline release and the current
release share during an upgrade (HLD §1.4): does the current changelog apply to a database the
baseline has populated, and does the baseline still work once it has.

**Source set and classpaths.** `src/schemaCompatTest/java` in `tandem-jdbc`. Its compile and runtime
classpaths hold `tandem-jdbc` at the baseline from Maven Central (with the `tandem-core` it brings),
Testcontainers PostgreSQL, the PostgreSQL driver, Liquibase (`liquibase-core`) at the version the build
already uses, JUnit and AssertJ, and no project dependency. `SchemaCompatTest.register` (§9.1) creates
the source set and declares `tandem-jdbc` at the baseline on it; the module's build script adds the
libraries. Both classpaths are in `CompatibilityBaseline.CONFIGURATIONS`, which the root build's
`centralClasspaths` includes, and the task applies the check of §2.2 to both. Compiling against the
baseline is the point: the scenario can use only API the baseline has. When a moved baseline stops the
scenario compiling, the commit that moves it updates the scenario. Without a baseline, the source set's
compilation and the task are both skipped, and the task logs the reason (§2.1).

**Docker.** The task starts PostgreSQL through Testcontainers, at the image the root convention passes
(`tandem.test.postgres.image`, or `TANDEM_TEST_POSTGRES_IMAGE`), and is added to
`dockerBoundTestTasks`. Without either, it starts the image `TandemTestContainer` defaults to; the
scenario states that default again, since no project of the build is on its classpath. Liquibase runs
with its analytics off, so the test reaches nothing outside the container.

**The scenario**, in four phases on one database:

1. **Baseline schema.** An empty database receives the changelog the baseline jar ships, applied with
   Liquibase at its published classpath path, `tandem/schema/postgres/changelog/db.changelog-master.xml`.
2. **Baseline writes.** The baseline first runs a relay's startup steps (the bucket-count check, the
   control source's start, the `LEASE` validation and a heartbeat), so every `tandem_*` table holds rows
   at the upgrade. The adapter then writes in every `seq` mode (`seq(long)`, `managedSeq()`,
   `unsequenced()`, each with and without `lockedWrite()`): one aggregate per combination, its first row
   through `insert` and its second through one `insertAll` across the aggregates. It claims the head of
   each aggregate and completes half of them; the other half is still in flight at the upgrade.
3. **Upgrade.** Liquibase updates the database with the current changelog, read from `tandem-jdbc`'s
   processed resources under the same published path. The shared path is what lets Liquibase recognise
   the changesets phase 1 applied, exactly as for an operator whose `spring.liquibase.change-log`
   points into the jar they upgrade. On the way, Liquibase validates their checksums. The step asserts
   that every changeset recorded before is still recorded, and that every row keeps its id and status.
4. **Baseline on the upgraded schema.** The baseline adapter exercises every statement it owns, one
   step per row below.

| Step | What the baseline does, and what the step asserts |
|---|---|
| Rows claimed before the upgrade | Completes them; the rows behind them are claimed with what was written (`seq` and its source, type, payload, headers) |
| Writes in every `seq` mode | Writes again as in phase 2; the backlog grows by the rows written, each is claimed as written, a managed number grows, and a number an aggregate already used is refused (`DuplicateSeqException`) |
| Retry | A retry with no delay is claimed again at once, with its attempt and error; one with a delay is not claimed before it is due |
| Failure | The row is `FAILED` with its error, its aggregate's later rows are held back, and the failed and blocked counts grow by one and two |
| Lease reclaim | An expired lease returns the row to `PENDING` with one more attempt, until the last attempt makes it `FAILED` |
| Cleanup | Deletes nothing older than an earlier cutoff, then the finished rows in batches of the size asked, and no other row |
| Bucket ownership | Under `LEASE`, through `BucketSource.forCoordination`: a second relay joins and the buckets split in two disjoint halves, with none uncovered; it leaves and the first takes them all back |
| Control state | Through `JdbcRelayControlSource`: start, heartbeat, and a whole-relay and a bucket pause the Admin API writes, read on refresh and resumed |
| Bucket count | `BucketCountGuard.check` passes with the stored count and refuses another |
| Outbox search | Through `JdbcOutboxQuery`: the status counts grow by what the step writes, the search filters (aggregate, aggregate type, type, status, correlation id, time window, after id, limit) return the expected rows, and the detail returns the payload and headers |
| Discard and replay | Through `JdbcDiscardService` and `JdbcReplayService`: only a failed row is discarded, with its reason; a dry run counts without changing; a replay re-queues only the eligible rows and counts the replay |
| Relay views and control | Through `JdbcRelayQuery` and `JdbcRelayControl`: the coordination mode, the relay status, the workers and the bucket views follow a bucket's pause, resume and release, and a whole-relay pause |

Each step asserts what it observes (rows returned, statuses after each transition, counts), not only
that no exception was raised: a changed column default or a narrower constraint can let a statement
run and still change what it returns. Each step works on aggregates of its own, each in a bucket no
other aggregate of the scenario uses, and claims only their buckets. What a step observes beyond the
adapter's return values it reads with plain SQL on the columns its own statements write, so a step
fails on what it exercises, not on another step's statement. When phase 3 fails, the steps of phase 4
are skipped rather than failed: they need the upgraded schema.

The scenario **calls the adapter's operations directly** rather than running a `WorkerPool`. The
statements are what a changeset can break, and calling them in order keeps the test deterministic,
with no waiting on worker threads. The only `WorkerPool` wiring every baseline shares is the
three-argument constructor, which never touches the lease tables.

The Admin API's operations go through the JDBC services it runs on, not through HTTP: every statement
the Admin API issues lives in `tandem-jdbc`, and the HTTP contract has its own gate (§6).

**What it catches:** a column removed or renamed; a `NOT NULL` column without a default (phase 3
fails on the populated table); a narrower `CHECK`; a changed default; an index or constraint the
baseline's `ON CONFLICT` relies on; a shipped changeset that changed (checksum validation, besides
§5.1).

**What it does not catch:** rows the current release writes and the baseline then reads, such as a
new status or `seq` mode met by a baseline relay. Both releases would have to share one classpath.
That direction stays with the tolerant-reader rule of HLD §1.4 and with review. Nor the wakeup's
`pg_notify`: it touches no table, and no constructor common to the baselines switches it on.

**Declared break:** `schema-compatibility | tandem-jdbc | *`, in a major version that breaks the schema
on purpose; its release-notes line says what operators must do before upgrading. Phases 1 and 2 run as
the scenario's setup, and every step of phases 3 and 4 reports on its own. The declaration accepts
only the failure of a step of phase 3 or 4, which is where the current changelog meets the baseline.
A failure before (the container does not start, the baseline changelog does not apply, the baseline
adapter fails on its own schema) or a scenario that does not compile is not a comparison, and fails
the task whatever the file declares (§3, rule 4).

**How the task applies it.** The setup runs in the scenario's `@BeforeAll`, and each step is one of its
test methods, run in order. `SchemaCompatTest`, a `Test` task in `buildSrc` (§9.1), runs the tests
without failing on them and records every failed test; `SchemaCompatOutcome` then judges. A failed test
method is a failed step. A failure Gradle reports against the class (`initializationError` for the
setup, `executionError` for the teardown) is not, and fails the task whatever the file declares; it may
have kept the steps from running, so it leaves a declaration's staleness undecided. Failed steps with
no declaration fail the task, which prints the declaration to write. With a declaration, the task logs
`Declared break accepted: <entry>, release notes: <line>, failed steps: <names>` and passes; a
declaration with no failed step is stale and fails it.

**Inputs.** Besides a test task's own, the task declares the baseline, the baseline BOM, the declared
breaks and the module's processed resources, which hold the current changelog. It hands their directory
to the scenario in the system property `tandem.schema-compat.current-resources`, an input compared by
content and relative path rather than by its location.

**Coverage.** JaCoCo is off for the task. The scenario runs the baseline release's classes, whose names
are the working tree's, and the module's merged report and the aggregated one read every `.exec` file:
their hits would be attributed to the current classes.

**Rejected:**

- *An empty database built from the current changelog.* It misses a changeset that fails on a
  populated table, and the rows a rolling upgrade leaves claimed.
- *A separate unpublished module.* The module registration checklist, for a test whose subject is
  `tandem-jdbc`'s SQL.

---

## 6. Admin API: oasdiff

`checkOpenApi` (`tandem-admin`) checks `docs/admin-api.openapi.yaml` in three ways: it must be valid
OpenAPI, compatible with the document at the baseline tag, and keep the names the contract freezes. The
first two run oasdiff (Apache 2.0 licensed); the third is the task's own code.

oasdiff runs from its container image, `tufin/oasdiff`, initially at **1.33.0**, pinned by tag and
digest in the version catalog: `oasdiff-image`, whose digest is the one of the image's multi-platform
index, so the same pin serves `linux/amd64` and `linux/arm64`. The commands, rule ids and finding
identities below are checked against that release. An image upgrade reruns the cases of §6.4, and fails
until the rules it adds are classified (§6.2). `check` already needs Docker for the integration tests,
and in CI the image comes through the Docker Hub mirror like every other one.

Each command is one `docker run --rm`, without network. It mounts the repository and the task's own
directory under `build/openapi/` (the baseline document and the severity file the task writes), both
read-only: oasdiff prints its findings on its standard output, and the task keeps what each command
printed under `build/reports/openapi/`. The container lives for one command, so it takes no Docker
slot, which exists for brokers.

The task concludes in lines, `<kind> <text>`, and passes when every line is `accepted`: `invalid` for
a finding of the validation, `undeclared` or `accepted` for a break and whether a declaration names it,
`stale` for a declaration that matched nothing, `error` for a command that kept the task from
comparing. It writes them to `build/compatibility/openapi.txt`, and a failure prints the ones that fail.
oasdiff's text is put on one line there, since a validation message can quote a schema over several.

### 6.1 Validity

`oasdiff validate docs/admin-api.openapi.yaml --fail-on INFO --format json` runs on every build, with
or without a baseline: the document must give no finding at all. It reports a missing `info` object, a
schema `type` OpenAPI does not define, a minimum length above the maximum, an example its schema
rejects. Each finding is a line `invalid <rule-id>: <text>`. No declaration accepts a finding (§3): an
invalid contract is fixed, never excused. The command exits 0 without a finding and 1 with some; any
other exit code, or 1 without a finding, is an error.

This is the validation HLD-admin-api §1 asks of CI. A comparison between two documents does not
replace it: two documents can agree with each other and both be invalid.

### 6.2 Compatibility

1. The task writes the baseline document to `build/openapi/baseline.yaml` from the tag (§2.3). Its input
   is the git object of the document at the tag, `git rev-parse v<baseline>:docs/admin-api.openapi.yaml`,
   so the comparison reruns when the content changes and the task stays up to date otherwise. Without a
   baseline, or for a module new since it (§2.4), it reads nothing from git.
2. `oasdiff breaking build/openapi/baseline.yaml docs/admin-api.openapi.yaml --format json
   --severity-levels build/openapi/severity-levels.txt` lists the `WARN` and `ERR` findings after
   applying Tandem's levels, with their rule id, level, method, path, text and `fingerprint`. The
   severity levels are the committed policy file without its comments (below). The output is kept
   under `build/reports/openapi/`. Each finding is a line
   `finding <fingerprint>: <level> <rule-id> at <method> <path>: <text>`, its location being its
   section of the document when it has no operation; a finding at `INFO`, should one be listed, is not.
3. Every finding of level `WARN` or `ERR` fails the task unless an entry of the declared breaks names
   its exact `fingerprint`: `openapi | tandem-admin | finding <fingerprint> | <release-notes line>`.
   Entries that match no finding are stale, and fail it too.
4. Any exit code other than 0 (oasdiff's own: 102 for a document it cannot load, 106 for a severity
   file it cannot read; 2 for a crash) fails the task, whatever the file declares (§3, rule 4). The
   task decides from the JSON, not from oasdiff's exit code, so a comparison and an error never share
   a code. Output that is not the JSON array oasdiff writes, or a finding of an unknown level, is an
   error too. While the comparison did not run, its `finding` entries are undecided: none of them is
   reported stale.

**A declaration identifies one change.** The rule id, method and path are not enough: making two
query parameters required on one endpoint produces two findings with the same three values.
oasdiff's [fingerprint](https://github.com/oasdiff/oasdiff/blob/v1.33.0/checker/fingerprint.go) also
includes the change's arguments, such as the property or parameter and the values the rule reports;
it excludes filenames and line numbers. The report prints the declaration item next to the full
finding, so review can see exactly what it accepts. A missing fingerprint, or one shared by distinct
findings, fails the task rather than broadening a declaration. Reformatting a document leaves the
declaration valid; changing the affected parameter or the reported old/new values requires a new one.

**The severity file is Tandem's compatibility policy, written where oasdiff applies it.** The
upstream defaults answer whether an existing client can still use the API. Tandem also promises
additive evolution and stable defaults (HLD-admin-api §1, HLD-api-stability §2), so some changes
upstream calls `INFO` must fail here. Conversely, Tandem's tolerant readers accept new enum values.

| Change | Tandem level | Rule ids or families to cover |
|---|---|---|
| A response enum gains a value | `INFO` | `response-property-enum-value-added` |
| A published element is removed, including an optional field/header or a deprecated endpoint/parameter | `ERR` | Includes `response-optional-property-removed`, `optional-response-header-removed`, `api-path-removed-with-deprecation`, `api-removed-with-deprecation`, `request-parameter-removed-with-deprecation` and the other removal rules for contract elements |
| A response field becomes required, or a new response field is required | `ERR` | `response-property-became-required`, `response-required-property-added`; request-side obligations already fail |
| A default is added, changed or removed on an existing element | `ERR` | Every `*-default-value-added`, `*-default-value-changed` and `*-default-value-removed` rule, for request parameters, bodies and properties, and response bodies and properties |
| An existing wire type or format changes | `ERR` | All type-change rules, including those upstream calls `type-compatible`, `type-generalized` or `type-specialized`, on requests as well as responses |
| An existing value range narrows | `ERR` | Request and response constraints: minimums raised, maximums lowered, new bounds, enum values removed and null no longer allowed; include the response rules upstream calls `INFO` |

The table names families, not a syntax oasdiff accepts: the file covers each family's body,
property, parameter and header variants by their literal rule ids. A removal of a constraint is not a
removal of a contract element: compatible relaxations of request constraints, documentation edits,
new endpoints, and new optional fields retain their compatible levels. A default on a new optional
field is part of that addition, not a change to an existing default. Other upstream `WARN` and `ERR`
findings keep failing. In particular, request restrictions must never be lowered to make response
enum growth pass.

**The file lists what Tandem sets apart from upstream.** `breaking` reports only `WARN` and `ERR`, so a
rule at `INFO` passes. The file raises to `ERR` the rules the table above says must fail, and lowers the
one rule of the table upstream calls an error; every other rule keeps its upstream level, and the rules
left at `INFO` on purpose (an element added, a request constraint relaxed, documentation, annotations and
lifecycle) are named in the file's comments rather than listed. oasdiff refuses a rule id it does not
know, so a misspelt line fails the next comparison, the acceptance cases included (§6.4).

**The file is reviewed against one image.** A line `reviewed-with <image>` names the image the
classification was made with, and the task fails on every build while the version catalog pins another
one. An image upgrade therefore fails until someone lists the `INFO` rules the new image adds, with the
command the file's header states, classifies each, and moves the line: the review happens in the change
that upgrades the image, not later.

**Comments live in the committed file only.** oasdiff rejects a severity file with a comment or a
blank line. `gradle/oasdiff-severity-levels.txt` groups its rules under comments naming the contract
rule each group implements; the task strips comments and blank lines into
`build/openapi/severity-levels.txt` and hands that copy to oasdiff. A line is `<rule-id> <level>`, the
level being `err`, `warn` or `info`. The task refuses `none`, which oasdiff accepts, because it would
hide the rule from the changelog as well, and a rule listed twice, whose level would depend on the
order.

**The classification of 1.33.0.** Of its 399 `INFO` rules, the file raises 191 to `ERR`. The table settles
most of them. Those it leaves open sit in groups headed "Judgement calls", each with its reasoning, so
review reads the decision where it is applied:

- names a code generator builds from: a component schema, an operation id or a tag removed (or
  renamed) is `ERR`, since clients are generated from the contract;
- the document's own version not bumped after a break stays `INFO`: the declaration records the break;
- a documented non-success status removed is `ERR`, a new success status `INFO`;
- a security scheme or an OAuth scope removed from the components is `ERR`;
- a discriminator or a mapping added is `INFO`; one removed, a mapping deleted or changed, or its
  property renamed is `ERR`;
- readOnly and writeOnly: a rule about a property on the side it never travels on (read-only in a
  request, write-only in a response) stays `INFO`, because the other side reports its own rule; a
  property that stops travelling on a side is a removal there, and one that starts travelling as
  required is a new obligation, both `ERR`;
- a schema that stops being the boolean `false` is a new element, `INFO`.

**Rejected:** *listing every `INFO` rule of the pinned image*, the 208 kept at `INFO` included, with the
task reading the image's catalog to fail on any rule the file lacks. It names each new rule at an image
upgrade, at the cost of a file twice as long and one more container per build; the `reviewed-with` line
forces the same review, and the command lists the same rules.

### 6.3 Frozen names

Two names of the contract live where no comparison of schemas reaches them: the problem `type` URLs,
which appear in examples and descriptions while the schema types the field only as a URI string, and
the server URL that carries the contract's version, `{basePath}/v1`, with the variables that complete
it. With a baseline set, the task reads both documents as oasdiff loads them,
`oasdiff upgrade --format json`, a rendering that rewrites schema keywords only, so servers and
strings reach the task as the document states them. It fails when:

- a problem `type` URL (`https://tandem.codingful.com/problems/<slug>`) of the baseline document no
  longer appears in any string of the current one (HLD-admin-api §1: a slug never changes once
  published);
- a server definition of the baseline is gone, or changed what a client can call (below).

**Servers are compared at the document level.** The task compares the document's servers, its own or
else OpenAPI's default `/`, matching them by their literal URL template, not their position. Each
baseline template must remain, every variable it references must keep its default, and a variable's
allowed values may grow but not lose any; a variable without an enum must not gain one. Descriptions and
declaration order do not matter, and a server added passes. A default changed with the template
unchanged fails: `{basePath}/v1` with `basePath` moving from `/tandem/admin` to `/other/admin` sends every
client that relies on the default to another address.

A path or an operation that declares servers of its own would escape that comparison, so the task refuses
it: each such level is a line `invalid server-override path <path>` or
`invalid server-override operation <METHOD> <path>`, which no declaration accepts. Tandem's contract
declares its servers once, for the whole API; should it ever need an override, the comparison is extended
to that level in the same change.

The finding's item names the template, `server <template>`; the line prints what changed next to it, the
old and new default or allowed values, or the servers left when the template is gone. A declaration
accepts every change to that server against this baseline, which is what one release-notes line about it
describes. Each problem slug's page under `site/problems/` stays guarded by the module's own tests; a
slug's declaration is `openapi | tandem-admin | problem-type <slug> | <release-notes line>`.

**Inputs.** The git object of the baseline document, the current document, the severity file, the declared
breaks and the image reference (§9.2): the comparison reruns when the content it compares changes, not
only the tag's name.

**Rejected:**

- *Specmatic.* It is a contract-testing tool, and its comparison is not a validator nor Tandem's
  notion of compatibility: it accepts a document without `info` or with an unknown schema `type`,
  treats a new response enum value as breaking with no setting to change that, and turns an internal
  error into the same verdict as an incompatibility, so a declaration could not tell them apart.
  Neither it nor oasdiff sees the frozen names of §6.3.
- *A gate of our own on swagger-parser and openapi-diff*, both on Maven Central. swagger-parser
  accepts an unknown schema `type`, openapi-diff treats a new response enum value as breaking, and
  Tandem's rules would have to be written on top of openapi-diff's model: the most code to own.
- *oasdiff from a downloaded release binary.* No Docker, but a download with a checksum per platform
  to maintain, for a build that needs Docker already.
- *oasdiff in the CLI's Go job only.* Outside `check`, so a local run never sees it.

Out of scope: generative conformance testing of the implementation. The implementation's conformance
stays with the response validation in the integration tests (HLD-admin-api §5).

### 6.4 Acceptance cases for the gate

The cases live in `libs/tandem-admin/src/openApiGateCases/`: `contract.yaml`, a copy of the contract frozen
there, then one directory per case. A case holds `expected.txt` and, where they differ from the frozen
contract, its document `openapi.yaml` and its baseline `baseline.yaml`; a missing one is the frozen contract.
It may also hold `accepted-breaks.txt`, its declarations in the format of §3, and a `severity-levels.txt`
handed to oasdiff as it is, in place of the policy.

`expected.txt` holds comment lines stating the change and the rule it exercises, then:

- `checks` and the parts of the gate the case runs: `validity` (`oasdiff validate` of the document),
  `compatibility` (`oasdiff breaking` against the baseline, with the policy), `frozen-names` (both documents
  as `oasdiff upgrade` loads them);
- `verdict pass` or `verdict fail`;
- the identity of every line the gate must conclude: the line without what follows its item, such as
  `undeclared finding d9112806f2f2`, `invalid info-required`, `undeclared server document {basePath}/v1` or
  `error breaking exit 106`. A compatibility finding is identified by its fingerprint, the item a
  declaration names; the comment names its rule.

A case holds when the gate reaches its verdict with exactly those identities, no more and no fewer, so a
mutation cannot pass on a finding it did not cause. `checkOpenApiCases`, a task of `tandem-admin` built on the
same `buildSrc` code as `checkOpenApi`, runs the cases one after the other with the pinned image. A case runs
one oasdiff command per part it checks, and a document read by `oasdiff upgrade` is read once for every case
that compares it. Its inputs are the case files, the image reference and the severity file, so it runs again
whenever one of them changes, and it is part of `check`.

Three cases run on the frozen contract, where its size, references and inherited servers are exercised:

| Case | Change | Expected |
|---|---|---|
| `unchanged` | None, through the whole gate | Pass |
| `declared-fingerprint` | `aggregateId` and `aggregateType` made required on one operation, the first declared | The first accepted, the second undeclared: one rule, method and path, two fingerprints |
| `slug-and-server` | A problem slug renamed; the document's `basePath` default changed; a path and an operation declaring servers of their own | Fail: one finding for the slug, one for the server, and the two overrides refused |

The others are minimal documents, one endpoint and the schema its change needs, one case per rule family of
§6.1 and §6.2 rather than one per body, property, parameter or header variant: the policy's completeness
check already gives every rule a level, and a case checks that a family's level is the one the table states.

| Case | Change | Expected |
|---|---|---|
| `invalid-document` | Missing `info`, unknown schema `type`, contradictory bounds, invalid example | Four validation findings |
| `response-enum-value-added` | A response enum gains a value | Pass |
| `compatible-changes` | A new endpoint; new optional fields, with a default; a request maximum raised; a description added | Pass |
| `optional-response-field-removed` | An optional response property removed | Fail |
| `response-field-made-required` | A response property made required | Fail |
| `default-changed` | A query parameter's default changed | Fail |
| `type-widened` | A query parameter's type widened, which upstream calls compatible | Fail |
| `response-range-narrowed` | A response property's maximum length lowered | Fail |
| `declared-default-changed-again` | A default change declared, then its new value changed again | Fail; the declaration is stale |
| `declared-finding-reformatted` | A declared change in a document whose keys, styles and lines all moved | The declaration still matches |
| `document-cannot-load` | A document oasdiff cannot load, with a declaration | Fail; the declaration is neither applied nor stale |
| `severity-file-cannot-load` | A severity file with a comment line | Fail |

Each fail case changes a rule upstream classes as `INFO`, so it fails only through the policy; the request-side
errors upstream already raises are in `declared-fingerprint`. The variants of a server change (an enum widened,
narrowed or introduced, a server added or reordered) are the task's own logic on the documents as oasdiff loads
them: `FrozenNamesTest` covers each, and `slug-and-server` shows that the loaded documents keep the servers of
the document, of a path and of an operation.

Three failures are ones the pinned image does not produce: a crash exit code, output that is not JSON, and a
finding without an identity of its own. `buildSrc`'s unit tests feed them to the same code (`OpenApiGateTest`).

---

## 7. Spring properties: metadata diff

`checkSpringMetadata` runs in `tandem-spring-producer`, `tandem-spring-relay` and `tandem-admin`, the
modules the root build names in `springMetadataModules`. It compares
`META-INF/spring-configuration-metadata.json` in the baseline jar (`compatibilityBaselineArchive`) with
the same file in the module's `jar`, and fails when a property of the baseline:

- is absent, or carries a `deprecation` of level `error` (Spring's way of declaring a key removed);
- declared a `type` and now declares another one, or none;
- declared a `defaultValue` and now declares another one, or none.

New properties pass, and so does a type or a default declared where the baseline declared none.
Descriptions, groups, hints and source types are ignored, and so is a deprecation of level `warning`.
Defaults are compared as the JSON values the file holds, so `256` and `"256"` differ. A property the
baseline already declares removed (level `error` there) is not compared: nothing of it is left to keep,
and a key removed in one major version would otherwise fail every later one.

A break is named by its key, and a property that breaks in more than one way is one break, its reasons
on one line. A declared break names the key: `spring-metadata | <module> | <key>`. The failure lists
each undeclared break with the declaration that would accept it, and each stale declaration. The task
logs each declaration it used with its release-notes line, and writes its verdict to
`build/compatibility/spring-metadata.txt`.

A file the gate cannot compare is an error, which no declaration accepts (§3, rule 4): a jar without
the metadata, a file that is not the JSON the processor writes, or a key declared twice with a
different type, default or deprecation level. A key declared twice the same way is read once.

**A complete file in every jar.** The configuration processor reads descriptions from the sources of
the classes the compiler processes, so an incremental compilation, which recompiles only some, writes
metadata without them. `compileJava` in the three modules is therefore non-incremental: every
compilation regenerates the whole file, so the jar the gate reads, like any jar built locally, carries
the complete metadata. The three modules are small, and the cost is a few seconds when one of them
changes. The setting is not part of the build cache key, so `compileJava` also declares an input of its
own, `fullCompilation`: an output an incremental compilation stored, before the setting or on a branch
without it, has another key, and the cache never hands it back for the same sources.

**Rejected:** *a second full compilation into a directory of the gate's own.* The module's jar could
still carry incomplete metadata from a local incremental build, and the gate would check a file nobody
ships.

The metadata declares a default only where a property record does (HLD-api-stability §5), so this
gate guards keys and types, and the effective defaults have the reference files of §8.3.

---

## 8. Reference files

### 8.1 Mechanism

- **Location.** `src/test/resources/reference/` of the module whose test renders the file. Each file
  belongs to one test, which compares it whole.
- **The test** renders the output from production code and compares it with the file. On a difference
  it fails with a unified diff and the command that regenerates the file.
- **Helper.** `ReferenceFile`, in `tandem-test`'s test fixtures next to
  `ConfigurationMetadataReference`, compares a rendering with its file or, in write mode, writes it.
- **Regeneration.** `./gradlew test -PupdateReferenceFiles` runs the tests with the reference tests
  in write mode: each rewrites its file instead of comparing, and the change then shows in the diff,
  where review judges it. The property is an input of every `Test` task, and with it set a `Test` task
  is never up to date, so neither the build cache nor an up-to-date check replays a result across the
  two modes. The build refuses the property when the `CI` environment variable is set, so a CI run
  always compares.
- **Format.** UTF-8, LF, one fact per line, a final newline, and a first comment line naming the test
  that renders the file and the regeneration command. Nothing that depends on the time or the machine.
- **Reading a change.** A line added is a minor version; a line changed or removed is breaking
  (HLD-api-stability §5). A reference file has no baseline and no entry in the declared breaks: the
  edit to the file is the declaration, and the release checklist collects it (§11).
- **Literals.** A reference file writes wire names as literals, and its test compares against the
  file rather than against the constants: built from the constants, it could not notice a constant
  whose value changed. This is the one exception to AGENTS.md, Testing §4 (§11).

**Rejected:** *a dedicated `updateReferenceFiles` test task.* Every `Test` task feeds
`jacocoMergedReport` and the aggregated coverage report, and `check` runs both, so a regenerating task
would run in every CI build and rewrite the files before they are compared. Excluding it by name from
both reports is one more list matched by name, which fails silently the day a third consumer of every
`Test` task appears.

### 8.2 Published message

The test in `tandem-kafka` renders two reference records through each built-in envelope on Kafka:

| File | Encoder |
|---|---|
| `message-cloudevents.txt` | `KafkaMessageEncoder.cloudEvents(router, config)` |
| `message-raw.txt` | `KafkaMessageEncoder.from(new RawMessageEncoder(router))` |

The router is the default topic router, so its naming is pinned as well. The two records have a fixed
id, aggregate and creation time:

- **complete**: every optional field set (event type, an application `seq`, stored content-type and
  data-schema headers, an application header, a correlation header), encoded with a configuration
  that also sets a default data schema;
- **minimal**: unsequenced, no event type, no header, no content type.

Together they pin which headers each envelope adds, and which it leaves out when a field is unset.
Each record renders as its topic, its key, one line per header with its value as UTF-8 text, and its
body as UTF-8 text:

```text
[complete]
topic <topic>
key <aggregate id>
header <name> <value>
body <payload>
```

CloudEvents headers are sorted by name, since the binary mode gives their order no meaning. Raw
headers keep the order the encoder produces, since the raw envelope promises the stored order
(LLD-alternative-envelope §2.3).

The AMQP binding belongs to `tandem-rabbitmq` and its own contract.

### 8.3 Effective defaults

| File | Module | Values |
|---|---|---|
| `effective-defaults.txt` | `tandem-spring-producer`, `tandem-spring-relay`, `tandem-admin` | every `tandem.*` key the module binds, as a Spring application gets it |
| `defaults.txt` | `tandem-jdbc` | every accessor of `RelayConfig.defaults()`; the base and cap of `BackoffStrategy.fullJitter()` |
| `defaults.txt` | `tandem-kafka` | `KafkaRelayConfig.of(source)`; the Kafka producer settings Tandem applies |
| `defaults.txt` | `tandem-micrometer` | the publish-latency histogram's ceiling, as the one-argument constructor sets it |

The two kinds of file are kept apart because they can differ. The Spring tier hands some values to
the classes itself (the bucket count comes from its own property, whatever `RelayConfig`'s builder
defaults to), and some values have no key at all (`RelayConfig.deliveryTimeoutMs`). An application
wired without Spring gets the classes' defaults, so those are recorded where the classes live.

**Spring modules.** The test starts the module's autoconfiguration with no `tandem.*` key set and only
the beans startup requires. The keys come from the module's generated metadata, so every key gets a
line and a key without one fails the test. Each value is read from the object that applies it: a
public accessor (`RelayConfig`, `KafkaRelayConfig`), the meter it configures (the publish-latency
histogram's ceiling), the request mapping it produces (the Admin API's base path), the presence of the
bean it switches on. Where nothing exposes a value, a package-private accessor in the module's own
package provides it, the seam Testing §2 allows. A key whose effect shows only when another key is set
is read with that one key set, and nothing else: `tandem.admin.base-path` with `tandem.admin.enabled`.

**Classes wired without Spring.** Each module's test renders the defaults of the classes it ships,
from its own package, so a package-private class is reachable:

- `tandem-jdbc` reads every public no-argument accessor of `RelayConfig.defaults()` by reflection,
  so an accessor added without its line fails the test, and the base and cap of the backoff
  `BackoffStrategy.fullJitter()` returns;
- `tandem-kafka` renders `KafkaRelayConfig.of(source)` (default content type, default data schema),
  and the producer settings `KafkaRelay` applies to an empty producer configuration: those Tandem fills
  in when absent (`acks`, `enable.idempotence`, `retries`, `delivery.timeout.ms`) and those it always
  sets (`key.serializer`, `value.serializer`). In `tandem-spring-relay`'s file,
  `tandem.kafka.producer` itself has no default;
- `tandem-micrometer` renders the ceiling `MicrometerTandemMetrics(registry)` gives the
  publish-latency histogram.

Defaults that are collaborators rather than values (a no-op metrics recorder, the system clock, no
wakeup) are stated in the javadoc of each builder, not in a file.

**Rendering.** `key=value`, one line per key, sorted:

- a duration in Spring's short form with the largest exact unit (`14d`, `15m`, `30s`, `100ms`);
- an enum constant by its name;
- `<none>` for a key without a default, `<required>` for one startup fails without;
- `<derived: rule>` for a default computed at run time, after the test checks that the value follows
  the rule: `tandem.relay.workers-per-instance=<derived: availableProcessors() * 2>`;
- in `tandem-kafka`'s `defaults.txt`, `<always: value>` for a producer setting Tandem overrides even
  when the application sets it.

Classes' defaults are keyed by class and accessor (`RelayConfig.batchSize=100`). Reading the Spring
values off the running autoconfiguration records what a Spring application actually gets, including
where the Spring tier sets a value of its own.

**Rejected:**

- *A table in the test mapping each key to a class constant.* It cannot see a property record's
  default diverging from `RelayConfig`'s, nor a binding that converts a value.
- *Parsing the configuration guide's table.* It ties a contract check to the layout of a document.

The shipped reference configurations (`tandem-*-reference.yml`) state the same defaults in prose; their
own test keeps checking keys only (LLD-spring-config §2.4).

### 8.4 Metric and span names

**Meters** (`meters.txt`, `tandem-micrometer`). The test registers `MicrometerTandemMetrics` on a
`SimpleMeterRegistry` and calls every `TandemMetrics` method once, so the meters registered on first
use are listed too. It fails when `TandemMetrics` declares a method it does not call, so a new
recording method cannot add a meter the file never sees. One line per meter, sorted by name: its name,
its Micrometer type, the base unit it declares (`-` when none) and its tag keys.

```text
tandem.outbox.publish.latency timer - []
tandem.relay.config.invalid gauge - [check]
```

**Spans** (`spans.txt`, once in `tandem-spring-relay` for `MicrometerTandemSpanRecorder`, once in
`tandem-tracing-otel` for `OtelTandemSpanRecorder`). Each test records spans through its recorder into
an in-memory exporter: one with every optional attribute set, one with none. One line per span: its
name, its kind and its attribute keys, sorted.

```text
[complete] tandem.relay.publish producer [<attribute keys>]
[minimal] tandem.relay.publish producer [<attribute keys>]
```

Both recorders emit the same spans, so the two files hold the same lines; each recorder owns its file,
so a change to one recorder alone shows as one file changing.

Type, base unit and kind are recorded with the names because they reach the wire as much as the names
do: Prometheus builds the exported name from the meter's name, type and unit (a counter gains
`_total`, a timer its unit), and trace backends filter spans on their kind. HLD-api-stability §2
freezes them with the names.

---

## 9. Build and CI wiring

### 9.1 `buildSrc`

`buildSrc/` holds the gates' build logic, in Java, the repository's language:

- the baseline helper: the property, the configurations of §2.2, the check that they resolved
  published components, and the baseline BOM's module list (§2.4);
- the reader of the declared breaks (§3);
- the task types `CheckInterfaceKinds`, `CheckBomModules`, `CheckJavaApi` (the plugin's
  `JapicmpTask`, extended), `CheckShippedChangesets`, `SchemaCompatTest` (a `Test` task that applies
  the declared breaks to the scenario's outcome, §5.2), `CheckSpringMetadata`, `CheckOpenApi`, and
  `CheckOpenApiCases`;
- `BaselineGate`, the interface every task type that compares with the baseline implements (all but
  `CheckInterfaceKinds` and `CheckOpenApiCases`, which read none): the inputs
  they share (the baseline, the module, the baseline BOM, the declared breaks, the components the baseline
  configurations resolved), their registration into `check` and `checkCompatibility`, the reading of the
  declared breaks (§3, rules 1 and 2), and the skip line or the check of the resolved components (§2.1,
  §2.2). An interface rather than a base class, because `CheckJavaApi` and `SchemaCompatTest` extend task
  types of their own;
- the japicmp rich-report rules (§4.2);
- the OpenAPI gate's own code (§6): the run of the pinned image, the judge of its output against the
  declared breaks, the frozen names, the severity policy and the cases' expectations.

It depends on the japicmp plugin's artifact, at the version the version catalog pins, which puts the
plugin on every build script's classpath. The plugin is published on the Gradle Plugin Portal only, so
that is `buildSrc`'s one repository besides Maven Central, limited to the plugin's group. The tests of
the rules run the plugin's worker action on jars they compile, so the rules are loaded and fed the way
a build loads and feeds them; they add the Guava the worker puts on japicmp's classpath. The parser of
Java sources (§4.3) is the JDK compiler's own tree API, and the JSON of oasdiff (§6) and of the Spring
metadata (§7) is read with Groovy's `JsonSlurper`, part of the Groovy that Gradle's API carries, so
neither brings a library. `buildSrc` is not a module: it has no entry in the settings, is outside the
module registration checklist, publishes nothing, and adds nothing to THIRD-PARTY-NOTICES.md. A change
to it reconfigures the whole build, an acceptable cost for code that changes rarely.

**Its own tests run because its jar depends on them.** Gradle builds `buildSrc` for every invocation of
the main build but never runs its tests, and no task of the main build can depend on a task of
`buildSrc`. With the jar depending on them, they run wherever the build logic changed, `check` in CI
included. The cost: a failing test of the build logic stops every Gradle invocation, an IDE sync
included, until it is fixed.

Its code lives in `com.codingful.tandem.compatibility`. A package named `build` would sit in a directory
the repository's `.gitignore` excludes, and its sources would never be committed.

**Rejected:**

- *Build scripts only* (§4.4).
- *An included `build-logic` build.* It needs settings wiring and plugin declarations that bring
  nothing at this size.

### 9.2 Tasks

| Where | Added |
|---|---|
| root convention, every published library module | `checkInterfaceKinds`, `checkJavaApi`, the baseline configurations of §2.2 |
| `tandem-bom` | `checkBomModules` |
| `tandem-jdbc` | `checkShippedChangesets`, the `schemaCompatTest` source set and task |
| `tandem-admin` | `checkOpenApi`, `checkOpenApiCases` and its cases directory (§6.4) |
| root convention, the modules of `springMetadataModules` (`tandem-spring-producer`, `tandem-spring-relay`, `tandem-admin`) | `checkSpringMetadata`, non-incremental `compileJava` (§7) |
| root | `checkCompatibility`, which runs every gate that reads a baseline |
| root build script | `schemaCompatTest` in `dockerBoundTestTasks`; the configurations of §2.2 in `centralClasspaths`; the property `updateReferenceFiles` passed to every `Test` task, and refused in CI (§8.1) |
| `gradle.properties` | `tandemBaseline` |
| `gradle/` | `accepted-breaks.txt` (§3), `oasdiff-severity-levels.txt` (§6.2) |
| version catalog | the japicmp plugin, and the Guava its worker runs on for `buildSrc`'s tests; the oasdiff image, by tag and index digest (`oasdiff-image`); Liquibase as it already is |

Every gate is part of `check`. `checkCompatibility` serves the dry run (§10) and a quick run after the
baseline moves. A gate joins both as it is registered (`BaselineGate.register`), so neither
keeps a list of gate names, and a later gate registers the same way.

**Inputs.** Each gate declares as task inputs everything its verdict depends on: the value of
`tandemBaseline`, the baseline files it resolved (archive, sources, BOM, or the git object of the
document at the tag), the current files it compares, and `gradle/accepted-breaks.txt`. `checkJavaApi` also
takes the file `checkInterfaceKinds` writes (§4.3), and `checkOpenApi` the severity file and the image
reference (§6). Otherwise Gradle could keep a gate up to date across a change that alters its verdict:
a declaration removed after a comparison it let pass would leave the gate green until something else
changed. The Test tasks that read the declared breaks (`schemaCompatTest`) declare the file the same
way, with the current changelog they apply (§5.2).

### 9.3 CI

There is no new job: the gates run in the build job's `./gradlew check`. Its checkout fetches tags
(`fetch-tags: true`), so the baseline tag and its tree are present for `checkOpenApi`. The job already
reaches Maven Central for `floorTest` and `pinnedTest`, and pulls Docker Hub images, the oasdiff one
included, through the mirror it sets up before `check`.

---

## 10. Dry run against 0.11.1

Before `1.0.0-rc.1` is tagged, the gates run once against `0.11.1`, with the declared breaks empty:

```bash
./gradlew checkCompatibility -PtandemBaseline=0.11.1 --continue
```

Expected result:

| Gate | Expected |
|---|---|
| `checkJavaApi` | Fails in `tandem-core`, `tandem-jdbc`, `tandem-kafka`, `tandem-spring-producer`, `tandem-spring-relay` and `tandem-admin`, reporting exactly the breaking rows of HLD-api-stability §6 (list below). Passes in `tandem-cloudevents`, `tandem-test`, `tandem-micrometer` and `tandem-tracing-otel` |
| `checkInterfaceKinds` | Passes: every public interface states its kind (it reads no baseline) |
| `checkBomModules` | Passes: the BOMs of 0.11.1 and of the working tree manage the same ten modules, so no module is new or gone |
| `checkShippedChangesets` | Passes: no changeset since 0.11.1 |
| `schemaCompatTest` | Passes: the schema is unchanged, and the scenario compiles against 0.11.1 |
| `checkOpenApi` | Passes: the document is valid with no finding at any level, unchanged since `v0.11.1`, and so keeps every slug and server URL |
| `checkSpringMetadata` | Passes: the same keys, types and declared defaults in all three modules |

The breaks `checkJavaApi` must report:

- **`tandem-core`**: `CausalContext` and `LamportClock` removed; `OutboxRecord.lamport()` and
  `OutboxRecord.Builder.lamport(Long)` removed; `CloudEventsHeaders.EXT_LOGICAL_CLOCK`,
  `CE_LOGICAL_CLOCK`, `EXT_CAUSATION_ID`, `CE_CAUSATION_ID` and `TandemHeaders.CAUSATION_ID` removed.
- **`tandem-jdbc`**: `FullJitterBackoff`, `JdbcBucketCountStore`, `BucketLeaseManager` and
  `PgNotifyWakeup` no longer public; `BucketCountGuard`'s constructor and instance `check` no longer
  public; the three- and four-argument `JdbcOutboxRepository` constructors and the three longer
  `WorkerPool` constructors removed.
- **`tandem-kafka`**: `ErrorClassifier`, `DefaultErrorClassifier` and `CloudEventEncoder` no longer
  public; the two `KafkaRelay` constructors other than `(producerConfig, router, cfg)` removed.
- **`tandem-spring-producer`**: `JacksonPayloadSerializer`, `MicrometerTracePropagator`,
  `MdcCorrelationTracePropagator`, `TandemOutboxProperties` and `TandemTracingProperties` no longer
  public.
- **`tandem-spring-relay`**: `MicrometerTandemSpanRecorder`, `TandemOutboxProperties`,
  `TandemTracingProperties`, `TandemKafkaProperties`, `TandemRelayProperties` and
  `TandemMetricsProperties` no longer public.
- **`tandem-admin`**: every public type of `com.codingful.tandem.admin` other than
  `TandemAdminAutoConfiguration`, and of its `outbox` and `relay` packages with their `dto`, removed:
  they moved under `com.codingful.tandem.admin.internal`, which the gate excludes.

The additions (`ReplayCriteria.builder()`, the three builders, `KafkaMessageEncoder.cloudEvents`) are
compatible and fail nothing. `Automatic-Module-Name` is in the manifest, which japicmp does not read.

Any other report means the §6 table or a gate is wrong, and the two are corrected together before
the tag. Once `1.0.0-rc.1` is on Maven Central, `tandemBaseline` becomes `1.0.0-rc.1`.

---

## 11. Changes outside the build

These land with the gates:

- **AGENTS.md, Testing §4**: the reference-file exception of §8.1.
- **AGENTS.md, Releases**: the library's breaking-change check becomes the gates. Before tagging,
  `check` is green with the previous release as baseline. The **Breaking** section of the notes is
  the entries of `gradle/accepted-breaks.txt`, plus every line changed or removed in a reference file
  since the baseline tag (`git diff v<baseline> -- '*/src/test/resources/reference/*'`), plus every
  released interface whose `<b>SPI:</b>` paragraph was removed since then (§4.3), plus any change to the
  names review guards (HLD-api-stability §2). After the release reaches Maven Central,
  one commit moves `tandemBaseline` to it and empties the declared breaks, setting their `baseline`
  line to the new value.
- **AGENTS.md, Adding a module**: a published library module gets `checkJavaApi` and
  `checkInterfaceKinds` from the root convention, and its baseline gates skip it as new until the
  baseline contains it (§2.4); a new Spring module also joins `springMetadataModules`, which gives it
  `checkSpringMetadata` (§7), and needs an effective-defaults reference test.
- **HLD-api-stability.md**: the "Exists" column of §5 and the status line.
- **LLD-jdbc.md §6** and **LLD-spring-config.md §2.4**: a pointer to §5 and §7 of this note.
- **`.github/workflows/ci.yml`**: the comment of the CLI's client drift step, which still names a
  Specmatic check for `tandem-admin`.
