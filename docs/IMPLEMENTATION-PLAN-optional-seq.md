# Tandem — Implementation Plan: Optional `seq`

**Version:** 2.1
**Status:** Implemented — all five phases done. A brownfield adoption guide, when one lands, needs the
same treatment; tracked in §5.
**Scope:** make `seq` one of three explicit per-message modes, and generalise the write-side ordering
detector to key on whichever ordering the row declares. Spans `tandem-core`, `tandem-jdbc`,
`tandem-kafka`, `tandem-test`, `tandem-spring-producer`, `tandem-admin`, `tandem-micrometer` and the
PostgreSQL schema.
**Design:** [HLD-managed-seq.md](HLD-managed-seq.md) §4.5 (the third mode), §4.6 (choosing one),
§6.1 (which ordering key the detector holds).

This document orders the work, fences the scope, and defines done-ness per phase. The *design* is
fixed in the HLD; nothing here restates it.

**Standing assumption:** Tandem is in `0.x` with no production adopters, so this plan treats breaking
changes as free. Contracts are changed in place rather than versioned alongside their predecessors,
and names are corrected rather than preserved. The one constraint that survives is *loudness*: a
change must fail visibly, never silently alter what a caller already expresses.

---

## 1. Phase 1 — Schema · **done**

Appended as `v4-optional-seq.sql`, leaving every shipped changeset byte-identical. `v0.7.0` published
`v1`–`v3`, so their checksums are recorded in whatever `DATABASECHANGELOG` an adopter already has —
including `v3`, where `stripComments:false` puts the prose itself inside the checksum, so even a
comment-only edit there would break an existing database.

- `ALTER COLUMN seq DROP NOT NULL` — a message may now carry no number at all.
- `seq_source SMALLINT` added, backfilled, then `SET NOT NULL` — three steps because the column must
  end with no `DEFAULT`, so an INSERT that forgets it cannot silently claim `APPLICATION`. The
  discriminator of HLD-managed-seq §6.1, with its value enumeration and rationale in the changeset
  comment, in the same idiom as the existing `status`.
- `CONSTRAINT tandem_outbox_seq_source_agrees CHECK ((seq IS NULL) = (seq_source = 2))`.
- `UNIQUE (aggregate_id, seq)` untouched — it simply becomes inert for a row with no `seq`, which the
  changeset records.

**Existing rows are backfilled as `APPLICATION`.** Their real provenance is unrecoverable — the
reason `seq_source` exists at all — and `APPLICATION` is the likelier one, since `managedSeq()` first
shipped in `v0.7.0`. The cost of guessing wrong is bounded: a genuinely managed row is judged on its
number rather than on `id`, and because `id` and `tandem_seq` are separate sequences that concurrent
inserts can interleave, such a row can raise an ordering violation that never happened. It reaches
only rows still awaiting delivery, so it drains with the backlog.

**Done-ness:** `./gradlew generateBaselineSql` propagates into the flat baseline and
`:tandem-jdbc:checkBaselineSql` is green. ✅

> **The schema is not inert.** `seq_source` is `NOT NULL` with no `DEFAULT`, deliberately — a default
> would let an INSERT that forgets the column silently claim `APPLICATION`. Every insert therefore
> fails (`null value in column "seq_source"`) until Phase 2 binds it, and `:tandem-jdbc:integrationTest`
> is red in the meantime. `NOT NULL DEFAULT 0` is the fallback if that window becomes unacceptable.

## 2. Phase 2 — Write side

| File | Change |
|---|---|
| `tandem-core/.../SeqSource.java` *(new)* | `enum SeqSource { APPLICATION(0), MANAGED(1), NONE(2), UNKNOWN(-1) }` with an explicit `code` field and `fromCode(int)` — **matching `OutboxStatus`, not `ordinal()`**, so reordering the enum cannot silently change what is persisted. Unlike `OutboxStatus.fromCode`, it **must not throw** on an unrecognised code: it returns `UNKNOWN`, which the detector keys on `id` (HLD-managed-seq §4.5, §6.1). `UNKNOWN` is read-only and has `code = -1` so it can never round-trip to the column. Public — it appears on `OutboxRecord`. |
| `tandem-core/.../OutboxMessage.java` | Add `unsequenced()`. `seqSource()` replaces the `managedSeq()` accessor; the three builder calls stay as the fluent surface. Three-way exclusivity plus fail-fast on none, the message naming all three modes and what each is for (HLD-managed-seq §4.6). `seq()` throws unless `APPLICATION`; add `hasSeq()`. `toString` renders `seq=none` for `NONE`. |
| `tandem-core/.../OutboxRecord.java` | Add `seqSource()` and `hasSeq()`; `seq()` throws when absent. The guard at line 35 changes meaning: keep rejecting a message still *pending* a managed number, accept a genuinely unsequenced one. |
| `tandem-jdbc/.../JdbcOutboxRepository.java` | `bind()` at line 222: bind `seq` or `setNull(index++, Types.BIGINT)`, then always bind `seq_source`. Both INSERT statements gain the column; the batching key becomes `seqSource() == MANAGED`, the only mode that omits `seq`. |
| `tandem-jdbc/.../OutboxRowMapper.java` | Add `seq_source` to `COLUMNS` (it flows through the claim's `RETURNING`); read it via `SeqSource.fromCode`, and read `seq` with `wasNull()` so an absent value is absent rather than `0`. |
| `tandem-test/.../InMemoryOutbox.java` | Mirror all three modes: keep the managed counter (line 183), exclude `NONE` rows from the `(aggregate_id, seq)` uniqueness map (line 147), carry `seqSource` onto the stored record. |
| `tandem-spring-producer/.../OutboxCollector.java` | Re-point the no-`seq` overloads (lines 58, 67) from *managed* to *unsequenced*; managed becomes reachable only via `add(OutboxMessage)`, as `lockedWrite()` already is. Keeps the tier at two shapes and points the shorter one at §4.6's answer for the undecided. |

Two consequences found while building it, neither visible from the file list above:

- **`OutboxRecord` needs `seq_source` as its own field, not derived from the message it wraps.** A
  mapper reading a `MANAGED` row supplies the resolved number, so the rebuilt message reports
  `APPLICATION` — the provenance survives only in the column. `OutboxRecord.seqSource()` is therefore
  the authority for a persisted row and `record.message().seqSource()` is not, which is documented on
  both accessors. The builder defaults it from the message (right for a record never persisted) and
  the mapper sets it explicitly.
- **Raw-SQL inserts in tests must bind `seq_source`.** `ManagedSeqSchemaIT` and `BucketLeaseManagerIT`
  write rows directly; both failed with `null value in column "seq_source"` until updated. That is
  the intended loud failure of a `NOT NULL` with no default, working as designed.

**Done-ness:** `./gradlew check` green.

> **The Admin API does not break here — it silently lies, which is worse.** This plan predicted
> `tandem-admin` would throw on an unsequenced row until Phase 3. It does not:
> `JdbcOutboxQuery:151` reads `rs.getLong("seq")` with no `wasNull()` and `OutboxRowView` declares a
> primitive `long seq`, so such a row surfaces through the Admin API as **`seq = 0`** — precisely the
> "plausible zero looking authoritative" this design refuses everywhere else. `check` stays green
> only because no test yet creates an unsequenced row and reads it back through the query side.
>
> A latent wrong answer with no failing test is worse than a loud break, and it means **Phase 3 is
> not optional polish after Phase 2 — the two must land together.** Phase 3 gains the missing
> `wasNull()` handling on the read side, and a test that would have caught this.

## 3. Phase 3 — Wire and Admin API · **done**

| File | Change |
|---|---|
| `tandem-kafka/.../CloudEventEncoder.java:50` | Emit `ce_seq` only when `record.hasSeq()`; the "always present → ce_seq" comment goes. |
| `docs/admin-api.openapi.yaml:603` | Remove `seq` from `OutboxEntry.required`; mark it `nullable: true`. Edited in `/v1` in place — no `/v2`. |
| `tandem-admin/.../OutboxEntryResponse.java` | `seq` becomes a nullable `Long`. Must land with the OpenAPI edit, not before it: the conformance test validates the implementation against the committed contract, so a nullable field under a `required` spec fails, and vice versa. |
| `tandem-jdbc/.../JdbcOutboxQuery.java:151` | `rs.getLong("seq")` gains `wasNull()` handling — without it an unsequenced row reads back as `0` (see the note in §2). `VIEW_COLUMNS` needs no `seq_source`: the Admin API does not expose it. |
| `tandem-core/.../OutboxRowView.java` · `OutboxRowDetail.java` | `seq` becomes a nullable `Long` on the read model. |
| `tandem-test/.../InMemoryOutbox.java` | Its `OutboxQuery` projection must mirror the same nullability, or the in-memory and JDBC read sides disagree. |

`seq_source` is **not** exposed: it is detector metadata, and the Admin API's job is operational
state, not internal mechanism. Reconsider only if an operator investigating an `order_violation`
alert needs it to interpret what they see — an additive optional field, cheap later, expensive to
remove.

Specmatic's backward-compatibility check will fail on the OpenAPI edit. That failure is expected and
is the signal the contract changed; it is waived rather than designed around.

**Tests added**, each covering a place a wrong answer was reachable and untested: an unsequenced
record encodes with **no** `ce_seq` and still carries `ce_id`, which is what consumers deduplicate
on; such a row reads back through `search` and `findById` with a `null` seq rather than `0`, on both
the JDBC and in-memory query sides; several unsequenced rows of one aggregate coexist, proving
`UNIQUE (aggregate_id, seq)` is genuinely inert for them; and all three modes inserted in one batch
each record the `seq_source` they used.

**Done-ness:** `./gradlew check` green — the whole build, for the first time since Phase 1.

> Two infrastructure failures appeared during verification and neither was a defect: the benchmark's
> `s3HotPartitionSmoke` timed out on its 6-minute drain inside a 16-minute full run but passes in
> 1m54s alone, and a Kafka Testcontainer failed to launch once (`ContainerLaunchException`) after
> repeated container churn, passing on retry. Worth knowing before either is mistaken for a
> regression.

## 4. Phase 4 — Detector · **done**

| File | Change |
|---|---|
| `tandem-jdbc/.../SeqWatermarks.java` | Rename to `PublishOrderWatermarks`. Map value becomes a `(Key, long)` pair, `Key ∈ {SEQ, ID}`. Verdicts and the LRU bound unchanged; class javadoc rewritten. |
| — | Entry point becomes `Verdict record(OutboxRecord)`, resolving `seqSource() == APPLICATION ? SEQ : ID` internally — one home for the policy, rather than spreading it into `RelayWorker`. A key change resets the entry. |
| — | Add `lastPublished(AggregateId)` returning the stored `(Key, long)`, for the report to name what the row was judged against. **Not** a widened return type on `record(...)`: `flushDone` calls that per row, and allocating a result object per row would put hot-path cost on information only the rare path needs. It works because `REGRESSED` deliberately leaves the watermark untouched, so reading it afterwards returns exactly the value compared against. |
| `tandem-jdbc/.../RelayWorker.java:199` | `watermarks.record(record.aggregateId(), record.seq())` → `watermarks.record(record)` |
| `tandem-jdbc/.../RelayWorker.java:219` | `store.replaysOf(record.id())` — already keyed on `id`, unchanged |
| `tandem-jdbc/.../RelayWorker.java:222` | Split the `ERROR` into two fixed messages, one per key (below). |

### 4.1 The report says more than it knows — split it

The shipped message asserts a cause the evidence does not always support, **today, not only after
this change**: every current row is `seq`-keyed, so

```
"Published an aggregate's events out of seq order, so writers to it are not serialised (HLD 4.2)"
```

claims unserialised writers whenever the observation also admits an application numbering its events
inconsistently with its own insert order. HLD-managed-seq §6.1 works through why the entitlement
differs by key. Two fixed messages, chosen on the key the watermark held:

| Key | Message |
|---|---|
| `ID` | `Published an aggregate's events out of insert order, so writers to it are not serialised (HLD 4.2)` |
| `SEQ` | `Published an aggregate's events in an order that disagrees with the order the application declared` |

Two constants rather than one interpolated string, so both stay greppable and aggregatable. **One
counter for both** — the metric is the alert, the message is the diagnosis; splitting the counter
would fragment alert rules for no operational gain.

Both gain the comparison that produced the verdict, in the existing `name:value` tail:
`key:seq, observed:7, lastPublished:41` — absent today, and the first thing anyone reading the alert
wants.

### 4.2 Renames

| Now | Becomes |
|---|---|
| `tandem.outbox.seq_regression.count` | `tandem.outbox.order_violation.count` |
| `TandemMetrics.incrementSeqRegression()` | `incrementOrderViolation()` |
| `RelayConfig.seqRegressionDetection` / `Builder.seqRegressionDetection(boolean)` | `orderViolationDetection` |
| `tandem.relay.seq-regression-detection` (Spring property) | `tandem.relay.order-violation-detection` |
| `TandemRelayProperties.seqRegressionDetection` | `orderViolationDetection` |
| `RecordingMetrics.seqRegressions()` | `orderViolations()` |
| `RelayWorkerSeqRegressionTest` | `RelayWorkerOrderViolationTest` |

"Order violation" is the term the HLD already uses for the violated write-side precondition, so the
metric, the config key and the prose finally agree.

### 4.3 Tests

- `SeqWatermarksTest` → `PublishOrderWatermarksTest`. Existing verdict and LRU cases carry over on the
  `SEQ` key; new cases for the `ID` key, for a key change resetting the entry, and for the two keys
  never being compared with each other.
- `RelayWorkerOrderViolationTest` — scenarios unchanged for `APPLICATION` rows, plus a `NONE`
  aggregate publishing out of `id` order and asserting the violation is reported.
- **`CommitOrderReorderIT` must pass unmodified.** Its rows are app-assigned, so they keep the `SEQ`
  key and the behaviour under test is unchanged. If it needs editing to stay green, the premise of
  this phase is wrong and the plan stops here.
- **New IT:** an unsequenced aggregate driven through the commit-order reorder (HLD-managed-seq §3.1),
  asserting detection still fires — the case that proves the `ID` key earns its place.
- **New:** an aggregate mixing `APPLICATION` and `MANAGED` rows, published in order, asserting
  **zero** violations — the false positive the discriminator removes.
- **New:** a row whose `seq_source` holds a code this build does not know, asserting it is read as
  `UNKNOWN`, keyed on `id`, and never throws — the forward-compatibility contract of
  HLD-managed-seq §4.5, and the one case a future schema version will actually exercise.
- **New:** the two reports of §4.1 — an `ID`-keyed violation asserting the serialisation message, a
  `SEQ`-keyed one asserting the weaker disagreement message, both asserting the `lastPublished` value
  reaches the tail. `RecordingMetrics` counts one violation in each case, since the split is in the
  log and not in the metric.

**Done-ness:** `./gradlew check` green, with `CommitOrderReorderIT`'s existing tests **unmodified** —
the signal this phase was built to produce.

Two things the file list above did not anticipate:

- **An `ID`-keyed violation cannot be staged against `InMemoryOutbox`.** Publication order there *is*
  id order, so the divergence has nowhere to come from. The seq-keyed tests stage a reorder by giving
  a lower `seq` a higher `id`; the id-keyed one has no such lever and belongs in
  `CommitOrderReorderIT`, where two real uncommitted transactions produce the race. The in-memory test
  covers the quiet case and says why.
- **`violationReport` was extracted as a package-private pure function.** Asserting the two messages
  otherwise needs a `System.LoggerFinder` harness, which the repo has no precedent for and which is a
  lot of machinery to pin two strings. Extracting the function is the seam AGENTS.md names first, and
  it keeps the claim each key licenses under test rather than only under review.

## 5. Phase 5 — Documentation · **done**

The design already lives in [HLD-managed-seq.md](HLD-managed-seq.md) (§4.5, §4.6, §6.1) and did not
need re-deriving. What this phase propagated to the documents that cited the old behaviour:

| Document | What changes |
|---|---|
| [HLD.md](HLD.md) | §3.2 — `SINGLE` with several instances now also costs degraded ordering detection, the one consequence with no signal of its own (HLD-managed-seq §6). §4.2 — drop the "event-sourcing stream revision" framing that pushes adopters toward `@Version`; state that supplying `seq` buys stronger detection, which is the honest reason to. §5.1 — schema, `seq_source`, the `CHECK`. §7/§8 — the metric under its new name, what each key can and cannot see, and the full list of when a watermark is lost (the existing note names only restart and eviction). |
| [LLD-jdbc.md](LLD-jdbc.md) | §3.9 detector, §6 schema |
| [LLD-core.md](LLD-core.md) · [LLD-test.md](LLD-test.md) · [LLD-spring-producer.md](LLD-spring-producer.md) · [LLD-kafka.md](LLD-kafka.md) · [LLD-micrometer.md](LLD-micrometer.md) | `SeqSource`, `OutboxMessage`/`OutboxRecord` contract, `InMemoryOutbox` parity, collector overloads, encoder, metric name |
| [LLD-spring-config.md](LLD-spring-config.md) | the renamed relay property |
| [HLD-cloudevents.md](HLD-cloudevents.md) | `ce_seq` is now conditional |
| `README.md` | the quickstart taught `.seq(order.version())` as *the* way; it now names the choice, and a known-issues entry states the three modes and the answer when undecided |

**One finding worth keeping.** The Grafana dashboard queried
`tandem_outbox_seq_regression_count_total`, which would have rendered an empty panel against a relay
emitting the renamed metric — stale in a way no test could catch. A published dashboard is a consumer
of the metric contract; it belongs on the rename checklist beside the code.

**Still outstanding: any brownfield adoption guide.** Such a document describes the write side to
someone meeting it for the first time, so it must state that a `seq` mode is chosen rather than
inherited — the mode table and the answer when undecided, rather than a single worked example.

## 6. Verification gate

- `./gradlew check` — `integrationTest`, the Spring modules' `bootFourTest`, and the coverage gate.
- **`CommitOrderReorderIT` green without modification** — the single most important signal here (§4.3).
- `./gradlew generateBaselineSql` plus the drift gate — green as of Phase 1.
- JaCoCo on the changed detector and builder paths — read which branches are uncovered, per
  [AGENTS.md](../AGENTS.md).
- `OutboxMessageTest` / `OutboxRecordTest` keep their `toString` sensitivity assertions.

**Phases do not stand alone as commits.** Phase 1 leaves every insert failing until Phase 2 binds
`seq_source`; Phase 2 leaves `tandem-admin` throwing on unsequenced rows until Phase 3; and Phase 5
documents semantics the earlier phases introduce. Phases 1–3 are the smallest group that lands on a
green `check`, and Phase 4 can follow separately on top of it.

## 7. Release notes

A single breaking release. `v0.8.0`: a breaking change in `0.x` is signalled by the minor bump
([AGENTS.md](../AGENTS.md) — Releases). The body below is the annotated tag's message verbatim — the
release workflow reads `%(contents:body)`, so it is never written by hand on the release page.

The body opens on the upgrade rather than on the feature, because two of these cost a reader real
time to discover: the schema cannot be migrated from `v0.7.0` in place, and three changes fail
*silently* rather than at compile time — the Spring property (an unknown key is ignored, so an
explicit `false` becomes `true`), `TandemMetrics.incrementSeqRegression()` (a `default` method, so an
existing override simply stops being called), and the `OutboxCollector` overload (same signature,
different wire contract). Those three also lead the Breaking section.

```
tandem v0.8.0 — seq becomes a choice: three explicit per-message modes

Upgrading from v0.7.0 is not a drop-in — read Breaking before you take it. Two
things make it awkward. The schema needs its v4 migration applied before a
v0.8.0 relay starts. And three of the changes are silent: they neither fail to
compile nor throw, they quietly alter what a caller already expresses.

New
- Three per-message seq modes, one required and none default: seq(long) supplies
  the aggregate's own version, managedSeq() draws Tandem's, unsequenced() stores
  no number at all. unsequenced() asks nothing of the domain and is the answer
  when undecided; seq(long) is the only mode that buys the stronger ordering
  detection.
- The choice costs nothing on the write path: the three modes land within 0.8%
  of each other, and managedSeq()'s shared sequence runs at 2.6% of its own
  ceiling at the highest row rate measured. HLD-managed-seq 3.4 has the numbers.
- The ordering-violation detector now judges each row on the ordering it
  declares — an app-assigned number where there is one, insert order otherwise —
  instead of always reading seq.
- SeqSource and the seq_source column make the three modes tellable apart on a
  persisted row.

Breaking
- tandem-spring-producer: OutboxCollector.record(aggregateType, aggregateId,
  payload) keeps its signature but now records unsequenced() where it recorded
  managedSeq(). Events published through it carry no ce_seq. A consumer that
  reads one must move the call to add(OutboxMessage) with managedSeq().
- tandem-spring-relay: tandem.relay.seq-regression-detection ->
  tandem.relay.order-violation-detection, and TandemRelayProperties's canonical
  constructor with it. An unknown property is ignored, so an explicit false
  silently becomes true on upgrade.
- tandem-core: TandemMetrics.incrementSeqRegression() ->
  incrementOrderViolation(). It is a default method, so an existing
  implementation still compiles and its override simply stops being called.
- tandem-micrometer: the meter tandem.outbox.seq_regression.count ->
  tandem.outbox.order_violation.count, and the adapter method that feeds it ->
  incrementOrderViolation(). Dashboards and alerts on the old meter name go
  quiet rather than error.
- tandem-jdbc: RelayConfig.seqRegressionDetection() and its builder setter ->
  orderViolationDetection().
- tandem-test: RecordingMetrics.incrementSeqRegression()/seqRegressions() ->
  incrementOrderViolation()/orderViolations().
- tandem-core: OutboxMessage.seq() and OutboxRecord.seq() throw unless the
  message was built with seq(long) — guard with hasSeq(). The managedSeq()
  accessor is replaced by seqSource(). OutboxRowView and OutboxRowDetail retype
  seq from long to Long, changing their canonical constructors.
- tandem-admin: OutboxEntryResponse retypes seq from long to Long.
- Kafka wire contract: ce_seq is omitted for an unsequenced message rather than
  always present. Consumers must tolerate its absence and deduplicate on ce_id.
- Admin API: seq is no longer required in OutboxEntry. Absent is not zero — a
  client must not substitute one.
- DB schema: seq becomes nullable and seq_source SMALLINT NOT NULL is added, in
  a new v4 changeset. Apply it before a v0.8.0 relay starts, since every insert
  now requires the column. Existing rows are backfilled as APPLICATION — the
  likelier provenance, but not a recoverable one: a row that actually took its
  number from managedSeq() is then judged on that number rather than on id,
  which can report an ordering violation that never happened. Only rows still
  awaiting delivery are affected, so it drains with the backlog.
```
