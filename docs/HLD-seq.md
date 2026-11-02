# Tandem: `seq` and write-side ordering (Design Note)

**Version:** 2.0  
**Status:** Built. `seq` is optional (§4.1) and Tandem can serialise the writers of one aggregate
(§4.2); the two are independent and chosen per message. Tandem never assigns `seq` itself (§4.5).  
**Companion to:** [HLD.md](HLD.md) §4.2 (Ordering Established at Write Time)

**This document is the design.** It records what an app-assigned `seq` costs an existing
application, the measurements behind that claim, and the two mechanisms built on top: a message may
carry no number at all (§4.1), and Tandem may serialise concurrent writers to one aggregate (§4.2).
§4.4 is how a caller chooses, §4.5 why Tandem does not number events itself, and §6 what the relay's
ordering detector can see.

---

## 1. The problem: what `seq` costs an existing application

Adopting Tandem in an application that already has aggregates carries two costs when every event must
carry an app-assigned `seq` (HLD §4.2):

1. **The aggregate must number its events.** An aggregate with no version field, or whose version
   does not advance once per event, has no `seq` source, so adoption reaches into the *domain*
   schema rather than only adding the `tandem_*` tables.
2. **Writes to one aggregate must be serialised.** HLD §4.2 states this as a hard precondition:
   within an aggregate, commit order must equal `seq` order must equal `id` order. An existing
   application that writes an aggregate from several code paths (a service, a batch job, an admin
   action) frequently does not satisfy it, and nothing in the application fails when it does not.

The second cost is the severe one, because its failure mode is silent. §3.1 measures it.

---

## 2. What `seq` actually does

`seq` is **not** the relay's ordering key. The claim (`JdbcOutboxStore`, LLD-jdbc §3.3) selects
`ORDER BY o.id` and gates on a head-of-chain `NOT EXISTS` over earlier rows of the same aggregate;
`seq` appears in no dispatch query. It has three jobs:

1. **`UNIQUE (aggregate_id, seq)`**: the safety net against a write-side bug, as HLD §4.2 says.
2. **The `seq` CloudEvents extension** (`ce_seq`), for consumers that want the aggregate's own
   number for the event alongside it.
3. **The write-side ordering detector's watermark**: the relay compares the `seq` order it publishes
   per aggregate against the order the application declared, and reports a backwards step (§6).

Deduplication is not among them: consumers already receive `ce_id` (the outbox `id`), unique by
construction and sufficient for it.

**Job 3 is the substantial one, and the least obvious.** A `seq` the application assigned is the only
record of the order the application intended, independent of the order rows were physically
inserted. Nothing else in the row carries it, so nothing else can check it (§6.1).

**Job 1 is narrower than "ordering defects".** `UNIQUE (aggregate_id, seq)` fires only on a
**duplicate** `seq`, and §3.1 measures that the reorder hazard produces two *different* values, so
the constraint never sees it. Nor does it check that the numbers increase. The two guards cover
disjoint failures:

| Failure | `UNIQUE (aggregate_id, seq)` | Detector (§6) |
|---|---|---|
| Two events given the **same** `seq` (a stale `@Version`, §3.2) | catches it: the business transaction aborts | does not see it: the rows publish in `id` order |
| Concurrent writers, **distinct** `seq`, commits inverted (§3.1) | **never fires** | catches it, subject to §6's limits |

The constraint protects only the rows still in the table: cleanup (HLD §4.7) deletes delivered rows,
so it is not a permanent deduplication of business operations either.

**`seq` is a number per event, not per state.** It increases within the aggregate with each event,
so it equals the aggregate's version only while every version produces exactly one event. A state
change that emits two events has one version and needs two numbers; taking both from the version
makes the second insert fail on the constraint, however correctly the ORM flushed.

---

## 3. Measurements

Every result here was established by experiment against a real PostgreSQL 16 and a real Spring Data
JPA / Hibernate domain.

### 3.1 Commit order, not `seq`, decides publication order

Two concurrent transactions write the same aggregate. `seq = 1` is inserted first and so receives the
lower `id`; `seq = 2` is inserted second and commits first.

- **The claim** (the relay's own poll query, which selects and locks the next batch of rows to
  publish: §2 above; `JdbcOutboxStore`, LLD-jdbc §3.3), **issued while the first transaction is still
  open**, returns **only the `seq = 2` row**. Not because the query is wrong: `seq = 1`'s transaction
  has not committed yet, and an uncommitted row is invisible to any other session's snapshot under
  ordinary PostgreSQL transaction isolation (MVCC). The relay is not special-cased here; it sees
  exactly what any other query would see at that instant. `seq = 2` is therefore published first. The
  `seq = 1` row only becomes visible, and only then claimable, once its own transaction commits.
  Publication order is **`[2, 1]`**.
- Neither guard fires. `UNIQUE (aggregate_id, seq)` sees two *different* `seq` values, so it has
  nothing to reject; the head-of-chain `NOT EXISTS` cannot see the earlier row at all, for the same
  MVCC reason.
- **The result is identical at `bucketCount` 1 and 256.** A single bucket serialises the *relay*, not
  the *writes*, and the defect is upstream of the relay. This is structural rather than incidental:
  one aggregate always hashes to one bucket (HLD §4.3), so `B` never affects per-aggregate ordering.
- Control: the same two writes committed in insert order are published `[1, 2]`. The cause is the
  commit order, not the concurrency.

This is pinned by `CommitOrderReorderIT` (`tandem-jdbc`), which runs both the reordering case and the
control at `bucketCount` 1 and 256, with and without a sequence number.

**Consequence for §1's second cost:** an application that does not serialise its writers gets
reordered events with no error and no failed insert: nothing in the data, and nothing the caller can
observe. The relay does report it as it publishes; see §6 for what that signal covers and what it
does not.

### 3.2 A JPA `@Version` is stale on every write-side tier

Hibernate increments `@Version` at flush. Every Tandem write-side tier runs *before* the flush,
inside the caller's transaction, which is the whole point of the outbox, so every tier observes the
pre-increment value. Measured across all three Spring tiers (the application-events tier with
`AbstractAggregateRoot` + `OutboxEventMapper`; the annotation tier with `@TransactionalOutbox` +
`TandemAggregate`; and the Template tier with `TransactionalOutboxTemplate`), the results were
**identical in all three**:

| Scenario | Observed | Outcome |
|---|---|---|
| One state-changing mutation per transaction | tier sees version `N`, flush persists `N+1` | works, but `seq` is always the persisted version minus one |
| Two mutations in one transaction | both mutations see the same pre-flush version | `DuplicateSeqException`: **the business transaction aborts** |
| A mutation that changes no persistent state | Hibernate issues no `UPDATE`, the version does not advance | the next transaction reuses the `seq`, so `DuplicateSeqException` |

The plain `OutboxRepository.insert()` tier is not exempt: it fails the same way whenever the caller
passes `entity.getVersion()`.

**The tier is irrelevant.** Choosing a different tier changes how much of the domain Tandem touches;
it does not change where `seq` comes from.

The third row is the dangerous one. It is deterministic rather than racy, but it depends on the
*data* (whether a given mutation happens to dirty a persistent field), so it survives a test suite
built on state-changing fixtures and surfaces in production.

### 3.3 Whether the aggregate's own lock serialises writers at all depends on flush order

§3.1 shows the reorder with no domain table in the picture. A real JPA application has one: an
`UPDATE` on the aggregate row, guarded by whatever lock the domain uses. It is natural to expect that
row lock to already prevent the reorder. It does not, by default, and the reason is the same flush
timing as §3.2: Hibernate defers the `UPDATE` to flush, and every write-side tier runs *before*
flush, so **the outbox row is inserted before the lock is ever taken**.

Two transactions, `T1` first to insert (lower `id`), `T2` first to commit: the same shape as §3.1,
now with a domain mutation ahead of the outbox insert.

```
-- No explicit flush: the domain UPDATE and the lock both happen at commit, after the insert.
T1: begin; UPDATE order SET status=? WHERE id=?   -- deferred by the ORM to flush/commit, not yet issued
T1: INSERT INTO tandem_outbox (seq=1, ...)         -- issued immediately, no lock held
                                                     -- (T1 left open)
T2: begin; UPDATE order SET status=? WHERE id=?    -- no lock from T1 exists yet, proceeds
T2: INSERT INTO tandem_outbox (seq=2, ...)
T2: commit                                          -- T2's UPDATE flushes and commits first
T1: commit                                          -- T1's UPDATE flushes now, too late
                                                     -- published: [2, 1], same reorder as §3.1

-- With an explicit flush before the outbox insert: the UPDATE, and its lock, come first.
T1: begin; UPDATE order SET status=? WHERE id=?; flush   -- lock taken now, held to commit
T1: INSERT INTO tandem_outbox (seq=1, ...)
                                                     -- (T1 left open, holding the row lock)
T2: begin; UPDATE order SET status=? WHERE id=?; flush   -- BLOCKS on T1's row lock
T1: commit                                          -- releases the lock
T2: (unblocks) INSERT INTO tandem_outbox (seq=2, ...); commit
                                                     -- published: [1, 2], in order
```

Measured against a real Hibernate domain, three shapes of writer:

| Writer touches | No explicit flush | Explicit flush before the outbox insert |
|---|---|---|
| The aggregate row itself, with `@Version` | reorders (`T1`'s commit fails its optimistic check instead: a lost event, not a reorder) | serialises: `T2` blocks on `T1`'s row lock |
| The aggregate row itself, no version | reorders | serialises |
| Only a *child* of the aggregate (e.g. an order line), never the root | reorders | **still reorders: no shared row to lock** |

**The explicit flush recommended in §3.2 for the stale `seq` is therefore doing two jobs, not one**: it
is also what makes the aggregate's write lock real. Skipping it does not make `@Version` merely
ineffective at protecting order; with a version, the *earlier* writer's commit fails outright,
discarding that transaction's outbox row with it.

**The flush does not help when there is no shared row.** Writers that only insert children of the
aggregate reorder regardless, because there is nothing for either transaction to lock. That case needs
its own serialisation: an explicit `SELECT … FOR UPDATE` on the aggregate row, an aggregate boundary
drawn so concurrent writers meet on one row, or Tandem's lock (§4.2).

**Not a Tandem defect, and not something a CDC-based outbox avoids either.** The Hibernate
flush-timing fact above is general and holds for any JPA application, with or without an outbox. It is
tempting to think a log-tailing design (Debezium reading the WAL, [comparison.md](comparison.md) §2)
sidesteps §3.1's reorder, since it reads the database's own commit order instead of reconstructing one
from `id`. It does not: commit order **is** what produces the reorder in the first place. Logical
decoding emits a transaction only once its `COMMIT` record is read, in the order those `COMMIT`
records appear in the WAL, so if `T2` (seq 2) commits before `T1` (seq 1), a WAL tailer emits `T2`'s
change first for exactly the same reason the claim does: that is, factually, what happened first. No
downstream reader, polling or log-tailing, can recover an order the write side never established,
because once two writers race with no lock between them there is no single "correct" order left to
recover; whichever commits first *is* first, for anyone watching.

The precondition in HLD §4.2 is therefore not a gap particular to this library, or to the poll versus
CDC choice: **any** consumer of committed database state depends on the write side actually
serialising per-aggregate writers. What CDC changes is unrelated to this hazard: it removes Tandem's
*own* approximation of order (`id` plus polling visibility, §2 above) in favour of reading the
database's serialization history directly, a real strength ([comparison.md](comparison.md) §2) against
Tandem's polling mechanics, not against an unserialised write side.

**And that approximation has a sharper consequence than "the relay is a step behind": whether the
same violation is even *visible* in the published order depends on poll timing, not on whether it
happened.** The head-of-chain gate (`NOT EXISTS ... e.id < o.id AND e.status IN (0,1,3)`) enforces
strict ascending `id` per aggregate **whenever a claim sees both rows already committed together**: a
later `id` is held back until the earlier one is `DONE`, regardless of which one actually committed
first. The reorder in §3.1 is only visible because the claim happened to run **inside** the narrow
window between the two commits, catching only the later row. The identical write-side race produces
no visible reorder at all if the next claim instead runs **after both have committed**:

| | `T2` commits, then `T1` commits (identical in both) | Relay poll | Published |
|---|---|---|---|
| Timeline A (measured, §3.1) | `T2` at 8ms, `T1` at 40ms | runs at 10ms, sees only `T2` | `[seq 2, seq 1]`: reorder visible, counter +1 |
| Timeline B (same race) | `T2` at 8ms, `T1` at 15ms | next poll runs at 100ms, sees both already committed | `[seq 1, seq 2]`: head-of-chain gate enforces order; **counter stays 0** |

The write-side defect is identical in both rows of the table. A WAL tailer would show `[2, 1]` in
**both** timelines: it processes each `COMMIT` record as it appears in the log, with no polling
cadence to land inside or outside of. Tandem's detector only fires when a claim's timing happens to
fall inside the race window, narrower than one `pollInterval` in practice, so a race that resolves
faster than that is invisible **by construction**, not by bad luck or a lost watermark. This is in
addition to, and structurally more common than, the watermark-eviction and relay-restart causes noted
in [HLD.md](HLD.md) §7: a zero reading is evidence of nothing.

---

## 4. The design: two independent mechanisms

§1 lists two costs. They are **separate problems** with separate mechanisms, separate audiences and
separate cost profiles, so each is opted into on its own:

| | Problem it closes | Mechanism | Hot-path cost | Persistent state |
|---|---|---|---|---|
| **4.1** | §1's first cost: `seq` is required of the client at all | a nullable column; `unsequenced()` | **none** | none |
| **4.2** | §1's second cost: concurrent writers to one aggregate are not serialised | `pg_advisory_xact_lock` in the caller's transaction; `lockedWrite()` | one statement | **none**: released at commit |

**§4.1 removes the obligation**: an aggregate with nothing to number its events with can still use
Tandem, with no change to its domain. **§4.2 changes how the application behaves**: concurrent writers
to one aggregate become waiters, a decision an adopter makes deliberately, and §4.3 explains why its
audience is narrower than it looks. Opting into one does not opt into the other. §4.4 is how to
choose a `seq` mode; §4.5 records why Tandem does not offer to number events itself.

### 4.1 `seq` is optional: `unsequenced()`

`OutboxMessage.Builder.unsequenced()` gives the row no sequence number. The write side binds `NULL`,
the row carries no number, and `ce_seq` is absent from the published event; consumers deduplicate on
`ce_id`, which is unique by construction and always present.

```sql
seq             BIGINT,                  -- nullable, no default: NULL means the row declares no ordering
```

Three consequences worth stating rather than discovering:

- **`UNIQUE (aggregate_id, seq)` goes inert for such a row**, since PostgreSQL treats NULLs as
  distinct, and loses nothing by it. That constraint catches only a *duplicate* `seq`, the
  stale-`@Version` family of §3.2: a failure that cannot arise when the application supplies no
  number. The guard is not bypassed, the case it guards is gone.
- **The column has no default.** An INSERT that leaves `seq` out stores `NULL`, a row with no number;
  nothing ever draws a number on the writer's behalf. A write-side SDK still binds the column in every
  INSERT, so the statement states the mode it means.
- **Whether a row has a number is the whole of what it records about its `seq`.** The detector (§6.1)
  needs nothing more: a number is one the application declared, and no number declares nothing.

`seq()` throws on an unsequenced message rather than returning `0`: a plausible zero would reach
`ce_seq`, a log line or a failure message looking authoritative. `hasSeq()` is the predicate to
branch on, on `OutboxMessage` and `OutboxRecord` alike.

**Two modes, mutually exclusive, and neither is the default:** `seq(long)` and `unsequenced()`.
Building a message that states neither fails. §4.4 is how to choose, and why there is no default to
fall back on.

### 4.2 The order: a transaction-scoped advisory lock

```sql
SELECT pg_advisory_xact_lock(hashtext(?));   -- ? = aggregate_id, before the outbox insert
```

Held for the remainder of the caller's transaction and released automatically at commit or rollback,
so a concurrent transaction writing the same aggregate blocks until the first commits. That makes
`commit order = id order` (and `= seq order`, when the number is taken after the lock) a property
**Tandem enforces**, rather than a precondition the application is asked to satisfy and given no way
to verify.

**The same guarantee as a counter row (§4.5), with no persistent state.** Both are a lock held to
commit; the difference is only where the lock lives: a table row that must exist forever, versus
shared memory that cleans itself up.

One property to state rather than discover: `hashtext` collisions make two unrelated aggregates
occasionally serialise, a throughput effect and never a correctness one; narrow it with the two-key
`pg_advisory_xact_lock(int4, int4)` form if it matters.

**Built as `OutboxMessage.Builder.lockedWrite()`**, a flag independent of the `seq` mode.
`JdbcOutboxRepository` takes the lock on the same connection right before the row's INSERT, in
`insert` and in `insertAll`; a batch touching several such aggregates takes every lock it needs, once
each, in **sorted aggregate id order**, before inserting anything: the deadlock guard of §5, enforced
rather than left to the caller for the one case Tandem controls end to end. A caller issuing separate
`insert` calls across aggregates within one transaction is responsible for that ordering itself;
nothing here can see across separate calls. Reachable on the annotation and application-events tiers
with no new API, since both already carry a caller-built `OutboxMessage` through unchanged; the
Template tier only via `add(OutboxMessage)`, not a `record(…)` overload, which would multiply that
method's surface for a mechanism most callers never need (Pareto, HLD §1.1). `InMemoryOutbox` needs
no lock of its own (every insert already serialises under one internal lock) but keeps the flag on
the stored message.

**A number computed before the lock is read too early.** The lock is taken inside the insert, so a
`seq` the caller computed beforehand, from state another writer may be about to change, is not
protected by it. A writer that relies on `lockedWrite()` alone either computes its number after
taking an equivalent lock of its own, or writes `unsequenced()`.

### 4.3 Why the two halves stay independent

**They close different costs for different people.** §4.1 removes an *adoption* barrier, and it
applies to every candidate adopter whose consumers do not need a number, whatever their concurrency
looks like. §4.2 removes a *correctness* precondition, and §3.3 measured that a large part of the
audience already satisfies it: an application that updates the aggregate root gets the serialisation
**for free** from an explicit flush, no Tandem lock required.

**Only one of them changes behaviour.** §4.1 adds no statement, no lock and no waiting. §4.2 turns
concurrent writers to one aggregate into waiters: the point of the mechanism, but a real behavioural
change that has to be chosen, not inherited from a decision about `seq`.

**Who is left after the flush, and why §4.2 is worth having on its own.** §3.3's third row: writers
that only insert *children* of the aggregate reorder even with an explicit flush, because there is no
shared row to lock. That population overlaps heavily with §4.1's (nothing bumps a version on the
root, so they have no number to give either), which is why the two belong in one design note. But
overlap is not identity: an aggregate with a good event counter can still have writers that never
touch the root. Independent flags, freely combined, cover all four cells.

### 4.4 Choosing a mode

| | `seq(long)` | `unsequenced()` |
|---|---|---|
| **The application must supply** | a number per event, increasing within the aggregate, plumbed to the call site | nothing |
| **Ordering detection** | **strongest**: catches both a commit-order reorder and a numbering that disagrees with insert order (§6.1) | commit-order reorder |
| **What a consumer reads in `ce_seq`** | the aggregate's own number for the event, joinable against the domain | nothing; consumers deduplicate on `ce_id` |
| **`UNIQUE (aggregate_id, seq)`** | active: rejects a duplicate number | inert (§4.1) |
| **What can go wrong** | the ORM staleness family (§3.2): a pre-flush `@Version` gives two events the same number and aborts the business transaction; a mutation that dirties no persistent field advances no version, so the next transaction reuses the number; one version that produces two events gives both the same number (§2). Needs an explicit flush before the outbox row is built, and a number per event rather than per state | nothing specific |

**The choice is one question: do consumers need a number the aggregate assigns to each event?**
If yes, `seq(long)`; otherwise `unsequenced()`.

**The tension worth naming:** `seq(long)` also buys the stronger detection, so an application that
*can* supply a number has a reason to even when no consumer reads it. But it is the mode most likely
to be got wrong: the §3.2 failures are real, one of them is data-dependent enough to survive a test
suite, and the explicit flush that avoids them is easy to omit. Stronger detection in exchange for a
hazard the other mode does not have is a genuine trade, not a free upgrade.

**Why no default.** The mode fixes what consumers see in `ce_seq`, and changing it later for an
aggregate type that already has consumers is a contract change (§5): a sticky decision, taken once
per aggregate type, worth one deliberate call. A default would also make something *invisible*: an
application that could have supplied `seq` would silently receive the weaker detection, and nothing
would ever surface that. The failure therefore states the choice rather than merely refusing: the
message names both modes and what each is for, because this is the moment the decision is actually
being made. Requiring the choice is the conservative position: accepting a message that states
neither, as `unsequenced()`, would remove a refusal and is an additive change any later minor version
may make, while the reverse would break callers.

**When undecided: `unsequenced()`.** A *default* serves a caller who never considered the question,
silently; a *recommendation* serves one who considered it and is unsure, and who has therefore
already learned that `seq(long)` buys stronger detection and that `ce_seq` is a consumer contract. The
recommendation is reached through the decision, not instead of it, which is why it belongs in this
document and in the failure message, and not in the builder.

Beyond requiring nothing of the domain and having no hazard of its own, it is the starting point
whose door stays open: moving *away* from it is the cheap direction.

| From → to | Effect on consumers |
|---|---|
| `unsequenced()` → `seq(long)` | **additive**: an extension appears where there was none; a consumer not reading it is unaffected |
| `seq(long)` → `unsequenced()` | breaking: a field a consumer may read disappears |

The one thing starting unsequenced gives up permanently is retrofitting the aggregate's numbers onto
events already published. Only `seq(long)` from the first event preserves it, and only for an
application that has numbers to give.

### 4.5 Rejected: Tandem numbering the events

Tandem does not assign `seq` on the application's behalf. Two mechanisms were considered for it, and
the reason they are not part of the design is the same: once `seq` may be omitted (§4.1), what a
Tandem-assigned number would still offer is small, and it costs every layer of the write contract.

**A database sequence as the column default.** The write side omits `seq` and
`DEFAULT nextval(…)` supplies it: no extra statement, no lock, and measured at no cost a caller could
resolve on the write path. What it gives a consumer is a number that is monotonic per aggregate and
otherwise meaningless:

- It is drawn from one global sequence, so it has gaps per aggregate: a consumer cannot recognise a
  missing event by a gap, which is the only thing a per-aggregate counter would add over `ce_id`.
- It is not a version of the domain, and cannot be joined against one.
- It does not serialise writers. Assigned at INSERT and published at COMMIT, the number taken first
  can still be published second; only the lock of §4.2 prevents that, and it does so with or without a
  number.
- It strengthens no check. The number restates insert order, so the detector would have to key such a
  row on `id` anyway (§6.1), exactly as it keys a row with no number.
- It shares `ce_seq` with the application's numbers while meaning something else, so moving a stream
  from one to the other changes the field's meaning with no signal to the consumer.

What it would cost is a second shape of everything that writes or reads a row: a second INSERT form
and a batch split around it, a column recording where each number came from so the detector could
tell the two apart, a migration step to move the sequence above an aggregate type's history before
converting it, a sequence whose `CACHE` must stay 1 for the numbers to stay monotonic, and one more
mode for every write-side SDK to implement and test (HLD-write-side-sdks). And it leaves a trap: with
a default on the column, an INSERT that forgets `seq` silently draws a number instead of failing or
storing none.

**A counter table** (`tandem_aggregate_seq`, one counter row per aggregate, incremented with
`INSERT … ON CONFLICT DO UPDATE … RETURNING`) would buy dense numbers and, through its row lock, the
serialisation of §4.2 in one statement. It is rejected on unbounded growth: the table holds one row
per aggregate **forever**. It cannot be pruned on the outbox's retention schedule, since deleting
`Order#42`'s counter makes its next event restart at `1`, colliding with history already published,
the same argument that rules out computing `MAX(seq) + 1` from `tandem_outbox`, whose rows cleanup
deletes (HLD §4.7). For per-order, per-session or per-request aggregates it grows larger than the
outbox itself, permanently, in the adopter's production database.

**What a consumer that wants a comparable number does instead** is ask the application for one:
`seq(long)` with a counter the aggregate keeps, which is the number with domain meaning the
alternatives above could not give.

---

## 5. Costs

Stated in full, because they are the reason each mechanism is opt-in rather than the default, and
separated by mechanism, because a caller chooses each independently (§4.3).

**§4.1, no number at all: two costs, both contractual.**

- **`ce_seq` is absent**, so a consumer that reads it must fall back to `ce_id` for deduplication:
  sufficient by construction, but a change for an existing stream, and the one direction that is not
  additive (§4.4).
- **The aggregate's numbers cannot be retrofitted** onto events already published unsequenced; only
  `seq(long)` from the first event preserves them.

Detection is deliberately *not* on this list: an unsequenced row keeps the commit-order detection
every row has, and only a numbered row has more (§6.1).

**§4.2, the lock: three costs, all of them real.**

- **One extra statement on the client's hot path**, inside the business transaction, so the latency
  lands on every write rather than on the relay.
- **Concurrent writers to one aggregate become waiters.** For an application that currently tolerates
  concurrent writes on an aggregate, this is a real behavioural change, not a transparent addition.
  It is the *point* of the mechanism, but it must be a decision rather than a surprise.
- **Deadlock risk across aggregates.** A transaction touching several aggregates takes several
  advisory locks; two transactions taking them in different orders deadlock. Within one `insertAll`
  batch `JdbcOutboxRepository` enforces the guard itself (sorted `aggregate_id` order), so this cost
  applies only to a caller issuing several separate `insert` calls across aggregates in one
  transaction, who is responsible for the same ordering. PostgreSQL detects and breaks a deadlock
  that does occur, so the failure is loud, not a hang.

**Not on either list, deliberately:** storage. §4.1 holds less than a numbered row and §4.2 holds
nothing at all, which is the reason the counter table is rejected (§4.5).

No mode is the default (§4.4): the choice is stated per message, and stating it is what keeps the
weaker detection from being inherited silently. Pareto applies to what a caller must *think about*,
not only to what they must type (HLD §1.1).

---

## 6. Detection: what it sees, and what it reads to see it

Detecting the §3.1 defect is cheap, independent of everything above, and helps every deployment
rather than only those that opt into §4.2.

The relay knows the last ordering value it published per aggregate; a value that goes **backwards**
is never legitimate. (A *gap* may be legitimate, since not every event of an aggregate necessarily
passes through the outbox, so only the backwards case is a sound signal.) That is
`tandem.outbox.order_violation.count`, specified in [HLD.md](HLD.md) §7 and §8: an in-process check at
publish time, because the rows are left in the table in perfect order and no later query can find the
violation. Operator replays are excluded at the source via the row's `replays` count.

**It under-reports and never over-reports.** The watermarks are bounded (an LRU per worker) and live
only in memory, so a non-zero reading is always a real violation, while zero is not proof of absence.
It reports after publication: it detects a violation, it does not prevent one.

**Exactly when the memory is lost, and when it is not.** The negatives matter as much as the
positives here, because the obvious worry is the wrong one:

| Lost | Survives |
|---|---|
| **LRU eviction** past `CAPACITY` aggregates per worker | **A worker thread dying and being restarted.** The supervisor replaces the `Thread`, not the `RelayWorker`, so the watermark object outlives the crash |
| **Relay process restart** | **Buckets moving between workers of one instance**, which cannot happen: an aggregate hashes to a fixed bucket, and the bucket-to-worker split is `bucket % workerCount`, decided once at pool construction |
| **A `LEASE` rebalance moving a bucket to another instance**: scale up or down, a deploy, a peer crashing | |
| **`SINGLE` coordination with more than one relay instance**, where the watermark is not lost but permanently *split* | |

The last two are the ones an operator has to know about, and they differ in an important way. A
`LEASE` rebalance is **already visible**: `BucketLeaseManager` logs bucket release and acquisition at
`INFO`, so an investigation can correlate a cold watermark with the moment it went cold. `SINGLE`
with several instances has **no signal at all**: every instance polls every bucket, so one
aggregate's history is divided among peers with nothing marking the division, and no instance can
even detect the situation (knowing whether peers exist would require the lease table that `SINGLE`
by definition does not use). That deployment is already documented as a misconfiguration for other
reasons (HLD §3.2); degraded detection is one more, and the one that hides the others.

**Persisting or sharing the watermarks is not the answer.** Losing a watermark has no correctness
cliff (the next row is simply a first sighting, exactly as an eviction already makes it), so nothing
forces it. The objections are that it would put a write on the relay's path for every published row,
and that it would turn an alerting signal into an audit log, which is precisely what this mechanism
declines to be.

**The correlation worth knowing about is specific to the embedded topology.** With a standalone
relay, scaling it changes nothing about the application's write-side concurrency: different
processes. With an **embedded** relay they are the same process: adding application instances both
increases the number of concurrent writers to an aggregate *and* triggers the `LEASE` rebalance that
resets the watermarks. Detection is therefore at its coldest in the one topology where the hazard it
watches for is simultaneously at its most likely.

### 6.1 Which ordering key the watermark holds

The contract in HLD §4.2 is that the orderings coincide: **commit order = `seq` order = `id`
order**. The detector compares the order rows were *published* against one of them, so which key it
holds decides which violations it can see.

Publication order is `id` order: the claim selects `ORDER BY o.id`, and the head-of-chain gate
(`e.id < o.id AND e.status IN (0,1,3)`) holds a row back while an earlier one of the same aggregate
is unfinished. One divergence from that order is **measured**: the MVCC window of §3.1, in which an
earlier, still-uncommitted row is invisible to the claim's snapshot, pinned by `CommitOrderReorderIT`.

The other paths have been **reasoned through, not measured**, and the distinction matters when
weighing what follows: retry (a due-later row still holds status 0, so the gate blocks its
successors), redelivery after a crashed relay (the row stays non-`DONE`, so the gate blocks again and
the republication is a `DUPLICATE` on either key), bucket reassignment under `LEASE`, an operator
replay (suppressed via the row's `replays`, keyed on `id` regardless), and a `DISCARDED` predecessor
(never published, so it set no watermark) all appear to leave both candidate keys silent or to yield
the same verdict. None of them has an executable test today.

| Violation | watermark on `seq` | watermark on `id` |
|---|---|---|
| `commit ≠ id`: the MVCC reorder of §3.1 | detects | detects |
| `seq ≠ id`: the application's numbering diverges from its own INSERT order | detects | **cannot see it** |

The second row is not a corner case. A watermark on `id` compares publication order against the order
rows were *inserted*, an internal invariant derived from the same column the publisher orders by. A
watermark on `seq` compares it against the order the *application declared*, which is what HLD §4.2
actually promises. `seq(long)` accepts any value: an application counter, a backfill's numbering, an
event store's revisions, two services numbering one aggregate independently. For those sources the
divergence is ordinary, and it is precisely what this detector exists to report.

**So a row is judged on its `seq` when it has one, and on its `id` otherwise.** A row with no number
declares no order, and `id` is the only ordering it has. Nothing else needs to be recorded about the
row: with Tandem never assigning `seq` (§4.5), every number in the column is one the application
declared.

**The weaker key licenses the stronger conclusion**, which inverts the intuition and decides what the
relay is entitled to say when it reports:

- **`id` key, violation observed.** Publication order diverged from insert order. The head-of-chain
  gate makes the MVCC window of §3.1 the only way that can happen, and that window requires two
  overlapping transactions on one aggregate committing in inverted order. The conclusion *"writers to
  this aggregate are not serialised"* follows by construction.
- **`seq` key, violation observed.** Publication order diverged from the order the application
  declared. That admits the MVCC window **or** an application whose numbering simply disagrees with
  its own insert order: a bug in how the number is produced, with no concurrency involved. The
  observation is a *disagreement*; which of the two orders was the right one, the detector does not
  establish.

So a numbered row detects more (the table above) but concludes less. The relay reports the two cases
differently rather than asserting the stronger cause in both: the second is a finding that needs
investigation, not a diagnosis.

A watermark records *which* key it holds, and **a key change resets the entry** rather than comparing
a `seq` against an `id`, so the row on which an aggregate switches modes is unverifiable, and an
aggregate that alternates modes frequently detects close to nothing. Switching mid-stream is already a
consumer contract change (§5), so it should be rare.

**This weakens one argument for §4.2, and only one.** The strongest argument for the lock would be
§1's: that the second cost is severe *because its failure mode is silent*. That argument is not
available: the failure mode is reported, imperfectly but alertably. What §4.2 still offers is the
difference between detecting a violated precondition and enforcing it: a real difference, but a much
smaller one than "the only way to know". It stands on §4.3's audience instead: writers that only
insert children of an aggregate, which an explicit flush cannot help either (§3.3), are real
regardless of whether the violation they cause is later reported.

---

## 7. Compatibility

- **DDL:** `seq` is a nullable `BIGINT` with no default, under `UNIQUE (aggregate_id, seq)`. Schema v6
  reached that state from the earlier schema, which carried a `tandem_seq` sequence as the column
  default and a `seq_source` column: it drops both and clears the number of every row recorded as
  numbered by Tandem, which becomes a row with no number (LLD-jdbc §6). A row Tandem numbered before
  schema v4 was recorded then as the application's, and keeps its number. That migration is a breaking change of
  the schema, released in a major version: every writer and relay of an earlier release stops before
  it is applied.
- **Kafka:** `ce_seq` carries the application's number when the row has one and is absent otherwise.
  A consumer of a stream that Tandem used to number stops receiving it.
- **API:** `OutboxMessage.Builder.seq(long)`, `unsequenced()` and `lockedWrite()`, with `hasSeq()` on
  `OutboxMessage` and `OutboxRecord`; `OutboxCollector.record(…)` has overloads with and without
  `seq`.
- **§4.2 needs no DDL and touches no row.** `pg_advisory_xact_lock` is session-scoped state, not a
  table: nothing for `generateBaselineSql`/`checkBaselineSql` (LLD-jdbc §6) to render.
