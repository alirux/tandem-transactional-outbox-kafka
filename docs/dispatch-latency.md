# Tandem: dispatch latency and post-commit wakeup

**Version:** 2.1
**Status:** Analysis complete, every option measured or costed. **R1 (§3.2) is built and measured**;
the wakeup mechanisms themselves remain undecided (§5).
**Companion to:** HLD §6 (End-to-End Flow), [LLD-jdbc.md](LLD-jdbc.md) §3.1 (the poll loop),
[relay-sizing.md](relay-sizing.md) (how to set the two knobs today, and the measurements this note
draws on).

This note analyses the latency between the commit of a business transaction and the publication of
its outbox rows to Kafka, and evaluates the options for shortening it, in particular the family of
designs generally called **post-commit wakeup**: signalling the relay that work exists instead of
waiting for it to discover the work by polling.

It exists because "the relay polls" is the most common objection raised against a library-based
outbox, and the objection deserves a quantified answer rather than a reflexive one.

**What v2 adds.** v1.0 was written before any of this was measured, and it deferred the decision to
a measurement it named (Q-D). That measurement now exists, on two hosts, and it moves three things:

1. The idle regime is far wider than v1 assumed. It covers **every offered rate below the relay's
   throughput ceiling**, not just low-rate traffic, so the poll term is in the median of essentially
   every deployment (§1).
2. The idle query load now has a price in CPU rather than an arithmetic identity (§1), and a short
   interval was shown **not** to cost delivery capacity (§3.1).
3. A third option appears that v1 did not consider, because §3 only ever weighed a *fixed* interval:
   the idle wait can be a function of recent activity instead of a constant (§3.2). It changes the
   recommendation, so it is analysed alongside the wakeup mechanisms rather than as a footnote.

---

## 1. Where the latency actually is

The relay's dispatch latency decomposes into four terms:

```
commit → publish  =  T_discover  +  T_claim  +  T_encode  +  T_kafka
```

`T_claim`, `T_encode` and `T_kafka` are work that must happen regardless of how the relay learns
about the row. Only **`T_discover`**, the interval between the row becoming visible and a worker
looking for it, is what a wakeup mechanism addresses.

**The poll loop does not sleep between batches.** A worker claims back-to-back while work remains,
and waits only when a claim returns nothing (LLD-jdbc §3.1, `WorkerPool.runWorker`). `pollInterval`
is therefore an **idle backoff**, not a per-batch delay, and the loop has three states rather than
the two v1 described:

| State | Wait | `T_discover` for a row arriving into it |
|---|---|---|
| **Busy**: the claim returned rows | none | ≈ 0, the loop is already spinning |
| **Draining**: the claim returned nothing but publishes are still in flight | 5 ms | uniform in `[0, 5 ms]` |
| **Quiet**: the claim returned nothing and nothing is in flight | `pollInterval` ±20% jitter | uniform in `[0, pollInterval]`, mean `pollInterval / 2` |

**The quiet state is not the exception.** Below its throughput ceiling the relay is by definition
faster than its arrivals, so workers drain their slice and go quiet whatever the offered rate. The
measurements bear this out directly: the median COMMIT→ack is invariant to the offered rate (52.3 ms
at 300 events/s, 54.1 to 54.6 ms at 600 events/s) and to the worker count (a factor of four in
concurrency moves it by 2 to 3%), and tracks the poll interval alone (relay-sizing §2):

```
p50 ≈ pollInterval / 2 + service        (service: 2 to 10 ms, host-dependent,
                                         and smaller at shorter intervals)
p99 ≈ 1.4 to 2 × pollInterval
```

So the poll term is not a tax on a minority profile. It is roughly **50 ms sitting in the median of
every deployment running the default**, and it is the largest single term in that median by an order
of magnitude.

Two consequences follow, and they point in opposite directions:

1. **Sustained throughput is unaffected by polling.** A wakeup mechanism cannot improve the busy
   state, because there is nothing to improve. Any claim that a wakeup "removes polling overhead
   from the hot path" is false here. The measured ceiling is the same at a 100 ms and at a 10 ms
   interval (§3.1), so the interval is not competing with delivery either.
2. **Discovery latency is bounded below by the knob, not by the machine.** The service floor under
   the poll term measured 2 to 7 ms on a small cloud VM and 4 to 10 ms on Docker in a VM, so
   single-digit-millisecond delivery is arithmetically out of reach while `T_discover` averages
   `pollInterval / 2` at any interval an operator would actually run.

**The third cost is the idle query load**, which scales as
`instances × workersPerInstance / pollInterval`. It now has a measured price rather than a count
(relay-sizing §1, PostgreSQL CPU over an idle baseline, 100% being one core):

| relay | queries/s | empty outbox | 200k `DONE` rows | + 2 000 unclaimable rows |
|---|---:|---:|---:|---:|
| 8 workers @ 100 ms | ~78 | 3.5% | 4.0% | 11.4% |
| 8 workers @ 20 ms | ~333 | 12.1% | 14.7% | 42.2% |
| 8 workers @ 10 ms | ~652 | 20.2% | 25.4% | 68.4% |

About **a third of a core per thousand idle queries per second** on a healthy outbox, and about **a
full core per thousand** once unclaimable rows accumulate: a partial index on `status = 0` keeps
delivered rows out of the scan, but a row that is `PENDING` and not yet due, or queued behind a
`FAILED` head, stays in it and is re-scanned on every cycle, with the claim's head-of-chain
`NOT EXISTS` running against each one. The number is noise on a single relay and stops being noise
at scale: ten instances of sixteen workers at 20 ms is 8 000 queries per second to discover nothing.

**This tension is the whole point of the analysis.** Latency and idle load are traded against each
other by a single knob, and what a wakeup genuinely buys is not speed but the ability to
**decouple** them: a low `T_discover` *and* a long `pollInterval`.

---

## 2. What any solution must preserve

These are non-negotiable and rule out most of the design space before it is explored.

**C1: The signal carries no work.** A wakeup notification must mean *"bucket N may have pending
rows"*, never *"here is event 12345"* and never *"here is the payload"*. The database stays the
single source of truth for what is pending. A signal that carries work becomes a delivery channel,
and a delivery channel that can drop, reorder, or overflow re-introduces exactly the failure modes
the outbox exists to eliminate.

**C2: Losing every signal must be harmless.** Polling remains active in all configurations, so a
lost, dropped, or never-delivered wakeup degrades latency and nothing else. This is what makes the
mechanism safe to add: it cannot be on the correctness path, so it cannot have a correctness bug.

**C3: No new infrastructure.** Tandem's positioning is "your database and Kafka, nothing else"
(HLD §11). A mechanism that requires a broker, a cache, or a coordination service for signalling
forfeits the project's main reason to exist.

**C4: No cost on the client write-side.** The write-side must not acquire dependencies, threads, or
connections for the benefit of the relay (HLD §1.3). Anything that cannot be expressed with the
`Connection` the write-side already holds belongs on the relay side.

**C5: Zero cost when unused, and a correct default without it.** Per Pareto (HLD §1.1), the
mechanism is opt-in, and the deployments that do not opt in, including every engine where it is
unavailable, must remain fully correct, differing only in latency.

**C6: It must work across processes.** The relay may run in a separate process from the writer, and
in `LEASE` mode across several processes (HLD §3.2). A mechanism confined to one JVM addresses only a
subset of the supported topologies, and must be described as such rather than as *the* solution.

---

## 3. Options

### 3.1 A shorter fixed poll interval (the baseline, cost zero)

The honest baseline every other option must beat, and a real answer rather than a placeholder. It is
now fully measured on both a developer Mac (Docker in a VM) and a small cloud VM (native Docker), at
a held 400 events/s, with zero ordering violations and zero lost events in every cell
(relay-sizing §2, raw runs in [benchmark-results/](benchmark-results/)):

| workers | poll | p50 | p95 | p99 | p99.9 |
|---:|---:|---:|---:|---:|---:|
| 8 | 100 ms | 60.0 | 135 | 203 | 292 |
| 8 | 50 ms | 31.4 | 67 | 100 | 133 |
| 8 | 20 ms | 16.0 | 34 | 45 | 67 |
| 8 | 10 ms | 9.3 | 18 | 23 | 35 |

The whole distribution translates down with the interval, and slightly faster than linearly: a
shorter sleep accumulates a smaller batch per wake, and the tail of a batch waits for its head, so
the service residual shrinks too (10.0 ms at a 100 ms interval down to 4.3 ms at 10 ms).

A second measurement removes the obvious worry about paying for it in throughput. The ceiling was
searched at both ends of the interval range, in a drained and in a blocked outbox, two replicates
per cell: 1450 versus 1550 events/s drained, 1100 versus 1175 blocked, with the **10 ms arm
sustaining slightly more in both states**. The idle queries cost CPU, and that CPU does not come back
as lost delivery capacity. What does cost capacity is the blocked outbox itself, roughly a fifth to a
quarter of the ceiling at either interval.

So the trade is purely latency against CPU, and it is linear. Its limit is the scaling term in §1:
it does not survive the combination of many instances, many workers, and a mostly-idle outbox, where
buying a 10 ms median costs two thirds of a core per relay for queries that find nothing.

- **Pros:** available today, no code, every engine, every topology, no new operational surface.
- **Cons:** trades idle load for latency linearly and pays it around the clock, including at 3 a.m.
  on an outbox nobody is writing to. Cannot reach the service floor without a load that is visible
  at scale.

### 3.2 An activity-adaptive idle backoff (new in v2, shipped)

The loop sleeps only after an empty claim. Nothing requires that sleep to be a constant: it can
start at a floor and grow geometrically with each consecutive empty claim, up to `pollInterval`, and
reset to the floor on any claim that returns rows. `pollInterval` keeps its documented meaning
exactly, the **ceiling on discovery latency**, and the worst case is unchanged.

Concretely, in `PollBackoff`: a floor (10 ms is the natural default, since the service term is 2 to
10 ms and polling faster than the work itself buys nothing), doubling, the existing ±20% jitter
applied at every step, and a reset on a productive claim. The 5 ms draining wait is a different
question and stays as it is. All three are configurable (`pollIntervalFloor`, `pollBackoffFactor`,
and `pollInterval` as the ceiling); a floor equal to the ceiling reproduces the fixed interval
exactly, which is also how the two are compared.

**What it does.** The wait self-tunes to the arrival gap of the worker's own bucket slice. At 400
events/s over 8 workers a slice sees a row roughly every 20 ms, so the backoff climbs from 10 to
20 ms and is reset before it can go further: expected median around 10 ms rather than 60, without
touching the configured interval. When the slice genuinely goes quiet the backoff walks up to the
ceiling within a few hundred milliseconds and stays there, so the idle load falls to
`workers / pollInterval` only while there is nothing to find.

**What it does not do**, stated first because it is the reason this is not a substitute for §3.4:
the **first event after a quiet gap still pays up to the ceiling**. Adaptive backoff prices a stream;
it cannot price an event that arrives out of silence, which is exactly the profile a wakeup exists
for.

**Measured, not projected.** S2 at 400 events/s, 8 workers, two replicates each, developer Mac
(relay-sizing §2): a fixed 100 ms interval gives p50 64.5 / 61.8 ms and p99 309 / 203; the shipped
default of a 10 ms floor under the same 100 ms ceiling gives **p50 12.8 / 13.1 / 12.2 and p99
61 / 59 / 57**,
zero ordering violations and zero lost events. That is 4.7× at the median and 3.4× at the p99 on the
clean replicate pair, with the ceiling, and therefore the idle cost, untouched. It does not reach the
fixed 10 ms cell (p50 9.3, p99 23): inside a stream's short pauses the backoff has already climbed a
step or two, so the ceiling still shows in the tail. What it buys against that cell is 80 idle
queries/s instead of 652.

On the 2 vCPU reference host, at 600 events/s and three runs, the same change takes the median from
54 ms to 12.6 and the p99 from 148 to 87, with the p99.9 unchanged within its own spread, and four
alternating ceiling searches there found no throughput cost (relay-sizing §2). Those are the figures
the README and the site publish.

**And it is not cheaper than §3.1 while traffic flows.** Under sustained load it converges to a fixed
interval near the arrival gap, plus the two or three polls the ramp adds per event. Projecting from
the measured cost table, the 400 events/s case above sits around 800 queries per second while the
stream runs, against 652 for a fixed 10 ms interval, and around 8 per second once the stream stops,
against 652. **The entire saving is in the quiet periods**, and that is where it is large: nights,
weekends, and, under `LEASE` with many buckets, every worker slice that happens to be cold while
others are busy. A single global interval cannot express that; a per-worker backoff prices each
slice independently, for free.

One case deserves a warning: an outbox carrying unclaimable rows *and* a live stream keeps the loop
in the fast regime while every claim re-scans those rows, which is the threefold multiplier of §1
applied at the higher rate. That combination is already the expensive one today, and this makes it
more so.

- **Pros:** roughly a sixfold improvement in the median against today's default, at an unchanged
  ceiling, so the worst case cannot regress. Every engine, every topology, no new port, no new
  dependency, no new failure mode, nothing on the client write-side. It is the only option here that
  reduces cost and latency in the same change, because it stops paying for discovery when there is
  nothing to discover.
- **Cons:** costs more CPU than today while traffic flows, in proportion to that traffic. Does
  nothing for the cold-burst profile. Changes the shipped default's behaviour, so the published
  latency figures and the two rules of thumb in relay-sizing §2 have to be re-measured and rewritten.
- **Effort:** small. One class, its configuration, unit tests, a re-run of the S2 sweep and the idle
  cost probe that already exist, and the documentation.

### 3.3 In-process wakeup hint

When the relay is embedded in the same JVM as the writer, the write-side can nudge the worker owning
the row's bucket after the transaction commits. The bucket is already computed in Java at insert time
(HLD §4.3), so the signal is a bucket number and needs no extra query. The worker parks on a bounded
wait instead of a plain sleep, and the nudge unparks it, collapsing `T_discover` to sub-millisecond.

Two constraints shape it:

- **Attaching to "after commit" is the hard part.** Plain JDBC has no commit hook. In Spring this is
  natural (a transaction synchronisation registered by `tandem-spring-producer`, firing after
  commit); in plain Java it can only be an explicit call the user makes after committing, which must
  therefore be optional and inconsequential when omitted (C2, C5).
- **It is much weaker in `LEASE` mode than it appears.** With an embedded relay on `N` replicas, the
  local relay owns roughly `1/N` of the buckets, so roughly `(N-1)/N` of local writes land in a
  bucket owned by a different process. Those hints are useless, and the event waits for the remote
  instance's poll as before. The hint is effective in `SINGLE` mode and in co-located deployments,
  and largely ineffective in exactly the horizontally-scaled deployments where §1's idle-load
  argument bites hardest.

- **Pros:** no protocol, no connection, no engine dependency; trivially satisfies C1 to C5.
- **Cons:** violates C6 (one JVM only); its benefit decays as `1/N` in `LEASE` mode; needs a commit
  hook the write-side cannot provide on its own outside Spring.

### 3.4 PostgreSQL `LISTEN` / `NOTIFY`

The only mechanism that crosses process boundaries without new infrastructure. Its semantics line up
with the outbox unusually well:

- **It is transactional.** A notification is delivered when its transaction commits and discarded
  when it rolls back. A signal can therefore never announce a row that does not exist, which is the
  pathology that makes out-of-transaction signalling unsound.
- **PostgreSQL coalesces duplicates.** Notifications with the same channel *and* the same payload
  within one transaction are delivered once. With the bucket as the payload, a transaction inserting
  fifty events for one aggregate produces one notification, with no coalescing code to write.
- **The payload is a bucket number**, satisfying C1 by construction: there is no room in it for work.

Three variants differ only in *who* emits the notification:

| Variant | Pros | Cons |
|---|---|---|
| **A second statement on the write-side connection** (`SELECT pg_notify('tandem_bucket', ?)` after the INSERT, same transaction) | No DDL; no commit hook needed, transactional semantics do the work; uses the `Connection` the write-side already holds (C4) | One extra round-trip per transaction, amortisable to one per distinct bucket per transaction |
| **A CTE fused into the INSERT** (`WITH ins AS (INSERT … RETURNING bucket) SELECT pg_notify(…) FROM ins`) | No additional round-trip | Turns the insert into a result-set-producing statement, complicating JDBC batching and generated-key retrieval, a real cost on the hot write path |
| **An `AFTER INSERT` trigger** | The writer does nothing at all; works for any writer, including one on an older Tandem version or a direct SQL insert | Opinionated DDL the operator must apply; per-row trigger overhead; moves library behaviour into the schema, where it is invisible and versioned separately |

Operational caveats, all manageable but all mandatory to document:

- The relay must hold a **dedicated listening connection per instance**, outside the business pool.
  With pgjdbc this means a thread blocked in `getNotifications(timeout)`, a socket read rather than a
  query, but still a thread and a backend.
- **Connection poolers in transaction-pooling mode break `LISTEN`.** This is the most common way for
  the mechanism to silently stop working, and it must be stated wherever the feature is documented
  rather than discovered in production.
- The notification queue is cluster-wide and bounded (8 GB). A listener that stops draining can fill
  it and cause commits to fail, a new way to affect the database that does not exist today. Correct
  consumption avoids it; the failure mode still has to be acknowledged.
- The channel name is per-database, so it has to carry the schema once one database can host more
  than one outbox.
- **PostgreSQL only.** MySQL has no equivalent primitive (HLD §5.4), so the fallback there is §3.1
  and §3.2, permanently.

### 3.5 Rejected

**An external signal bus** (a Redis channel, a dedicated Kafka topic). Violates C3, and, more
seriously, violates C1 and C2 the moment the signal is emitted outside the database transaction: a
signal sent before a commit that then rolls back, or a commit whose signal is lost, reproduces the
dual-write problem in miniature. On the write side it would also breach C4.

**CDC / logical replication as the wakeup trigger.** Contradicts the project's premise: if a CDC
pipeline is available and operable, the outbox relay is largely redundant (comparison.md §2).

**A dedicated signal table polled at a tight interval.** Relocates the polling to a smaller table
without eliminating it. Against a partial index on `status = 0` over a drained outbox, the saving is
close to zero, because the current query is already the cheap one.

**Fewer workers so that each stays busy.** Proposed and measured, and it is not a latency mechanism:
cutting 8 workers to 2 at a fixed 100 ms left the median untouched (60.0 to 58.3 ms on the Mac, 56.9
to 55.7 ms on the cloud VM). Workers do not stay busy at 200 events/s each, they still go fully quiet
and pay the whole offset. The p99 did improve, but that is less thread contention making the service
term cheaper, not rows being discovered sooner. What the worker count *does* buy is the iso-cost
trade: 2 workers at 25 ms and 8 at 100 ms issue the same 80 idle queries per second, and the former
is four times better at the median. That is a way of spending the §3.1 knob, not an alternative to it,
and it turns against you on a blocked outbox, where a wider bucket slice per claim scans
proportionally more unclaimable rows.

---

## 4. Side by side

Idle discovery is the median for a row arriving into a quiet worker. Cost is the idle query load for
one relay of 8 workers at the default 100 ms ceiling.

| | Discovery, stream | Discovery, first event after silence | Idle cost | Cross-process | Engines | New failure surface | Effort |
|---|---|---|---|---|---|---|---|
| Current default, 100 ms fixed | ~60 ms | ~50 ms | 80 q/s, 3.5% core | n/a | all | none | none |
| §3.1 Fixed 10 ms | ~9 ms | ~5 ms | 652 q/s, 20% core (68% blocked) | yes | all | none | none |
| §3.2 Adaptive backoff (shipped) | ~13 ms measured | up to the ceiling | traffic-proportional under load, `workers / pollInterval` quiet | yes | all | none | small |
| §3.3 In-process hint | sub-ms, locally-owned buckets only | sub-ms, same restriction | as configured | **no** | all | none | medium-low |
| §3.4 `pg_notify` | sub-ms | sub-ms | as configured | yes | PostgreSQL | listening connection, pooler incompatibility, notification queue | medium |

The two rows that matter are §3.2 and §3.4, and they are **complements, not alternatives**: adaptive
backoff prices the warm stream and costs nothing when idle, and the wakeup removes the single case
adaptive backoff is bad at. Together they are what finally allows the ceiling to be raised to 500 ms
or a second, which is where the idle half of §1's trade-off is actually won. Neither one alone gets
both.

---

## 5. Recommendation

**R1. Adopt §3.2, with the ceiling left at the current 100 ms default. Done.** It is the only change
here that improves latency and idle cost in the same step, it is available on every engine and in
every topology, and with an unchanged ceiling it cannot regress the worst case. Measured at 4.7× on
the median (§3.2) for a small change to one class. The cost to accept is CPU while traffic flows, in
proportion to that traffic, and a re-measurement of the published figures: **the reference numbers on
the site and in the archived runs were taken at the fixed 100 ms interval and now describe the
pre-adaptive default.**

**R2. Treat §3.4 as conditional, not as the next step.** After R1 the remaining gap is exactly one
profile: an event arriving out of silence, with a single-digit-millisecond requirement. Build the
port and the `pg_notify` adapter when a real deployment presents that profile, or when raising the
ceiling into the hundreds of milliseconds becomes worthwhile for the idle-load reasons in §1. Its
value is much higher after R1 than before, because R1 is what makes a long ceiling tolerable for
everything else.

**R3. Do not ship §3.3 on its own.** Under `LEASE` it helps `1/N` of writes and is silent about the
rest, which is a mechanism whose behaviour cannot be explained to an operator in one sentence. If the
port of §6 is built, an in-process adapter is a small by-product; it is not a deliverable.

**R4. The blocked-outbox multiplier is a separate item, and possibly a larger win than any of the
above.** Unclaimable rows triple the cost of every empty claim and cost a fifth to a quarter of the
throughput ceiling, and both effects come from the shape of the claim rather than from the interval:
rows that are `PENDING` but not yet due, or queued behind a `FAILED` head, stay in the partial index
and are re-scanned every cycle, with the head-of-chain `NOT EXISTS` evaluated against each. That is an
indexing and query-shape question, independent of everything in §3, and it should not be folded into
this note's decision.

---

## 6. Proposed shape, if the wakeup is built

A port in `tandem-core`, a wakeup signal with a `signal(int bucket)` emission side and a subscription
side consumed by the relay, with a **no-op default** and adapters (HLD §1.2):

1. **None (default).** Polling with the idle backoff of §3.2. Zero cost, every engine, every
   topology.
2. **In-process**, applicable when the relay and the write-side share a JVM.
3. **`pg_notify`**, explicitly opt-in, for the split topology on PostgreSQL.

The worker's sleep becomes a bounded wait on the signal rather than a plain sleep, so the poll
interval remains the ceiling on discovery latency in every configuration. That single property is
what keeps the whole feature off the correctness path (C2): with no adapter configured, the wait
expires and the loop behaves exactly as it does today; with an adapter configured but broken,
misconfigured, or silently disabled by a connection pooler, the wait expires and the loop behaves
exactly as it does today.

The composition with §3.2 is direct: a signal resets the worker's backoff to its floor and unparks
it, so the adaptive ramp and the wakeup share one mechanism rather than competing for the sleep.

---

## 7. When this is not worth building

Stated plainly, because the analysis does not support building the wakeup unconditionally:

- **If the outbox is rarely idle**, the gain is zero. The busy state already has `T_discover ≈ 0`,
  and no mechanism improves on zero.
- **If the traffic is a continuous stream below the ceiling**, which the measurements say is the
  common case, §3.2 already brings the median to the service floor for free.
- **If the deployment is a single relay with a few workers**, §3.1 reaches ~10 ms today at a cost of
  a fifth of a core, and 10 ms is below the noise floor of most of the systems this library serves.
- **If the target engine is MySQL**, only §3.1, §3.2 and §3.3 are available at all.

The profile that justifies the wakeup is specific: **bursty traffic with long idle gaps**, or **many
instances multiplied by many workers on a mostly-idle outbox**, plus a latency requirement in the
single-digit milliseconds. Absent that profile, the correct action is R1 plus the tuning guide.

---

## 8. Open questions

- **Q-A.** Should the signal be per-bucket or a single global "something changed"? Per-bucket wakes
  exactly one worker but couples the signal to the bucket count (immutable per HLD §4.3, so the
  coupling is stable); a global signal wakes every worker on every write, which does not scale with
  worker count. Open, and only relevant once §3.4 is greenlit.
- **Q-B.** In `LEASE` mode, is the `1/N` effectiveness of the in-process hint (§3.3) worth shipping at
  all, or should the in-process adapter be restricted to `SINGLE` mode so its behaviour is not
  misleading? R3 leans to the latter.
- **Q-C.** Which `pg_notify` emission variant (§3.4)? The extra round-trip is the safest for the write
  path, and the trigger is the only one that also covers writers outside Tandem's control.
- **Q-D. Closed.** *Does the benchmark harness need an idle-path latency scenario before any of this
  is decided?* It did, it now has one, and the answer changed the note: S2 reports COMMIT→ack
  percentiles at a held rate with `--workers=` and `--poll-interval=`, the idle cost probe prices the
  queries, and the capacity probe rules out the interval costing throughput. The results are in
  relay-sizing §1 and §2 and in [benchmark-results/](benchmark-results/).
- **Q-E. Closed.** *The parameters of §3.2, and whether to expose them at all.* Both are configurable
  (`pollIntervalFloor`, default 10 ms; `pollBackoffFactor`, default 2.0), on the grounds that the
  floor is now what sets the latency of the common case and an operator with a budget needs to be
  able to set it. `pollInterval` keeps its name and becomes the ceiling, so an existing configuration
  keeps its meaning: it is still what bounds the worst case and the idle load.
- **Q-F.** Should the default `pollInterval` move once §3.2 is in? It becomes a pure ceiling on the
  cold case, so a longer default would cut idle load further at the cost of that one profile. Not a
  default to change without a measurement of how often a slice actually goes cold in practice.
