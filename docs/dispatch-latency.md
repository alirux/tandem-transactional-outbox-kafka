# Tandem: dispatch latency and post-commit wakeup

**Version:** 3.0
**Status:** Analysis complete, every option measured or costed. **R1 (§3.2) is built and measured**, and
so is the wakeup itself: the port of §6 with the `pg_notify` adapter of §3.4, opt-in and off by default.
Only the in-process hint (§3.3) is deliberately not built (R3).
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

**What v3 adds.** The wakeup itself. §3.4 is built and opt-in, §6 records the shape it took rather
than the shape proposed, and the three questions its design turned on are closed (Q-A, Q-B, Q-C). The
analysis in §1 to §5 is unchanged: nothing measured since v2 revised it, and the recommendation it
produced is exactly what was implemented.

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

### 3.4 PostgreSQL `LISTEN` / `NOTIFY` (shipped, opt-in)

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

**What was built.** The first variant, a second statement on the write-side connection, for the reason
Q-C gives: it is the only one that costs the write path nothing but a round trip, needs no DDL, and
leaves JDBC batching alone. The payload is the bucket number (Q-A), so a signal wakes the one worker
that owns it and the cost of a wakeup does not grow with the worker count.

Concretely, and in one line each:

| | |
|---|---|
| Channel | `tandem_wakeup`, a published contract between a writer and a relay |
| Emission | `Wakeup.PG_NOTIFY` on `JdbcOutboxRepository`, or `tandem.outbox.wakeup: pg-notify` under Spring. One statement per `insert`/`insertAll` call, over the array of distinct buckets that call wrote, so a fifty-row batch costs the same single round trip as one row |
| Subscription | `PgNotifyWakeup`, one dedicated connection per relay instance, reconnecting with full jitter and asking for a full sweep whenever it (re)subscribes, since nothing emitted while it was disconnected was delivered |
| Relay-side seam | `WakeupSource`, alongside `BucketSource` and `RelayControlSource` rather than in `tandem-core`: only the `WorkerPool` consumes it, and there is no swappable boundary anywhere else (HLD §1.1) |
| Worker wait | `WorkerWakeups`, a per-worker flag the idle wait ends on. A signal that arrives while its worker is mid-claim is remembered, not dropped, which is the case the mechanism exists for |
| Driver footprint | `org.postgresql:postgresql` is `compileOnly` on `tandem-jdbc` and reaches no consumer POM; the one class touching it loads only where the mode is configured |

**What it costs the database, measured.** At 400 events/s under `LEASE` with two instances of eight
workers, the relay issues **~820 claims/s without the wakeup and ~1290 with it**, two and three per
delivered event: half as many again, confirmed by three independent PostgreSQL counters (dispatch
index scans +57%, aggregate index scans +43%, committed transactions +50%). Per worker that is a claim
every ~20 ms against every ~12 ms, so the wakeup arm sits essentially pinned to its floor. Neither arm
lost throughput on that host, but this is the cost the §3.2 analysis predicted in words and now has a
number for, and roughly half of it is the broadcast below rather than the mechanism itself
([benchmark-results/2026-08-30-wakeup](benchmark-results/2026-08-30-wakeup/)).

**One of the three ways to buy that cost back is now built.** They address different halves of it,
and the split matters: at 3.2 claims per delivered event against 2.0, most of the extra claims are
*productive but thinner* (the same work in smaller batches), not wasted.

1. **Filter signals by ownership (built).** The broadcast below is the waste, and
   `WorkerPool` now drops a signal whose bucket this instance neither owns nor may claim. It reads a
   snapshot of the owned set refreshed on the heartbeat tick, because `BucketSource.ownedBuckets()` is
   a live query and consulting it per notification would cost more than the wakes it saves; a stale
   snapshot drops a signal, which costs one poll interval and nothing else. A sweep is exempt, since it
   does not claim to know which bucket. Under `SINGLE` it only ever removes paused buckets.

2. **Do not reset the ramp on a wake (not built).** A signal puts the woken worker's backoff back at
   its floor before the claim runs. For a bucket that really has work this changes nothing — the claim
   finds rows and the ordinary path resets anyway — but for a wake that claims nothing it pins the
   worker at the floor, which is exactly what a burst of signals for rows it had already taken produces.

3. **Give a wake a minimum spacing (not built).** Even filtered, nothing bounds how often a signal can
   make a worker claim: `pollIntervalFloor` is not a brake, because the wake bypasses it. Refusing to
   wake a worker that claimed less than `X` ms ago would turn the remaining cost into a choice — at
   25 ms that is 40 claims/s per worker, *below* the polling arm's ~50, with discovery still bounded at
   25 ms rather than at the ceiling. It is the one of the three that trades latency away, which is why
   it is a knob and not a default.

An **external cache or signal bus does none of them**, and not for want of tuning: the claim is not a read.
It also enforces the head-of-chain gate and takes exclusive ownership of the row, so a copy held
elsewhere still has to be followed by the same statement against the database before anything is
published (§3.5 rejects the bus form on its own grounds).

**One property to know before turning it on under `LEASE`:** the notification is a broadcast, so every
instance is told about every bucket, including the roughly `(N-1)/N` of them it does not own. The relay
filters those out rather than acting on them (item 1 above), so what remains of the broadcast is the
delivery itself: the notification still crosses the network to every listener and is still drained
there. That is cheap next to a claim, but it is not nothing, and it is the reason a channel-per-bucket
scheme would still have something to offer at large instance counts — at the price of making the
channel name a published contract. It is not a correctness question, and it does not arise under
`SINGLE`, where one instance owns everything.

Its correctness rests on doing nothing new: a missing signal, a pooler that ate the subscription, a
relay listening for a mechanism the writer does not emit, or no driver at all, each leave a relay that
discovers rows by polling exactly as it did before, within `pollInterval`.

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
| §3.4 `pg_notify` (shipped) | sub-ms | sub-ms | as configured | yes | PostgreSQL | listening connection, pooler incompatibility, notification queue | medium |

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

**R2. §3.4 is built, and it is off by default.** After R1 the remaining gap was exactly one profile:
an event arriving out of silence, with a single-digit-millisecond requirement. The port and the
`pg_notify` adapter now close it for the deployments that have that profile, while every deployment
that does not keeps the behaviour and the cost it had. Two consequences follow from leaving it opt-in
rather than making it the default: a relay only holds a listening connection where an operator asked
for one, and the published latency figures continue to describe the polling default (§3.2), which is
what the overwhelming majority of deployments run.

Turning it on is what makes a **longer `pollInterval`** worth considering, and that is where the idle
half of §1's trade is actually won: with the ceiling at a second, an idle fleet pays 8 queries per
second per instance instead of 80, and the cold row that a second would otherwise cost is the one case
the signal covers. Raising the ceiling is still a separate, deliberate decision (Q-F), not something
enabling the wakeup should do on its own.

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

## 6. The shape that was built

A relay-side seam, `WakeupSource`, with a **no-op default** and one adapter (HLD §1.2):

1. **None (default).** Polling with the idle backoff of §3.2. Zero cost, every engine, every
   topology.
2. **`pg_notify`**, explicitly opt-in, for the split topology on PostgreSQL (§3.4).

An in-process adapter is not built, per R3.

The worker's sleep became a **bounded wait** on the signal rather than a plain sleep, so the poll
interval remains the ceiling on discovery latency in every configuration. That single property is
what keeps the whole feature off the correctness path (C2): with no adapter configured, the wait
expires and the loop behaves exactly as it did before; with an adapter configured but broken,
misconfigured, or silently disabled by a connection pooler, the wait expires and the loop behaves
exactly as it did before. The relay logs a failed subscription and keeps delivering.

The composition with §3.2 is direct: a signal resets the worker's backoff to its floor and ends its
wait, so the adaptive ramp and the wakeup share one mechanism rather than competing for the sleep.

**Where the seam is.** In `tandem-jdbc`, next to `BucketSource` and `RelayControlSource`, not in
`tandem-core`. The `WorkerPool` is the only consumer and the emission side needs the caller's JDBC
`Connection`, which a dependency-free core port cannot name; a port in `tandem-core` would have been
ceremony around a boundary nothing else crosses (HLD §1.1, §1.2).

**What is measured.** The mechanism is pinned end to end against a real PostgreSQL
(`PgNotifyWakeupIT`): a row written into a relay whose next poll is thirty seconds away is dispatched
in tens of milliseconds, a rolled-back write signals nothing, a batch signals each bucket it wrote
once, an unreadable payload is skipped without unsubscribing, and a killed listening backend
resubscribes and keeps delivering.

The **benchmark** for it is S10 (LLD-benchmark §8), the cold-burst shape none of S1 to S9 had: 2
events/s so that every event arrives into a slice already waiting at the ceiling, four windows in the
order poll, wakeup, wakeup, poll inside one run. Run on a developer Mac and on the reference host,
~300 events per window
([benchmark-results/2026-08-30-wakeup](benchmark-results/2026-08-30-wakeup/)):

| | commit→ack p50 | p95 | p99 | write txn p50 |
|---|---:|---:|---:|---:|
| Mac, poll | 64.0 ms | 114.7 | 122.5 | 3.86 ms |
| Mac, wakeup | **7.7 ms** | **9.3** | **10.3** | 4.39 ms |
| reference host, poll | 56.5 ms | 106.7 | 117.8 | 1.32 ms |
| reference host, wakeup | **5.5 ms** | **8.3** | **11.5** | 1.42 ms |

**8× at the median on the Mac and 10× on the reference host, with the arms not overlapping at any
percentile on either**: the worst reading in a wakeup window is better than the best median in a poll
window. The poll arm lands where `pollInterval / 2 + service` says it should, which is what argues
the difference is the mechanism rather than the machine.

The write side pays for it, in the direction one round trip should, and the size of that payment is
where the two hosts part company: 0.53 ms on the Mac against **0.10 ms on the reference host**, whose
write transaction is three times faster to begin with (native Docker, no VM `fsync` penalty). Neither
figure is resolvable, because on both machines the spread inside each arm is at least as large as the
gap between them, so what the runs establish is an **upper bound of about a tenth of a millisecond**
on a native host.

**And it costs no throughput this host can resolve.** Four alternating `S1` ceiling searches on the
reference host (none, wakeup, wakeup, none): 1300 and 1350 events/s in the poll arm, 1300 in the
wakeup arm, one search producing no bracket at all. The poll arm disagrees with itself by more than
the arms disagree with each other, which is the same verdict, and the same reasoning, as the adaptive
backoff's own A/B on this machine. It rules out a large effect, not one of a few per cent.

---

## 7. When this is not worth turning on

Stated plainly, because the analysis does not support enabling the wakeup unconditionally, which is
why it ships off:

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

- **Q-A. Closed.** *Per-bucket or one global "something changed"?* Per-bucket, carried as the
  notification payload. It wakes exactly one worker, so a wakeup's cost is independent of the worker
  count, and the coupling it introduces is to a value that is immutable anyway (HLD §4.3).
- **Q-B. Closed.** *Is the in-process hint worth shipping under `LEASE`?* Not shipped at all, per R3.
  The seam is there, so an in-process adapter remains a small by-product if a deployment ever asks for
  one; nothing about the current design has to change to add it.
- **Q-C. Closed.** *Which emission variant?* The second statement on the write-side connection. The
  CTE saves the round trip but turns the insert into a result-set-producing statement, on the hot write
  path, and the trigger moves library behaviour into operator-applied DDL. The round trip is amortised
  instead: one statement per insert call, over the array of distinct buckets it wrote.
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
- **Q-G. Closed.** *Should the harness grow the cold-burst scenario S1 to S9 do not have?* It did.
  S10 runs both arms inside one run, on both hosts, and the ceiling A/B that answers the capacity half
  ran on the reference host (§6). What remains is not a measurement but a limit of the instrument: on a
  fast developer machine S1 cannot bracket a ceiling at all, because the relay outruns the harness's
  own load generator, so that half of the question can only ever be asked on the small host.
- **Q-H.** Do wakeups, and claims generally, deserve a metric? Nothing counts either today: an operator
  whose pooler silently ate the subscription sees a latency profile that looks like the polling default
  and no other symptom, and the claim rate itself had to be recovered from PostgreSQL's own counters to
  be known at all (§3.4). That detour also surfaced something no one had seen: at the plateau the relay
  alternates between two plans for the claim query, roughly every five to ten minutes, and which one is
  running cannot be read from outside the database. It is the argument that this is not merely a
  nicety: what is not counted is not noticed. A counter is cheap; whether it belongs in the metrics
  port, which is a published contract, is the actual question.
