# Sizing the relay: workers and poll interval

**Applies to:** `RelayConfig.workersPerInstance` and `RelayConfig.pollInterval` (`tandem-jdbc`).
**Companion to:** [dispatch-latency.md](dispatch-latency.md) (why the latency is where it is, and what
it would take to remove it), [LLD-jdbc.md](LLD-jdbc.md) §3.1 (the poll loop itself).

Two knobs, one of which does something other than what its name suggests. This guide says what each
one controls, gives a procedure for setting them, and shows the measurements the procedure is drawn
from.

---

## 1. What each knob actually controls

**`workersPerInstance` — throughput and blast radius, not latency.** Workers are the relay's
concurrency: each owns the buckets where `bucket % workersPerInstance == workerIndex`, claims from
them, and dispatches asynchronously. More workers means more parallel claim/publish pipelines, and a
smaller share of the outbox stranded behind any one wedged worker.

**`pollInterval` — the bound on discovery latency.** It is an **idle backoff, not a per-batch delay**:
a worker claims back-to-back for as long as work remains, and waits `pollInterval` (±20% jitter) only
after a claim that returned nothing *and* left nothing in flight. An empty claim with publishes still
outstanding waits 5 ms instead. So `pollInterval` is what a row waits when its bucket was quiet at the
moment it was written — which, below the relay's throughput ceiling, is most rows.

**The identity that ties them together:**

```
idle query load  =  instances × workersPerInstance / pollInterval
```

Every idle worker issues one claim per interval. At the defaults (8 workers, 100 ms) that is 80
queries per second per instance to discover nothing — negligible on one relay, and 8 000/s across 10
instances of 16 workers at 20 ms.

The identity counts queries. What each query *costs* depends on what the outbox holds
(`./gradlew :tandem-benchmark:idlePollCostProbe`; PostgreSQL CPU over an idle baseline, 100% = one
core, Docker-in-a-VM):

| relay | queries/s | empty outbox | 200k `DONE` rows | + 2 000 unclaimable rows |
|---|---:|---:|---:|---:|
| 8 workers @ 100 ms | ~78 | 3.5% | 4.0% | 11.4% |
| 8 workers @ 20 ms | ~333 | 12.1% | 14.7% | 42.2% |
| 8 workers @ 10 ms | ~652 | 20.2% | 25.4% | 68.4% |
| 2 workers @ 25 ms | ~71 | 4.4% | 3.9% | 20.8% |

**Delivered rows are nearly free; stuck rows are not.** `idx_tandem_outbox_dispatch` is partial on
`status = 0`, so a `DONE` row leaves it — 200 000 of them add under a quarter to the cost of a poll.
A row that is `PENDING` but *unclaimable* stays in the index and is scanned on every cycle, and the
head-of-chain `NOT EXISTS` runs against each one. Two ordinary situations produce them: a row backing
off after a failure, and a row queued behind a `FAILED` head. Fewer than eight such rows per bucket
roughly **triple** the cost of every claim that finds nothing.

**Budget accordingly:** about **a third of a core per thousand idle queries/s** on a healthy outbox,
and about **one core per thousand** once stuck rows accumulate. The arithmetic above over-estimates
the rate itself — a worker's idle cycle is the sleep *plus* the claim it just ran, so the true period
is `pollInterval + claim time` and the measured rate lands 3-17% under `workers / pollInterval` — so
it is safe to plan with.

**The two knobs buy each other only while the outbox is healthy.** A claim filters
`WHERE bucket = ANY(?)` over the worker's own slice, so halving the workers doubles the buckets each
claim covers. With the partial index near-empty that costs nothing, and 8 workers at 100 ms and 2 at
25 ms are genuinely interchangeable. With stuck rows in the index the wider slice scans proportionally
more of them, and the same 80 queries/s cost **twice as much** at 2 workers as at 8. Trading workers
for a shorter interval is cost-neutral on a clean outbox and turns against you on a blocked one —
which is to say, at the moment something is already wrong.

---

## 2. What the measurements show

Two axes, measured with S2 (COMMIT→ack, HdrHistogram) at a constant 400 events/s. Milliseconds.

**Changing workers, poll interval fixed — the median does not move:**

| workers | poll | p50 | p95 | p99 | p99.9 |
|---:|---:|---:|---:|---:|---:|
| 8 | 100 ms | 60.0 | 135 | 203 | 292 |
| 4 | 100 ms | 59.4 | 125 | 184 | 247 |
| 2 | 100 ms | 58.3 | 118 | 155 | 217 |

A factor of four in concurrency moves the median by 3%. The tails do improve, but that is the service
term getting cheaper (fewer threads contending), not rows being discovered sooner.

**Changing the poll interval, workers fixed — everything moves:**

| workers | poll | p50 | p95 | p99 | p99.9 |
|---:|---:|---:|---:|---:|---:|
| 8 | 100 ms | 60.0 | 135 | 203 | 292 |
| 8 | 50 ms | 31.4 | 67 | 100 | 133 |
| 8 | 20 ms | 16.0 | 34 | 45 | 67 |
| 8 | 10 ms | 9.3 | 18 | 23 | 35 |

**Three configurations costing the database exactly the same 80 idle queries/s:**

| workers | poll | p50 | p99 |
|---:|---:|---:|---:|
| 8 | 100 ms | 60.0 | 203 |
| 4 | 50 ms | 31.1 | 92 |
| 2 | 25 ms | 18.2 | 47 |

Zero ordering violations and zero lost events in every cell, including at 10 ms.

Two rules of thumb come out of it:

```
p50 ≈ pollInterval / 2 + service        (service: ~2-10 ms, host-dependent,
                                         and smaller at shorter intervals)
p99 ≈ 1.4 to 2 × pollInterval
```

**Both were re-measured on a different host** — a 2 vCPU cloud VM with native Linux Docker, where the
tables above came from a developer Mac running Docker in a VM. Same three claims, same 400 events/s:

| workers | poll | p50 | p99 | |
|---:|---:|---:|---:|---|
| 8 | 100 ms | 56.9 | 187 | baseline |
| 2 | 100 ms | 55.7 | 162 | worker axis: −2% on the median for a 4× cut in concurrency |
| 2 | 25 ms | 14.3 | 35 | same 80 idle queries/s as the baseline, 4.0× the median and 5.3× the p99 |

The service term under `poll/2` is smaller there (6.9 ms at 100 ms, 1.8 ms at 25 ms) — native Docker,
no VM `fsync` penalty. Treat both sets as showing the *shape*, not as capacity numbers, and re-measure
the service term on your own hardware (§6).

One caveat carried over from the archived runs: on a burstable instance and a short window the
**median reproduces and the p99 does not**. The baseline cell above is the shipped default, and gave
p99 187 ms where a longer archived run of the same configuration gave 148
([benchmark-results/](benchmark-results/)). Size against the median; treat a single p99 as indicative.

---

## 3. The procedure

**Step 1 — size the workers for throughput, and leave headroom.** Start at the default
(`availableProcessors() × 2`, minimum 1) and raise it only if the relay cannot keep up with the
offered rate. Aim to sit at roughly half the rate the relay can sustain: the slack is what drains the
backlog after a burst, a restart, or a Kafka hiccup, and it costs nothing while unused. Do not tune
this knob for latency in either direction — §2 shows it does not move the median.

**Step 2 — set the poll interval from the latency budget.** Invert the rule of thumb: for a target
p99 of `T`, set `pollInterval ≈ T / 2`. Some worked values:

| Latency budget (p99) | `pollInterval` |
|---|---|
| ~200 ms — projections, search indexing, analytics | 100 ms (the default) |
| ~100 ms — integration events between services | 50 ms |
| ~50 ms — a user-visible side effect, no read-your-writes | 20 ms |
| ~20 ms — read-your-writes through events | 10 ms |
| single-digit ms | not reachable by polling — see §5 |

**Step 3 — price what you just bought.** Compute `instances × workers / pollInterval`, then price it
at roughly a third of a core per thousand queries/s, or a full core per thousand if the outbox carries
stuck rows (§1). At the default sizing that is ~4% of one core and not worth a thought; at 800
queries/s it is a quarter of a core on a clean outbox and two thirds on a blocked one. The load scales
with instance count, so a figure that is fine on one relay is 10× that at ten — that multiplication,
not the single-relay number, is usually what makes this matter.

**Step 4 — verify on your own hardware.** §6.

---

## 4. Starting points

| Profile | `workersPerInstance` | `pollInterval` | Idle load |
|---|---|---|---|
| Single embedded relay, ordinary integration events | default (`cores × 2`) | 100 ms | ~80/s |
| Single relay, latency-sensitive consumer | default | 20 ms | ~400/s |
| Mostly-idle outbox, latency matters | 2 | 20 ms | 100/s |
| `LEASE`, several instances, ordinary events | default | 100 ms | 80/s × instances |
| `LEASE`, many instances, latency matters | 4 | 25 ms | 160/s × instances |
| High sustained throughput, latency secondary | default or higher | 100 ms | ~80/s |

The two rows with a reduced worker count assume `FAILED` and backing-off rows are cleared rather than
left to accumulate (the Admin API's replay and discard exist for this). On an outbox that collects
stuck rows, keep the workers at their default and buy latency with the interval alone — a wide bucket
slice and a short interval is the one combination that multiplies both halves of the cost.

---

## 5. Floors, and what tuning will not fix

**The service floor.** Under the poll term there is irreducible work: the claim round trip, the
CloudEvents encode, the Kafka publish with `acks=all`, and the consumer's own fetch. It measured
~2-7 ms on a small cloud VM and ~4-10 ms on Docker-in-a-VM. A poll interval far below it buys nothing
but query load.

**The host tail.** On a saturated or shared machine the p99.9 carries tens of milliseconds that are
neither poll nor service — GC, CPU contention, a broker pause. Lowering the poll interval translates
the whole distribution down; it does not remove that tail.

**Below the floor.** Single-digit-millisecond delivery is not reachable by shortening the interval —
that is the case a post-commit wakeup exists for, and the design (including why it is not built yet,
and why it is much weaker under `LEASE` than it looks) is in [dispatch-latency.md](dispatch-latency.md).

---

## 6. Measuring it yourself

The load-test harness takes both knobs, so the table in §2 can be reproduced against your own
database and broker rather than trusted:

```bash
./gradlew :tandem-benchmark:loadTest --args="--demo --duration=90 --workers=4 --poll-interval=25 S2"
```

S2 reports COMMIT→ack p50/p95/p99/p99.9 at a held load, and the run's first line records the sizing it
used — quote a percentile only together with that line.

The cost side has its own probe, which holds the outbox at each of the three states in §1 and reports
the query rate each sizing actually produces alongside what it costs PostgreSQL:

```bash
./gradlew :tandem-benchmark:idlePollCostProbe --args="--seconds=60"
```

Both require Docker ([LLD-benchmark.md](LLD-benchmark.md) §9).

---

## 7. Side effects worth knowing before you turn the interval down

- **`pollInterval` is also the first retry delay after a failing cycle**, growing exponentially to
  `reclaimInterval` (default 5 s). A 10 ms interval means a relay whose database is down starts
  retrying every 10 ms — bounded by the cap, but it reaches it through a few more doublings.
- **Keep `workersPerInstance ≤ bucketCount`.** Buckets are assigned as `bucket % workersPerInstance`;
  beyond the bucket count, the surplus workers own nothing and poll forever to find it.
- **Under `LEASE`, the split is over the buckets the instance currently owns**, not all of them, so
  worker slices are less even than the arithmetic suggests. Prefer a worker count that divides the
  bucket count.
- **Both knobs are read at relay startup.** Changing either needs a restart — unlike pause/resume,
  which the Admin API applies at runtime.
