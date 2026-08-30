# Sizing the relay: workers and poll interval

**Applies to:** `RelayConfig.workersPerInstance`, `RelayConfig.pollInterval`,
`RelayConfig.pollIntervalFloor` and `RelayConfig.pollBackoffFactor` (`tandem-jdbc`).
**Companion to:** [dispatch-latency.md](dispatch-latency.md) (why the latency is where it is, and what
it would take to remove the rest of it), [LLD-jdbc.md](LLD-jdbc.md) §3.1 (the poll loop itself).

Four knobs, none of which does quite what its name suggests: the worker count is not a latency knob,
and the poll interval is a ceiling rather than the wait itself. This guide says what each one
controls, gives a procedure for setting them, and shows the measurements the procedure is drawn from.

---

## 1. What each knob actually controls

**`workersPerInstance` — throughput and blast radius, not latency.** Workers are the relay's
concurrency: each owns the buckets where `bucket % workersPerInstance == workerIndex`, claims from
them, and dispatches asynchronously. More workers means more parallel claim/publish pipelines, and a
smaller share of the outbox stranded behind any one wedged worker.

**The idle backoff is a range, not a value, and it adapts to the traffic.** A worker claims
back-to-back for as long as work remains, and waits only after a claim that returned nothing *and*
left nothing in flight (an empty claim with publishes still outstanding waits 5 ms instead). That
wait starts at **`pollIntervalFloor`** (10 ms) after every claim that found rows and grows by
**`pollBackoffFactor`** (2.0) on each consecutive empty claim, up to **`pollInterval`** (100 ms).

**`pollIntervalFloor` — what a row of a live stream waits.** Below the relay's throughput ceiling the
relay is faster than its arrivals, so its workers drain and go quiet between rows at any load. Those
rows are the common case, and what they wait for is the floor, not the interval.

**`pollInterval` — the ceiling: what the first row after a quiet stretch waits, and what a quiet
relay costs.** It bounds the wait a worker can reach, so it is still the worst case, and since a
genuinely idle relay sits at it, it is also what the idle query load is sized against.

**`pollBackoffFactor` — how fast the climb is.** At 2.0 a worker goes 10, 20, 40, 80, 100 ms: four
empty claims to reach a 100 ms ceiling, so a bucket that pauses for a second is polled about eight
times rather than ten, and one that pauses for an hour is polled at the ceiling throughout. Lower it
only to keep short pauses cheaper in latency, at the price of more queries in them.

**The identity that ties them together, for a relay that has gone quiet:**

```
idle query load  =  instances × workersPerInstance / pollInterval
```

Every quiet worker issues one claim per interval. At the defaults (8 workers, 100 ms) that is 80
queries per second per instance to discover nothing — negligible on one relay, and 8 000/s across 10
instances of 16 workers at 20 ms. **While a stream is running the rate is set by the traffic
instead**, since each row resets its worker to the floor: it rises with the arrival rate up to
`workers / pollIntervalFloor`, and falls back to the identity above within a few hundred milliseconds
of the last row. This is the half of the trade-off the floor is worth paying for: the short-interval
cost is paid only while there is something to find.

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
and about **one core per thousand** once stuck rows accumulate. That is a cost in CPU, and it does
*not* show up as lost delivery capacity — §2 measures the ceiling at both ends of the interval range
and finds no difference it can resolve. The arithmetic above over-estimates
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

### The floor against a fixed interval

The first thing to measure was the change itself: the same relay, the same load, with and without the
ramp (`--poll-floor=` equal to `--poll-interval=` is the fixed timing every earlier run used). S2 at a
constant 400 events/s, 8 workers, two replicates each, run in reverse order on the second pass.
Milliseconds, developer Mac, Docker in a VM:

| idle backoff | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| fixed 100 ms | 64.5 / 61.8 | 153 / 134 | 309 / 203 | 1358 / 283 |
| **10 ms floor → 100 ms ceiling (the default)** | **12.8 / 13.1 / 12.2** | **42 / 42 / 40** | **61 / 59 / 57** | **96 / 93 / 92** |

Zero ordering violations and zero lost events in all four runs. On the clean replicate pair that is
**4.7× at the median and 3.4× at the p99**, for an unchanged ceiling and an unchanged idle cost: the
cost probe re-run under the new default measures 75.4 queries/s at the 100 ms ceiling against the ~78
it measured before, because a worker with nothing to find reaches the ceiling within four empty claims
and stays there. Both, with the raw logs:
[benchmark-results/2026-08-30-adaptive-backoff/](benchmark-results/2026-08-30-adaptive-backoff/). The
adaptive arm also reproduces far better than the fixed one (12.8 against 13.1, where the fixed arm's
p99.9 moved by a factor of five between replicates), which is what you would expect once most rows
stop waiting on a 100 ms sleep that the host's own jitter then rides on top of.

**Confirmed on the reference host, where the published figures come from.** Three runs of S2 at 600
events/s on the 2 vCPU cloud VM: p50 13.7 / 11.2 / 12.8 ms and p99 92 / 78 / 91, against the 54.3 /
54.6 / 54.1 and 148 / 149 / 146 the same scenario gave there at the fixed interval. The median moves
by 4.3×, the p99 by 1.7×, and the p99.9 not at all: above the poll term the distribution is the
host's. Four alternating ceiling searches on that host, adaptive against fixed, found no capacity
cost either — 1325 against 1275 events/s, inside each arm's own spread
([benchmark-results/2026-08-30-ec2-s1-ab/](benchmark-results/2026-08-30-ec2-s1-ab/)).

**It does not reach a fixed 10 ms interval, and it is not meant to.** That cell measured p50 9.3 and
p99 23 on the same host (below), against 13 and 60 here: within a stream's short pauses the backoff
has already climbed a step or two, so the ceiling still shows up in the tail. What it buys against
that cell is the idle load, 80 queries/s instead of 652.

### The fixed-interval sweep the model is drawn from

These tables predate the adaptive backoff and are what established that the poll term, not the
service term, is what the median is made of. They remain the reference for what a fixed interval
does, and for what the *ceiling* still governs. S2 (COMMIT→ack, HdrHistogram) at a constant 400
events/s. Milliseconds.

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

Two rules of thumb come out of it, stated for a fixed interval and holding for whichever wait the
worker is actually sitting at:

```
p50 ≈ wait / 2 + service                (service: ~2-10 ms, host-dependent,
                                         and smaller at shorter waits)
p99 ≈ 1.4 to 2 × wait
```

With the adaptive default the wait is the floor for a row arriving into a live stream and the ceiling
for one arriving after a quiet stretch, so the two rules bracket the distribution rather than
describing one point of it: `p50 ≈ pollIntervalFloor / 2 + service`, and a tail that reaches towards
`pollInterval` as the stream's pauses lengthen.

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

### A short interval does not take capacity away from real work

§1 prices the idle queries in CPU. That is not the same as them competing with delivery, so the
ceiling itself was measured at both ends of the interval range, in both outbox states
(`./gradlew :tandem-benchmark:pollIntervalCapacityProbe`; 8 workers, 200 000 `DONE` rows, 2 000
unclaimable rows in the blocked state; two replicates per cell, run in reverse order on the second
pass so a drifting host feeds both arms of every comparison):

| outbox | poll | ceiling, per replicate | mean |
|---|---:|---:|---:|
| drained | 100 ms | 1350 / 1550 | 1450 |
| drained | 10 ms | 1550 / 1550 | 1550 |
| blocked | 100 ms | 1100 / 1100 | 1100 |
| blocked | 10 ms | 1100 / 1250 | 1175 |

**In both states the 10 ms arm sustained slightly *more*, not less** — about 7%, which is smaller
than the spread between replicates of the same cell (up to 13.8%). The honest reading is that this
host cannot tell the two intervals apart; what it rules out is the effect being large. The threefold
idle cost a blocked outbox adds to each claim does not come back as a lower ceiling.

**What does cost capacity is the blocked outbox itself.** At either interval it sustains 1100–1175
against 1450–1550 drained — roughly a fifth to a quarter of the ceiling, with the two ranges not
overlapping. Which is §1's conclusion reached from the other side: the thing to watch is stuck rows,
not the poll interval. A poisoned aggregate costs throughput as well as CPU, and it costs it whether
you poll ten times a second or once.

All eight searches bracketed their ceiling, so none of these figures is a lower bound set by the
search budget. Developer Mac, Docker in a VM: read the ratios, not the rates.

---

## 3. The procedure

**Step 1 — size the workers for throughput, and leave headroom.** Start at the default
(`availableProcessors() × 2`, minimum 1) and raise it only if the relay cannot keep up with the
offered rate. Aim to sit at roughly half the rate the relay can sustain: the slack is what drains the
backlog after a burst, a restart, or a Kafka hiccup, and it costs nothing while unused. Do not tune
this knob for latency in either direction — §2 shows it does not move the median.

**Step 2 — start from the defaults, and move the floor before the ceiling.** The default pair (10 ms
floor, 100 ms ceiling) already puts the median of a live stream in the low tens of milliseconds at the
idle cost of a 100 ms interval, which is why it is the default. Reach for a knob only when a budget
says so:

| What the budget is about | Knob | Worked values |
|---|---|---|
| Latency of a row written into a stream that is already flowing (the common case) | `pollIntervalFloor` | `p50 ≈ floor / 2 + service`. 10 ms (default) for a ~15 ms median; 4 ms is around the service floor on a small VM and nothing below it buys anything |
| Latency of the first row after a bucket has been quiet for a while, and the whole tail | `pollInterval` | `p99 ≈ 1.4 to 2 × interval` for that row. 100 ms (default) for a ~200 ms worst case; 20 ms for ~40 ms |
| What a relay costs while there is nothing to deliver | `pollInterval` | `instances × workers / pollInterval` queries/s; raise it to 500 ms or 1 s on a fleet that is mostly idle, at the price of the row above |
| How much a short pause in the stream costs | `pollBackoffFactor` | 2.0 (default); 1.5 climbs more gently, so a bucket that pauses for a beat is re-checked sooner, for more queries in every pause |
| Single-digit milliseconds, for a row after silence | none | not reachable by polling — see §5 |

**Step 3 — price what you just bought.** Compute `instances × workers / pollInterval` for the quiet
state, and `instances × workers / pollIntervalFloor` for the ceiling it can reach while a stream runs.
Then price it at roughly a third of a core per thousand queries/s, or a full core per thousand if the outbox carries
stuck rows (§1). At the default sizing that is ~4% of one core and not worth a thought; at 800
queries/s it is a quarter of a core on a clean outbox and two thirds on a blocked one. The load scales
with instance count, so a figure that is fine on one relay is 10× that at ten — that multiplication,
not the single-relay number, is usually what makes this matter.

**Step 4 — verify on your own hardware.** §6.

---

## 4. Starting points

| Profile | `workersPerInstance` | floor → ceiling | Idle load |
|---|---|---|---|
| Single embedded relay, ordinary integration events | default (`cores × 2`) | 10 ms → 100 ms (defaults) | ~80/s |
| Single relay, latency-sensitive consumer | default | 10 ms → 20 ms | ~400/s |
| Mostly-idle outbox, occasional events that matter when they come | default | 10 ms → 20 ms | ~400/s |
| Mostly-idle outbox, latency matters only under load | default | 10 ms → 1 s | ~8/s |
| `LEASE`, several instances, ordinary events | default | defaults | 80/s × instances |
| `LEASE`, many instances, mostly idle | default | 10 ms → 500 ms | 16/s × instances |
| High sustained throughput, latency secondary | default or higher | defaults | ~80/s |

The two mostly-idle rows are the choice the ceiling now makes explicit: **a bucket that goes quiet is
either cheap or fast on its first row back, and you pick which.** Under the defaults an idle fleet
pays 80 queries/s per instance for a ~100 ms cold row; at a 1 s ceiling it pays 8 for a cold row of up
to a second, and nothing changes for any row that arrives while the stream is already flowing. That
choice is what the opt-in post-commit wakeup removes on PostgreSQL: with
`tandem.outbox.wakeup: pg-notify` the cold row is signalled rather than waited for, so a long ceiling
costs nothing but the idle load it saves ([dispatch-latency.md](dispatch-latency.md) §3.4).

Note also what is *not* in the table any more: trading workers away to afford a shorter interval. It
worked (8 workers at 100 ms and 2 at 25 ms cost the same 80 queries/s, §2), but it bought latency the
floor now gives for free, and it multiplied the cost of a blocked outbox, since a claim over a wider
bucket slice scans proportionally more unclaimable rows. Keep the workers sized for throughput.

---

## 5. Floors, and what tuning will not fix

**The service floor.** Under the poll term there is irreducible work: the claim round trip, the
CloudEvents encode, the Kafka publish with `acks=all`, and the consumer's own fetch. It measured
~2-7 ms on a small cloud VM and ~4-10 ms on Docker-in-a-VM. A poll interval far below it buys nothing
but query load.

**The host tail.** On a saturated or shared machine the p99.9 carries tens of milliseconds that are
neither poll nor service — GC, CPU contention, a broker pause. Lowering the poll interval translates
the whole distribution down; it does not remove that tail.

**The cold row.** Shortening the floor does nothing for a row that arrives into a bucket which has
been quiet long enough for its worker to reach the ceiling: that one waits the ceiling, and no amount
of tuning makes it both fast and cheap. The post-commit wakeup is what does, on PostgreSQL and on
request: `tandem.outbox.wakeup: pg-notify` on both sides, after which the ceiling only bounds what
happens when a signal does not arrive. Measured at 2 events/s on the reference host, the cold row went
from a 57 ms median and a 118 ms p99 to **5.5 ms and 11.5 ms**, for about a tenth of a millisecond
added to a 1.3 ms write transaction and no throughput cost the host could resolve
([benchmark-results/2026-08-30-wakeup](benchmark-results/2026-08-30-wakeup/); a developer Mac agrees
on the ratios). The design, and what it costs the write path, is in
[dispatch-latency.md](dispatch-latency.md) §3.4.

---

## 6. Measuring it yourself

The load-test harness takes all three knobs, so the tables in §2 can be reproduced against your own
database and broker rather than trusted:

```bash
./gradlew :tandem-benchmark:loadTest --args="--demo --duration=90 --workers=4 --poll-interval=25 --poll-floor=5 S2"
```

Passing `--poll-floor=` equal to `--poll-interval=` measures a fixed interval, which is how the
adaptive backoff is compared against the timing every run archived before it used.

S2 reports COMMIT→ack p50/p95/p99/p99.9 at a held load, and the run's first line records the sizing it
used — quote a percentile only together with that line.

The cost side has its own probe, which holds the outbox at each of the three states in §1 and reports
the query rate each sizing actually produces alongside what it costs PostgreSQL:

```bash
./gradlew :tandem-benchmark:idlePollCostProbe --args="--seconds=60"
```

Both require Docker ([LLD-benchmark.md](LLD-benchmark.md) §9).

---

## 7. Side effects worth knowing before you turn the knobs down

- **`pollInterval` is also the first retry delay after a failing cycle**, growing exponentially to
  `reclaimInterval` (default 5 s). It is deliberately anchored on the ceiling and not on the floor,
  so lowering the floor does not make a relay retry a database that is down ten times faster; that
  only happens if you lower `pollInterval` itself.
- **Keep `workersPerInstance ≤ bucketCount`.** Buckets are assigned as `bucket % workersPerInstance`;
  beyond the bucket count, the surplus workers own nothing and poll forever to find it.
- **Under `LEASE`, the split is over the buckets the instance currently owns**, not all of them, so
  worker slices are less even than the arithmetic suggests. Prefer a worker count that divides the
  bucket count.
- **Both knobs are read at relay startup.** Changing either needs a restart — unlike pause/resume,
  which the Admin API applies at runtime.
