# Is the post-commit wakeup worth turning on? S10 on two hosts, plus a ceiling A/B

The wakeup ([dispatch-latency §3.4](../../dispatch-latency.md)) removes the one case the adaptive idle
backoff cannot price: a row arriving into a bucket whose worker has gone quiet and is waiting at
`pollInterval`. It is not free on the way in, because the write side runs one extra statement inside
the caller's transaction. This run measures both sides of that trade.

Run on **both hosts**: the developer Mac (Docker in a VM) and the `m7i-flex.large` reference host
(2 vCPU, native Docker, [the host section](../README.md#the-host)). They agree on the finding and
disagree on the absolute milliseconds, which is what a laptop and a small cloud VM should do.

## Configuration

`./gradlew :tandem-benchmark:loadTest --args="S10"`, harness
defaults: 8 workers, batch 100, 256 buckets, 1 KB payloads, `pollInterval` 100 ms, `pollIntervalFloor`
10 ms, `acks=all`, `enable.idempotence=true`.

S10 holds **2 events/s**, low enough that every event arrives into a slice that has been silent for
seconds, and runs four windows of 2.5 minutes in the order **poll, wakeup, wakeup, poll**, so a laptop
drifting over the twelve minutes feeds both arms rather than one. Each window builds its own relay
instance and its own write side; one consumer and one recorder span the run.

## Results

Developer Mac (`s10-mac.log`):

| window | arm | commit→ack p50 | p95 | p99 | write txn p50 | write txn p99 | events |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | poll | 68.0 ms | 116.1 | 126.7 | 4.13 ms | 6.15 | 302 |
| 2 | wakeup | 8.2 ms | 10.3 | 11.2 | 4.64 ms | 6.86 | 301 |
| 3 | wakeup | 7.2 ms | 8.3 | 9.4 | 4.14 ms | 6.10 | 301 |
| 4 | poll | 59.9 ms | 113.2 | 118.2 | 3.59 ms | 5.79 | 301 |

Reference host (`s10-ec2.log`):

| window | arm | commit→ack p50 | p95 | p99 | write txn p50 | write txn p99 | events |
|---:|---|---:|---:|---:|---:|---:|---:|
| 1 | poll | 58.9 ms | 107.2 | 116.3 | 1.52 ms | 4.46 | 302 |
| 2 | wakeup | 5.9 ms | 8.9 | 12.7 | 1.51 ms | 4.78 | 301 |
| 3 | wakeup | 5.1 ms | 7.6 | 10.2 | 1.33 ms | 3.65 | 301 |
| 4 | poll | 54.1 ms | 106.2 | 119.3 | 1.11 ms | 3.43 | 301 |

Arm means:

| | commit→ack p50 | p95 | p99 | write txn p50 |
|---|---|---|---|---|
| Mac, poll | 64.0 ms | 114.7 | 122.5 | 3.86 ms |
| Mac, wakeup | **7.7 ms (8.3×)** | 9.3 (12×) | 10.3 (12×) | 4.39 ms |
| reference, poll | 56.5 ms | 106.7 | 117.8 | 1.32 ms |
| reference, wakeup | **5.5 ms (10.3×)** | 8.3 (13×) | 11.5 (10×) | 1.42 ms |

Zero ordering violations and zero lost events in all eight windows.

**The discovery win is unambiguous.** The arms do not overlap anywhere: the *worst* percentile measured
in either wakeup window (11.2 ms) is better than the *best* median measured in either poll window
(59.9 ms). And the poll arm behaves exactly as the poll interval predicts it should — a mean of
`pollInterval / 2` plus service, with a p99 near the interval itself — which is what says the two arms
differ by the mechanism and not by the weather.

**The write cost is real in direction and unresolvable in size on either host.** The arm means differ
by 0.53 ms on the Mac (3.86 → 4.39) and by **0.10 ms on the reference host** (1.32 → 1.42), which is
the shape an extra round trip should have and the difference one would expect between a socket inside
a VM and a native one. On both machines, though, the spread *inside* each arm is at least as large as
the gap between them (reference poll: 1.11 and 1.52), and the arms interleave. So what these runs
establish is an **upper bound of about a tenth of a millisecond on a native host**, not a measurement.

The write transaction itself is three times faster on the two-core cloud VM than on the ten-core Mac
(1.3 ms against 3.9 ms). That is the VM `fsync` penalty of Docker on macOS, not a property of either
relay arm, and it is why the wakeup's round trip looks five times more expensive there.

## The capacity question: no cost this host can resolve

Four `--duration=240 S1` ceiling searches on the reference host, alternating **none, wakeup, wakeup,
none** so a drifting instance feeds both arms — the same protocol as
[2026-08-30-ec2-s1-ab](../2026-08-30-ec2-s1-ab/README.md):

| order | arm | ceiling | bracketed below |
|---|---|---:|---:|
| 1 | poll | 1300 | 1350 |
| 2 | wakeup | *(no bracket)* | — |
| 3 | wakeup | 1300 | 1400 |
| 4 | poll | 1350 | 1400 |

**The poll arm disagrees with itself by 4% (1300 and 1350), and the wakeup arm's one valid reading
sits inside that.** The verdict is the same one the adaptive-backoff A/B reached on this machine: no
capacity cost this host can resolve. What it rules out is a large effect; it cannot rule out one of a
few per cent, which is roughly what a 0.1 ms addition to a 1.3 ms write transaction would predict.

Search 2 produced no ceiling at all: the ramp reported `NOT A MAXIMUM — 12800 events/s is only a lower
bound`. That is a known shape of the ramp's gate rather than a result — it reads lag *flatness*, and a
write path that self-limits under load keeps lag flat while the offered rate runs away from what is
actually landing. It happened once in four searches, in an arm whose other search bracketed normally,
so it is noise in the instrument, not a finding about the wakeup. The log is kept.

**On the Mac this measurement is impossible**, which is worth recording because it looks like a
failure and is not one. Both arms there reported `NOT A MAXIMUM` at the same 12800 lower bound: the
load generator is bounded by its connection pool (32 connections at ~4 ms per write transaction is
about 8 000 inserts/s at most) and the relay on a ten-core machine keeps up with everything the
generator can offer, so the search runs out of load before the relay runs out of capacity. It brackets
on the reference host precisely because that host's ceiling (~1300/s) is well below the generator's
own limit.

## What it costs the database: about half as many claims again

S10 and the ceiling searches say nothing about how hard the relay actually queries, because **nothing
in the product counts a claim** — the per-cycle log line only fires when a claim returns rows, and
there is no metric. So the count was taken from PostgreSQL's own counters instead, over two
six-minute S9 runs at 400 events/s under `LEASE` (2 instances × 8 workers, 16 buckets per worker),
identical but for the wakeup (`s9-claims-poll.log`, `s9-claims-wakeup.log`, sampled every 30 s into
`claims-poll.tsv` / `claims-wakeup.tsv` by [`sample-claims.sh`](../sample-claims.sh)):

| | claims/s | per delivered event | transactions/s |
|---|---:|---:|---:|
| poll | **~820** | 2.0 | 2 365 |
| wakeup | **~1 290** | 3.2 | 3 543 |

Per worker that is one claim every ~20 ms without the wakeup and every ~12 ms with it, which means
the wakeup arm sits essentially pinned to its 10 ms floor. **Three independent counters agree on the
ratio**: dispatch-index scans +57%, aggregate-index scans +43%, committed transactions +50%.

Roughly half of that increase is the `LEASE` broadcast: `LISTEN` delivers every notification to every
instance, so with two instances about half the wakes are for buckets the woken worker does not own,
and it finds nothing (LLD-jdbc §3.10). Neither arm lost throughput here — both held 400/s with a p50
of 17-21 ms — but the extra database work is real and grows with the instance count.

**How the claim count was derived, and its one assumption.** The claim's plan is an Index Scan with
`bucket = ANY(array)`, a `ScalarArrayOpExpr`, so PostgreSQL descends the index once per array element
and `idx_scan` advances by one per bucket in the worker's slice. Dividing the dispatch-index rate by
16 gives the claim rate. The ratio between the arms does not depend on that divisor; the absolute
number does.

## A second finding: the dispatch index stops being used once the table fills

In **both** arms the `idx_tandem_outbox_dispatch` counter climbs at ~13 000/s and then freezes
outright (six increments in two minutes) while the aggregate index's rate triples. It happens around
200 MB / 200k rows, in both runs, which is autoanalyze updating the statistics and the planner
abandoning the partial index that exists specifically to drive the poll, in favour of driving from
the anti-join side.

Correctness is unaffected and this is not a wakeup behaviour: it is a property of the claim under a
full table that had never been observed, precisely because nothing counts claims. The `EXPLAIN` of
the post-fill plan is not captured here — the container was gone by the time it was asked for — so
what is archived is the counter evidence, not the plan itself.

## What this does not say

- **Nothing about a busy stream.** At any rate high enough to keep a worker's slice warm the backoff
  already sits at its floor, and there is nothing for a signal to remove. S10 is deliberately the
  opposite shape.
- **Nothing about capacity, from S10 itself.** The extra statement's effect on the ceiling is a
  saturated-host question and S10 offers 2 events/s; that is what the ceiling searches above are for.
- **Not a KPI.** Neither host meets the reference baseline of
  [HLD-load-testing §5.1](../../HLD-load-testing.md), and `m7i-flex.large` is burstable, so its
  ceilings are indicative ([the host section](../README.md)). The ratios are the result here; the
  absolute milliseconds belong to the machines they were taken on.

## Files

| File | What it is |
|---|---|
| `s10-mac.log`, `s10-ec2.log` | The harness's stdout for each S10 run, filtered as [the archive README](../README.md) describes |
| `s1-ab-1-none.log` … `s1-ab-4-none.log` | The four alternating ceiling searches on the reference host, in the order they ran |
| `s1-ab-mac.log` | The Mac's two attempts, kept as the evidence that the ceiling is not measurable there |
| `s9-claims-poll.log`, `s9-claims-wakeup.log` | The two six-minute S9 runs the claim count was taken from |
| `claims-poll.tsv`, `claims-wakeup.tsv` | PostgreSQL's index-scan and transaction counters, sampled every 30 s through each of those runs |
