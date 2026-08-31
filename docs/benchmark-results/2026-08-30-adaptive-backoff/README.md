# Adaptive idle backoff: what the floor is worth, and what it costs

The relay's idle wait stopped being a constant. It now starts at `pollIntervalFloor` (10 ms) after
every claim that returned rows and climbs by `pollBackoffFactor` (2.0) to `pollInterval` (100 ms)
while claims keep coming back empty ([LLD-jdbc §3.1](../../LLD-jdbc.md)). These runs measure both halves
of that change on one host: the latency it buys, and the idle load it does not spend.

## Configuration

Two arms of S2 (COMMIT→ack, HdrHistogram), two replicates each, the second pass in reverse order so a
drifting host feeds both arms. Every run at the same offered rate of 400 events/s, 8 workers, 256
buckets, 1 KB payloads. `--poll-floor=` equal to `--poll-interval=` is the fixed timing every run
archived before this one used, so it is the control arm rather than a separate configuration.

```
--demo --duration=90 --workers=8 --poll-interval=100 --poll-floor=100 S2   # control: fixed 100 ms
--demo --duration=90 --workers=8 --poll-interval=100 --poll-floor=10  S2   # the shipped default
```

The cost side is the same probe as
[2026-08-30-poll-interval](../2026-08-30-poll-interval/), re-run under the new default with a
shortened window: `idlePollCostProbe --seconds=25`.

## Results

**Latency, milliseconds, per replicate:**

| idle backoff | p50 | p95 | p99 | p99.9 |
|---|---:|---:|---:|---:|
| fixed 100 ms | 64.5 / 61.8 | 153 / 134 | 309 / 203 | 1358 / 283 |
| 10 ms floor → 100 ms ceiling | 12.8 / 13.1 / 12.2 | 42 / 42 / 40 | 61 / 59 / 57 | 96 / 93 / 92 |

The adaptive arm's third replicate was run after the timing decision moved out of the worker loop and
into `PollBackoff` (a behaviour-preserving refactor), to check that it really was one.

Zero ordering violations and zero lost events in all four runs. On the clean replicate pair (both run
with nothing else on the machine) that is **4.7× at the median and 3.4× at the p99**.

Read the second replicate, not the first: replicate 1's control arm overlapped a Gradle test run on
the same laptop, which is where its p99.9 of 1358 ms comes from. It is kept because the median
survived it (64.5 against 61.8) and because the contamination worked *against* the adaptive arm,
which ran second.

**The adaptive arm also reproduces better than the control**: 12.8 against 13.1 at the median, where
the control's p99.9 moved by a factor of five between replicates. Once most rows stop waiting on a
100 ms sleep, there is less for the host's own jitter to ride on.

**It does not reach a fixed 10 ms interval, and is not meant to.** That cell measured p50 9.3 and
p99 23 on this host ([relay-sizing §2](../../relay-sizing.md)), against 13 and 60 here: inside a
stream's short pauses the backoff has already climbed a step or two, so the ceiling still shows in
the tail. What it buys against that cell is the idle load, 80 queries/s rather than 652.

**Idle query load, under the new default (`--seconds=25`):**

| cell | expected q/s | measured q/s | PostgreSQL CPU over baseline, empty / drained / blocked |
|---|---:|---:|---:|
| 8 workers @ 100 ms | 80.0 | 75.4 | 2.9% / 3.4% / 10.4% |
| 8 workers @ 20 ms | 400.0 | 313.9 | 9.8% / 12.9% / 37.3% |
| 8 workers @ 10 ms | 800.0 | 652.9 | 17.0% / 21.5% / 66.8% |
| 2 workers @ 25 ms | 80.0 | 75.8 | 2.9% / 3.5% / 19.8% |

**Unchanged, which is the point.** The same probe measured 78 q/s and 3.5% at the 100 ms cell before
the adaptive backoff existed. A worker with nothing to find reaches the ceiling within four empty
claims and stays there, so `instances × workers / pollInterval` still prices an idle relay, and the
floor is paid only while there is something to deliver.

## Files

| File | What it is |
|---|---|
| `s2-fixed-rep1.log`, `s2-fixed-rep2.log` | control arm: floor equal to the 100 ms ceiling |
| `s2-adaptive-rep1.log`, `s2-adaptive-rep2.log`, `s2-adaptive-rep3.log` | the shipped default: 10 ms floor, 100 ms ceiling |
| `idle-poll-cost.log` | the cost probe under the new default, three outbox states |

Each log is the run's own output with Gradle's task lines and the per-row `FINE` chatter removed; the
sizing line, every ramp hold and the PASS line are intact.

Developer Mac with Docker in a VM, not the reference host
([HLD-load-testing §5](../../HLD-load-testing.md)): read the ratios between arms, never the rates. **The
figures published on the site and in the earlier archives were all measured at the fixed 100 ms
interval** and therefore describe the pre-adaptive default; re-measuring them belongs on the
reference host, not here.
