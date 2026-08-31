# Crisis recovery: what an outage backlog costs, and where recovery stops being possible

Throwaway measurement, 2026-08-31, on the Mac (not the reference host, so absolute thresholds are
indicative; the mechanism and the shape of the curve are not host-specific). Environment: S9 at 400
events/s, `LEASE` 2 instances × 8 workers, 256 buckets, 1 KB payloads, retention 10 min, cleanup every
30 s, `--wakeup=pg-notify`.

## Method

The backlog is built from the generator's **own** rows, so the correctness ledger stays consistent and
per-aggregate ordering keeps being verified. A `BEFORE INSERT` trigger pushes `next_attempt_at` an
hour ahead, so every row lands pending and unclaimable, sitting in the partial dispatch index where
every claim scans it. Dropping the trigger and one `UPDATE ... SET next_attempt_at = NULL` releases
the whole backlog at once, which is t0 of the recovery.

**A first design failed and is worth recording**: blocking rows with a once-a-second `UPDATE ... WHERE
status = 0` races the relay and loses. The relay delivers within milliseconds, so each pass caught
only the handful of rows pending at that instant: 639 rows in 60 seconds instead of 24 000. The
trigger has no race because it acts at write time.

auto_explain was armed before HikariCP built its pool (see `capture-claim-plan.sh` for why that race
matters) and the container log was dumped to disk every 60 s, because the container is destroyed the
instant the run ends.

## Result 1: recovery is fine from a short outage and impossible from a longer one

| outage | backlog | recovery | drain rate |
|---|---:|---|---:|
| 60 s | 24 295 rows | **9 s** | 2 429/s average |
| 250 s | 100 113 rows | **never; it diverges** | delivery falls below the offered rate |

After the 100 k release the backlog **grew**: 100 113 at t0, 145 398 after 5 min, 201 374 after 10 min,
oldest row 612 s old and climbing. The harness's own windows show why: delivered throughput collapsed
from 399.4/s to 206.9/s and 249.2/s while writes stayed at 400/s. The relay stops keeping up with a
load it carries comfortably when the outbox is clean.

The 24 k drain curve **accelerates as it empties** (≈1 320/s in the first seconds, ≈4 940/s at the
end), which is the same effect seen from the other side: per-row cost falls as the backlog shrinks.

## Result 2: the claim's cost is wildly superlinear in the backlog

Measured from the relay's own executions (auto_explain, not a hand-typed EXPLAIN):

| backlog | claim cost (mean) | vs plateau |
|---|---:|---:|
| plateau, ~5 pending | 0.291 ms | 1x |
| 24 000 | 7.449 ms | 26x |
| 200 000 | **2 271 ms** | **7 800x** |

At 200 k only the degraded plan runs, and it scans **234 596 rows per claim** and discards nearly all
of them. With 16 workers at 2.3 s per claim returning at most 100 rows each, the ceiling is ~700
rows/s; the harness measured 230-250/s delivered. That is the feedback loop: the backlog slows the
claim, the slow claim slows delivery, slow delivery grows the backlog.

At the 24 k backlog both plans were still running, and the gap between them widened from 1.25x at the
plateau to 2.25x (3.318 ms for dispatch+aggregate against 7.449 ms for aggregate-only).

## What this means for backlog item 9b

**It is not hygiene and not a micro-optimisation.** The plan the planner picks under backlog demotes
the bucket predicate to a `Filter`, so every claim scans the whole pending set instead of its own
1/16th bucket slice, and the anti-join degrades to a quadratic `Join Filter`. The dispatch plan, with
`Index Cond: bucket = ANY(...)`, would be bounded by the slice. **The planner abandons the bounded
plan exactly when boundedness is what matters.** The item should be re-prioritised accordingly.

## What is not measured yet

- **Where the threshold is.** 24 k recovers, 100 k does not. The number an operator would want is
  between the two, and it needs a bisection on a clean environment.
- **Whether shedding load recovers it.** Left running to the end of the run, where the generator stops
  and writes go to zero.
- **Anything on the reference host.** These are Mac numbers.

## Correction: the instrument contaminated windows 6-8

At ~12:07 `auto_explain.sample_rate` was raised to 1.0 and the container log reached 1.55 GB while a
dumper re-read it every 60 s. `log_analyze = on` at full sampling instruments **every** execution
inside PostgreSQL, so windows 6, 7 and 8 (12:15, 12:25, 12:35) are contaminated. Windows 1-5 are
clean. Measured cost of the contamination: the drain ran at 46 rows/s with it and **60 rows/s**
without, about 23%.

**A conclusion drawn from the contaminated windows is withdrawn.** Degradation was reported as
accelerating beyond the hyperbolic law; with the instrument removed the simple law holds throughout.

## Result 3: the law, and the drain with writes stopped

Delivery × backlog is approximately constant:

| point | backlog | delivery | product |
|---|---:|---:|---:|
| window 4 (clean) | 313 k | 221.7/s | 69.4 |
| window 5 (clean) | 460 k | 155.2/s | 71.3 |
| drain, zero writes (clean) | 1 041 k | 60/s | 62.5 |

The last row is the important one: it holds even with the generator stopped, so the cost is a property
of the backlog, not of concurrent write load.

Extrapolating the drain from a 1.04 M backlog with zero writes, `t = N² / 2K`:

| remaining backlog | time to clear it |
|---|---|
| 1 040 000 | ~143 min |
| 500 000 | ~33 min |
| 200 000 | ~5 min |
| 100 000 | ~1.3 min |

**Recovery is self-accelerating but almost all the time is spent at the start.** The last 100 k drains
in eighty seconds; the first 500 k takes nearly two hours.

Operationally: shedding write load is necessary and not sufficient. What matters is intervening while
the backlog is still tens of thousands of rows, where recovery is minutes.

**Not observed directly**: a complete settle. S9 waits `max(5 min, duration/4)` = 20 min for its final
drain, and 1 M rows need hours, so the run tears down first. A dedicated run should size the backlog
so the drain fits inside that allowance (~100 k drains in ~100 s).

## Result 4, and a correction to Results 1-3: the cause is chain density, not the plan

Measured at the 1 M backlog, before teardown:

    pending_rows  distinct_aggregates  rows_per_chain
       1 028 401                1 024         1 004.3

The head-of-chain gate admits **one claimable row per aggregate**, so at most 1 024 rows in the whole
outbox are claimable: a density of one in a thousand. Any plan must sift ~1 000 rows to find one it
can take.

**Forcing the plan changes nothing.** The same query with `enable_bitmapscan = off` ran 16.8 s against
16.1 s, and at this backlog the planner picks a third plan again (`tandem_outbox_pkey`, 903 777 rows
removed by filter). The index choice is not the lever.

**This is the mechanism behind the hyperbolic law.** At a fixed aggregate cardinality the density of
claimable rows falls as 1/backlog, so cost per delivered row rises proportionally, so
delivery × backlog is constant. Results 1-3 measured the effect correctly; their attribution to the
plan choice (item 9b) was wrong, and is withdrawn here. Item 9b is real and separate: it is about
which plan is picked, not about why the claim gets expensive under backlog.

**A fix direction the data suggests.** `idx_tandem_outbox_aggregate` is `(aggregate_id, id) WHERE
status IN (0,1,3)`, which is exactly the structure needed to find each chain's head with a
`DISTINCT ON (aggregate_id)` skip scan: ~1 024 index probes instead of a million-row scan. The claim's
cost would then scale with the number of aggregates, which is bounded, rather than with the backlog,
which is not. Not implemented, not benchmarked, but motivated by a measurement.
