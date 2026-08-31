# S9 endurance with the post-commit wakeup: six hours at 400 events/s

The same six hours as [`2026-08-30-endurance`](../2026-08-30-endurance/), on the same host, at the same
offered rate, with the same cleanup settings. One flag differs: `--wakeup=pg-notify`. **Verdict: PASS,
18/18 windows.**

## Configuration

400 events/s fixed (never ramped), 2 relay instances × 8 workers, 256 buckets, 1 KB payloads,
retention 10 min, cleanup every 30 s in batches of 20 000, 52-connection pool. ~8.17M events written,
7.93M deleted by cleanup. 2026-08-30 22:11 to 2026-08-31 04:12.

```
--duration=21600 --window=1200 --rate=400 --connections=52
--retention=600 --cleanup-interval=30 --cleanup-batch=20000 --wakeup=pg-notify S9
```

The pool is 52 rather than the baseline's 48 because each relay instance holds a permanent `LISTEN`
connection outside the working set.

## Results: nothing drifted, and the latency is a third of the baseline's

| | first window | last window | over the run |
|---|---|---|---|
| Throughput | 395.2/s | 400.0/s | offered 400/s, no slope |
| Latency p50 | 21 ms | 12 ms | 12-14 ms from window 3 on |
| Latency p99 | 211 ms | 42 ms | band 39-51 ms from window 2 on |
| `tandem_outbox` | 445 MB | 450 MB | plateau ~245k rows |
| Dead tuples | 6k | 80k | sawtooth 78-92k; autovacuum kept up (341 passes) |
| Bucket coverage | 256/256 | 256/256 | complete and disjoint in every window |

Zero ordering violations, zero lost events, zero duplicates. Threads flat at 51-52 (the baseline's
49-50 plus the two `LISTEN` connections).

Window 1 is startup and is the only outlier in either arm, exactly as in the baseline.

### Against the poll baseline

| | poll (baseline) | wakeup (this run) | |
|---|---|---|---|
| Throughput | 395.7 → 399.6/s | 395.2 → 400.0/s | unchanged |
| p50 | flat 58-60 ms | flat 12-14 ms | **~4.7x** |
| p99 | band 156-173 ms | band 39-47 ms | **~3.8x** |
| Cleanup deadlocks | 20 | **0** | see below |
| Host `CPU_Speed_Limit` | median 74%, min 55% | median 60%, min 41% | this run ran *more* throttled |

**The host strengthens the comparison rather than weakening it.** `CPU_Speed_Limit` was sampled every
minute (`host-samples.tsv`, 348 samples): median 60%, minimum 41%, hourly means climbing gently from
57.6% at 22:00 to 62.1% at 03:00 as the machine freed up overnight. The baseline night sat at a median
of 74%. The wakeup arm therefore delivered its latency on a *more* throttled machine, and the gentle
upward drift in host speed is monotonic while the latency is flat, so the two are not tracking each
other.

**Heap must be read as a series, not as endpoints.** The summary line reports 47 MB → 58 MB. The
eighteen samples are a sawtooth with no trend: 47, 54, 34, 41, 60, 33, 39, 52, 30, 65, 63, 52, 59, 32,
49, 50, 64, 58.

## The cleanup deadlock fix holds

The baseline run found concurrent `cleanup` passes deadlocking each other, 20 occurrences, and the fix
(`FOR UPDATE SKIP LOCKED` in the cleanup subselect, LLD-jdbc §3.7) shipped before this run. Six hours
with two instances deleting 7.93M rows produced **`deadlocks = 0` and `xact_rollback = 0`** in
`pg_stat_database` (`final-db-snapshot.txt`), and zero `SEVERE` and zero `WARNING` in the whole log.

## What this run says about the claim query (backlog item 9b)

Item 9b holds that the planner abandons the partial dispatch index once the table fills. The run
measured the relay directly, through PostgreSQL's index counters sampled every 5 minutes
(`claims.tsv`), and the picture is more specific than that.

**The relay's plan choice oscillates, binary, roughly every 5 to 10 minutes.**
`idx_tandem_outbox_dispatch` runs at about 9 700 scans/s or at 0/s and almost never in between: of 71
sampled intervals, 23 are near zero and 21 are above 5 000/s. `idx_tandem_outbox_aggregate` is
complementary, 1 250/s when dispatch is active and 1 850/s when it is not.

**The real work does not change with the regime.** `xact_commit` is flat at ~3 700/s across both, and
throughput and latency are indifferent to which one is running (window 2, mostly aggregate: p50 16,
p99 51; window 3, mostly dispatch: p50 14, p99 45).

**The two plans are not equally cheap in tuples read**, from the run totals: the aggregate index
served 33.4M scans reading 2.44 **billion** index tuples (73 per scan), the dispatch index 70.1M scans
reading 25.8M tuples (0.37 per scan), about 200x apart. It cost no latency here because the buffer hit
ratio was 97%, but that margin is a property of this working set fitting in shared buffers, not of the
plan.

### A methodological trap, and the reason the four EXPLAIN captures prove less than they appear to

Four `EXPLAIN` captures were taken while the container was alive (it is destroyed the moment the run
ends). They show the plan changing between the fill phase and the plateau, and between two shapes at
the plateau, including one where the anti join degrades to a `Join Filter` cross product that is
quadratic in the pending count (`explain-3-late.txt`).

**None of them describes the plan the relay executes.** Capture #4 was deliberately timed, by probing
the counter for 30 s first, to land inside a phase where the relay was measurably scanning the
dispatch index at 1 772 scans/s. In that same second, a fresh `psql` plan for the identical query text
still chose the aggregate index (`explain-4-dispatch-regime.txt`). The likely cause is
parameterization: the relay's prepared statement takes the bucket array as a parameter and gets a
generic plan, while these captures hardcode the literal array and hand the planner exact selectivity.

So the quadratic shape is real but belongs to the literal-array plan, and nothing about the relay's
plan may be inferred from any of the four. **Settling item 9b needs a different instrument**:
`auto_explain` server-side, or `EXPLAIN EXECUTE` against a prepared statement mirroring the relay's.
Neither is doable on a live run.

### A note on `sample-claims.sh`

Claims/s derived from `idx_scan` are comparable **only at equal plan**. The dispatch plan is a
`ScalarArrayOpExpr` scan counting 16 per claim, one per owned bucket; the aggregate plan counts 1.
That is why total index scans move by 6x between regimes while `xact_commit` does not. The script
should record the active plan next to the counter. This does not affect the poll-vs-wakeup claim
measurement in [`2026-08-30-wakeup`](../2026-08-30-wakeup/), taken on a cold outbox where both arms
ran the dispatch plan.

## Two structural costs the totals exposed

Neither is a regression and neither was previously quantified (`final-db-snapshot.txt`):

- **No HOT updates, ever.** `n_tup_upd = 16 342 754`, exactly twice the inserts (both status
  transitions), with `n_tup_hot_upd = 0`. HOT is impossible by construction here, because both updates
  change `status`, which appears in the predicates of four partial indexes. Every row therefore pays
  two full rewrites of all applicable index entries.
- **The plateau's disk constraint is Kafka, not the outbox.** The outbox held at ~450 MB throughout,
  while free disk fell from 33 GB to about 21 GB at roughly 2 GB/hour: the broker's log of the 8.17M
  delivered messages. Any longer run should size the disk against Kafka retention, not against
  `rate × row_size × duration`.

## Files

| File | What it is |
|---|---|
| `s9-endurance-wakeup.log` | the harness's own stdout: the eighteen window lines and the verdict |
| `host-samples.tsv` | per-minute `CPU_Speed_Limit`, load average, and per-container CPU/memory/IO |
| `claims.tsv` | per-5-minute index scan and `xact_commit` counters (`../sample-claims.sh`) |
| `vacuum.tsv` | per-minute autovacuum/autoanalyze counts and both index sizes (`../sample-vacuum.sh`) |
| `container-map.tsv` | which container name is PostgreSQL and which is Kafka |
| `explain-1-early.txt` | claim plan while the table was filling (10k rows) |
| `explain-2-plateau.txt` | claim plan at the plateau, plus index definitions, sizes and status counts |
| `explain-3-late.txt` | claim plan late at night, aggregate regime, with `ANALYZE, BUFFERS` |
| `explain-4-dispatch-regime.txt` | claim plan captured inside a measured dispatch regime: the negative result above |
| `final-db-snapshot.txt` | index and table statistics taken at 03:52, before the container was destroyed |
| `operator-log.md` | every intervention with its exact time, including which claim samples the EXPLAINs perturbed |

Absolute numbers are indicative, never KPIs: this is not the reference host
([HLD-load-testing §5](../../HLD-load-testing.md)). The run's value is in the *slope*, and there was
none.
