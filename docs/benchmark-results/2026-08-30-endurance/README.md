# S9 endurance — six hours at 400 events/s

A moderate load held for six hours under `LEASE` coordination, sliced into eighteen twenty-minute
reporting windows, with DONE-row cleanup deleting throughout. **Verdict: PASS, 18/18 windows.**

## Configuration

400 events/s fixed (never ramped), 2 relay instances × 8 workers, 256 buckets, 1 KB payloads,
retention 10 min, cleanup every 30 s in batches of 20 000, 48-connection pool. ~8.6M events.

```
--duration=21600 --window=1200 --rate=400 --connections=48
--retention=600 --cleanup-interval=30 --cleanup-batch=20000 S9
```

## Results — nothing drifted

| | first window | last window | over the run |
|---|---|---|---|
| Throughput | 395.7/s | 399.6/s | offered 400/s, no slope |
| Latency p50 | 58 ms | 59 ms | flat 58–60 ms |
| Latency p99 | 173 ms | 162 ms | band 156–173 ms, no slope |
| `tandem_outbox` | 420 MB | 439 MB | plateau ~245k rows |
| Dead tuples | 72k | 82k | stable 78–92k; autovacuum kept up |
| Bucket coverage | 256/256 | 256/256 | complete and disjoint in every window |

Zero ordering violations, zero lost events, zero duplicates. Threads flat at 49–50.

**Heap must be read as a series, not as endpoints.** The summary line reports 48 MB → 81 MB, which
reads like a leak; the eighteen samples are a sawtooth between 30 and 94 MB with no trend (30 MB at
window 2, 43 at window 11, 48 at window 15). It is *used* heap sampled at arbitrary points of the GC
cycle.

**The host did not mask the result.** This is an Intel Mac, which reduces clock under sustained load,
so `CPU_Speed_Limit` was sampled every minute (`host-samples.tsv`): median 74%, minimum 55%, hourly
means between 70% and 84% across the whole night — no progressive thermal decline. Flat throughput is
therefore a property of the system under test, not of a machine that never slowed down.

## Why the run was worth doing

Every other scenario measures the first few minutes of a relay's life, and the failure modes an outbox
dies of in production are slow ones. Two things came out of this that a short run cannot produce:

- **A defect in `tandem-jdbc`, now fixed.** Concurrent `cleanup` passes deadlocked each other — 20
  occurrences, ~1.4% of passes. The cleanup subselect took no row locks while the claim beside it used
  `FOR UPDATE SKIP LOCKED`, so two passes over overlapping id windows waited on each other. It is
  reachable only with more than one instance *and* cleanup actually deleting, which the 14-day default
  retention never produces. `postgres-deadlocks.log` holds the database's own report; the fix and its
  regression test are in `tandem-jdbc` (LLD-jdbc §3.7).
- **Confirmation of properties that only time can show.** Bucket coverage stayed complete and disjoint
  across roughly 720 lease renewal cycles; autovacuum kept pace with a churn table for six hours; the
  outbox reached a steady size instead of growing.

## Files

| File | What it is |
|---|---|
| `s9-endurance.log` | the harness's own stdout — the eighteen window lines and the verdict |
| `host-samples.tsv` | per-minute `CPU_Speed_Limit`, load average, and per-container CPU/memory/IO |
| `container-map.tsv` | which container name is PostgreSQL and which is Kafka |
| `plateau-check.log` | table size and row counts during the first fifteen minutes, where cleanup catches up |
| `postgres-deadlocks.log` | PostgreSQL's deadlock reports, with both offending statements |

Absolute numbers are indicative, never KPIs: this is not the reference host
([HLD-load-testing §5](../HLD-load-testing.md)). The run's value is in the *slope*, and there was none.
