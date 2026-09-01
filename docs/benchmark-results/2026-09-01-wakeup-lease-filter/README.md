# What the ownership filter takes off the database, and what it leaves

Under `LEASE` the wakeup notification is a broadcast: every instance is told about every bucket, and
acting on the ones it does not own costs a claim that can only find nothing. The relay now filters
those out (LLD-jdbc §3.10). This measures what that is worth, and what remains after it.

## Configuration

Three S9 arms on a developer Mac, six minutes each at a fixed 400 events/s under `LEASE` (2 instances
× 8 workers, 256 buckets, 1 KB payloads, retention 10 min, cleanup every 30 s, 52-connection pool),
run back to back on one build. Only the relay differs:

| arm | wakeup | ownership filter |
|---|---|---|
| `poll` | off | n/a |
| `filter-off` | on | **disabled in the source for this run** |
| `filter-on` | on | on (what ships) |

The middle arm is the control: without it the poll arm and the wakeup arm differ in more than the
filter, and the filter's effect cannot be separated from the mechanism's.

`xact_commit` is the counter used, sampled every 30 s ([`sample-claims.sh`](../sample-claims.sh)).
Index-scan counters are recorded but **not** used: since the v5 index the claim runs a plan that does
not touch `idx_tandem_outbox_dispatch` at all (its counter reads zero throughout all three arms), so
only the transaction count is comparable across arms. Every claim is one transaction, and the other
contributors — 400 inserts/s, the mark-done batches for the same delivered rows, cleanup, reclaim —
are identical in all three, so the difference between arms is claims.

## Results

| arm | transactions/s | vs poll | COMMIT→ack p50 | p99 |
|---|---:|---:|---:|---:|
| poll | 2 348 | — | 19-20 ms | 99-100 ms |
| wakeup, filter off | 3 257 | **+38.7%** | 12-15 ms | 40-50 ms |
| wakeup, filter on | 2 958 | **+26.0%** | 12-13 ms | 36-65 ms |

**The filter removes 298 transactions/s**, about a third of the wakeup's overhead and 9% of everything
the database was doing, **at no cost in latency**: the two wakeup arms are indistinguishable on both
percentiles. That is the whole case for it — it removes work that could not have produced anything.

**It removes less than the arithmetic suggests, and the reason is worth knowing.** One wasted signal
per event per non-owning instance would be 400/s; the measurement says 298. Wasted wakes coalesce: a
signal arriving while its worker is mid-claim only sets the flag, so several of them collapse into one
wasted claim rather than one each.

**What remains is not waste.** The wakeup still costs 26% more transactions than polling, and that
residual is the claims that *do* find rows: 400 events/s delivered in more, smaller batches, because
each signal starts a claim rather than letting a few rows accumulate. No filter addresses that; a
minimum spacing between wake-driven claims would (dispatch-latency §3.4, item 3), at the price of
turning "discovery immediately" into "discovery within X ms".

So the trade this run measures, for a relay that has turned the wakeup on under `LEASE`: **+26% database
transactions for a 1.6× better median and a 2× better p99**, where before the filter it was +39% for
the same latency.

## The cold row did not regress

`s10-after.log` is S10 on the same build: **p50 64.9 → 7.5 ms (8.7×)**, p99 127.2 → 10.1, against 8.3×
measured before these two changes. This matters because the second change (a wake no longer resets the
worker's backoff) could in principle have slowed the claims that follow a signal. It did not.

## Caveats

- **One run per arm**, sequential rather than interleaved, on a laptop. The poll arm landed at 2 348
  transactions/s against 2 365 measured on a different build two days earlier, which says the
  instrument is stable, but a 9% difference deserves a replicate before it is quoted as a KPI.
- Not the reference host ([HLD-load-testing §5.1](../../HLD-load-testing.md)): the ratios are the
  result, the absolute numbers belong to this machine.

## Files

| File | What it is |
|---|---|
| `s9-poll.log`, `s9-filter-off.log`, `s9-filter-on.log` | the three arms' harness output |
| `claims-*.tsv` | PostgreSQL's counters through each arm, sampled every 30 s |
| `s10-after.log` | the cold-row regression check on the same build |
