# Outage recovery: why a backlog was unrecoverable, and the index that fixed it

Five runs on 2026-08-31, four on a Mac and one on the reference host, all at 400 events/s with
`--wakeup=pg-notify`, retention 10 min, cleanup every 30 s. The question was how an outbox recovers once the relay has been down. The answer
changed three times, and each change came from an instrument, not from a better argument.

**The result.** A 30-minute relay outage left the outbox unable to catch up: delivery collapsed below
the offered rate and the backlog grew without bound. With one added partial index, the same outage
recovers in under seven minutes. The index costs under 1% more WAL per insert.

| outage | backlog | before `v5` | with `v5` |
|---|---:|---|---|
| 15 min | ~358 k rows | never finished; net drain fell to 15 rows/s | **146 s** |
| 30 min | ~713 k rows | **diverging** | **405 s** |

Correctness never wavered in any run at any backlog: zero ordering violations, zero lost, zero
duplicates, throughout.

**Confirmed on the reference host too**, which runs Docker natively rather than inside a VM. A shorter
ladder there (2 vCPU, so the outages were 10 and 20 minutes) recovered 239 871 rows in 65.6 s and
479 328 rows in 139.3 s: **3 656 and 3 440 rows/s, essentially the same rate at twice the backlog**,
which is the property that was missing before. It is faster than the eight-core Mac on two cores,
because there is no VM `fsync` penalty on every commit. That host is burstable, so those times are not
reproducible, and the no-index arm was not run there: this confirms the behaviour on native Docker, it
does not measure the index's effect on that host.

## The mechanism

The claim takes the oldest claimable rows: `... ORDER BY o.id ... LIMIT n`. Ids grow with time, so the
planner can satisfy that order for free by reading `tandem_outbox_pkey` — and it does. That index
carries **every** row, and a busy outbox holds far more delivered rows than pending ones, because
`DONE` rows accumulate for the whole retention window before cleanup removes them. Under a backlog the
claim therefore spends its time crossing delivered rows to reach the few it wants, and its cost grows
with the table rather than with the work.

Measured on the relay's own executions with `auto_explain` (`run2-relay-claim-plans.txt`):

    steady state, 4 pending      dispatch+aggregate    0.36 ms   1 row discarded
    under backlog                tandem_outbox_pkey    1138 ms   202 317 rows discarded

`idx_tandem_outbox_dispatch (bucket, id) WHERE status = 0` cannot help: it yields rows in bucket-major
order, so satisfying `ORDER BY id` from it costs a sort, and the planner declines.
`idx_tandem_outbox_pending_id (id) WHERE status = 0` is the same `id` order over pending rows only, so
the sort-free path survives and the delivered rows stop being visited.

**A second feedback loop, seen live.** During a recovery the relay's own deliveries create `DONE` rows
faster than cleanup removes them, so the primary key walk lengthens as the recovery proceeds. The net
drain rate fell 442 -> 183 -> 118 -> 15 rows/s while `DONE` rose from 172 k to 186 k. The recovery was
slowing itself down.

## Three readings of the same failure, two of them wrong

Worth recording because the wrong ones were each supported by a measurement.

1. **"Chain density is the cause."** A throwaway crisis measurement
   (`appendix-throwaway-crisis-findings.md`) showed delivery × backlog holding constant, and at 1 M
   pending rows there were only 1 024 distinct aggregates: one claimable row per thousand, because the
   head-of-chain gate admits one row per aggregate. True, and not the cause.
2. **"It is an artefact of the generator."** The benchmark's fixed 1 024-aggregate population makes
   chains grow without bound, which no real write side does. S11 was built with a lifecycle population
   instead (aggregates emit ~100 events and retire), and chain length duly stayed bounded: 62 rows per
   chain at a 720 k backlog against 1 004 before. **It diverged anyway.** The model mattered, moving
   the threshold from ~100 k to ~700 k, and it was not the cause either.
3. **"The planner's access path is the cause."** Confirmed by `auto_explain` reading the relay's own
   plans, and then by the fix.

The mechanism was **already in the repository**, in `LLD-jdbc` §7.4, recorded while studying MySQL's
next-key locks: the claim "makes the optimiser choose `PRIMARY` as the access path — not
`idx_tandem_outbox_dispatch` — in order to satisfy `ORDER BY id LIMIT n`". Nobody had drawn the
PostgreSQL consequence. What this investigation added is what that choice costs under a backlog, and
that a partial index removes it.

## What was tried and is worse

`query-variants-at-plateau.txt` and `query-variants-mid-recovery.txt` compare four shapes against the
same loaded table. Two candidate fixes are worse than the problem:

| variant | at 242 k pending |
|---|---:|
| the claim as it is | 37 ms |
| ordered by `(bucket, id)` instead of `id` | 105 ms |
| one index scan per bucket, joined laterally | 649 ms |
| with `idx_tandem_outbox_pending_id` | **3.9 ms** |

**The `ORDER BY o.id` is load-bearing and must not be relaxed.** An aggregate's oldest row has its
lowest id, and that is exactly the row the head-of-chain gate admits, so `id` order meets the heads
first. A bucket-major walk works through rows the gate then rejects: 5 879 anti-join probes to find 100,
against 111.

## Files

| File | What it is |
|---|---|
| `run1-no-index-s11.log` | S11, both cells, no index: 15 min recovers slowly, 30 min diverges |
| `run2-no-index-autoexplain-s11.log` | the same with `auto_explain` armed; its first cell is misclassified, see below |
| `run2-relay-claim-plans.txt` | 117 plans the relay actually executed, the evidence for the mechanism |
| `run3-variant-probe-s11.log` | the run the query variants were measured against |
| `query-variants-at-plateau.txt` | four query shapes at 242 k pending, 12 k delivered rows |
| `query-variants-mid-recovery.txt` | two of them at 281 k pending, 85 k delivered rows, the painful point |
| `run4-with-index-s11.log` | S11, both cells, with `v5`: both recover |
| `*-pending.tsv` | pending and delivered rows every 2-3 s; the Mac runs also carry distinct pending aggregates, which is how the chain-density reading was tested |
| `run5-ec2-with-index-s11.log` | the same, with `v5`, on the reference host (native Docker, 2 vCPU, 10 and 20 minute outages) |
| `index-write-cost.md` | what the index costs the write path, and why S1 could not answer it |
| `appendix-throwaway-crisis-*` | the earlier throwaway measurement and its own corrections |

**`run5`'s sampler wrote a literal `\t` instead of a tab**, a quoting slip in the remote one-liner;
the archived file has been repaired to real tabs and given a header, and is otherwise as collected.

**One observation from `run5` worth a look, not a conclusion.** During its recoveries that host
delivered roughly 3 850 rows/s counting the incoming writes, while S1's measured ceiling on the same
host is 1 200-1 450 events/s. Not a contradiction: S1 drives the whole system including the generator
and the write path, whereas a recovery is almost all delivery and consumption. But it suggests the
published ceiling is dominated by the write side rather than by the relay's delivery capacity, which
is a different statement from the one the number appears to make, and it deserves its own measurement
before being asserted.

**`run2`'s first cell is reported as DIVERGING and was not.** S11's divergence check compared each
sample with the previous one, so a healthy recovery that wobbles upward for thirty seconds — which it
does, because the write side never stops — was called diverging at 51 118 pending having started from
359 119. The check now requires pending to exceed the backlog the recovery started from. The run is
kept because its plans are the evidence, and because a detector that misfires on the good case is
worth remembering.

Absolute numbers are indicative, never KPIs: this is not the reference host
([HLD-load-testing §5](../../HLD-load-testing.md)). What should hold anywhere is the shape: a claim
whose cost tracks the table rather than the backlog, and an index that severs the two.
