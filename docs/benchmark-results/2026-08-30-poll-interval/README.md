# Poll interval vs capacity — does a short interval take work away from delivery?

The sustainable ceiling measured at two poll intervals in two outbox states, to answer a question the
idle-cost measurement could not: those extra claims cost CPU, but do they compete with real traffic?

## Configuration

`pollIntervalCapacityProbe`, 8 workers, 256 buckets, 1 KB payloads, 200 000 `DONE` rows in both
states and 2 000 unclaimable rows added for the blocked one. Two replicates per cell, the second pass
in reverse order so a drifting host feeds both arms of every comparison. Each cell is one
`RampController` search: 45 s sustain window, 5% tolerance, 6-minute budget, seeded at 800 events/s.

```
--sustain=45 --budget=360 --seed-rate=800 --replicates=2 --done-rows=200000 --blocked-rows=2000
```

## Results

| outbox | poll | ceiling, per replicate | mean | spread | bracketed |
|---|---:|---:|---:|---:|---|
| drained | 100 ms | 1350 / 1550 | 1450 | 13.8% | yes |
| drained | 10 ms | 1550 / 1550 | 1550 | 0.0% | yes |
| blocked | 100 ms | 1100 / 1100 | 1100 | 0.0% | yes |
| blocked | 10 ms | 1100 / 1250 | 1175 | 12.8% | yes |

**No capacity cost from the short interval.** In both states 10 ms sustained about 7% *more* than
100 ms — the opposite direction from the hypothesis, and smaller than the spread between replicates
of the same cell. This host cannot separate the two intervals; what it does rule out is a large
effect. The threefold idle cost a blocked outbox adds per claim does not return as a lower ceiling.

**The blocked outbox costs capacity on its own**, at either interval: 1100–1175 against 1450–1550
drained, a fifth to a quarter of the ceiling, with the ranges not overlapping. Unclaimable rows are
what to watch — not how often the relay polls.

All eight searches bracketed their ceiling, so no figure here is a lower bound set by the search
budget.

## Files

| File | What it is |
|---|---|
| `poll-interval-capacity.log` | the probe's stdout — every ramp hold, per-cell result, and the summary |
| `host-samples.tsv` | per-minute host CPU speed limit, load average, and per-container CPU/memory/IO |

Developer Mac with Docker in a VM, not the reference host
([HLD-load-testing §5](../../HLD-load-testing.md)): read the ratios between cells, never the rates. The
host's own CPU speed limit ranged from 55% to 100% during the session, which is what the replication
and the reversed second pass exist to absorb.
