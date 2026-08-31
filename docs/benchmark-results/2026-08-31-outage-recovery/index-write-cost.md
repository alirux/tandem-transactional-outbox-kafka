# What `idx_tandem_outbox_pending_id` costs the write path

Measured 2026-08-31 on a dedicated `postgres:16-alpine` with the Tandem baseline schema applied and
**no relay running**, so nothing competes with the writes. Rows carry a 900-byte JSONB payload.

## Why not S1

The obvious instrument was the throughput scenario, and it cannot answer this on this host. S1 needs
to bracket a ceiling, and on a fast developer machine it never does, because the relay outruns the
harness's own load generator (dispatch-latency, Q-G). A comparison between two unbracketed lower
bounds says nothing.

S10 does report the write transaction's own duration, which is the right *kind* of number, but it runs
at 2 events/s and yielded 243 samples per arm against a 4 ms transaction dominated by commit. An index
entry costs tens of microseconds; that is far below the resolution of that sample.

Insert throughput measured directly with `pgbench` was no better. Five consecutive 45-second cells
under identical conditions gave **3841, 3166, 3294, 3091, 2085 tps**: a ±35% spread between cells that
differ in nothing, which is roughly ten times any plausible effect. It never reached a plateau.

## The instrument that worked: WAL bytes

Index maintenance emits WAL records, and WAL volume is close to deterministic where elapsed time is
not. `pg_current_wal_lsn()` before and after a fixed number of writes gives bytes per row.

### INSERT, which is the client's own transaction

20 000 inserts, 8 clients, `CHECKPOINT` before each cell, index created and dropped between cells:

| replicate | with index | without index | delta |
|---|---:|---:|---:|
| 1 | 6 601 | 6 519 | +82 |
| 2 | 6 677 | 6 652 | +25 |
| 3 | 6 775 | 6 765 | +10 |

**About +39 bytes per insert on ~6 600, i.e. +0.6%.** All three deltas are positive, and the common
rise across replicates (6 519 -> 6 765) is the table growing, which the paired comparison cancels. The
absolute figure is physically plausible on its own: a btree entry is roughly twenty bytes plus the WAL
record's own overhead.

### UPDATE `PENDING -> IN_FLIGHT`, which is the relay's cost

The index is partial on `status = 0`, so the claim's update removes the entry. Same instrument, 20 000
rows per cell, **ABBA order** (with, without, without, with):

| replicate | with (mean of 2) | without (mean of 2) | delta |
|---|---:|---:|---:|
| 1 | 8 686 | 8 695 | -9 |
| 2 | 8 901 | 8 904 | -3 |

**Below the resolution of the measurement.** Cell-to-cell drift is 50-90 bytes, an order of magnitude
above the effect, so all that can be said is that the cost is under about 1%.

**A first attempt at this measurement was invalid and is worth recording.** It ran `with` before
`without` in every replicate, so the upward drift landed on the same arm every time and produced a
consistently *negative* delta: the index appearing to make the update cheaper. The sign was impossible,
which is the only reason it was caught. A small delta with a plausible sign would have been believed.

## Caveat that cuts against the conclusion

`CHECKPOINT` before each cell makes the cells comparable, and it also means every touched page is
written to WAL in full. Those full-page writes are most of the 6 600 and 8 700 bytes, so they inflate
the denominator and **understate the index's relative share**. In steady state, away from a checkpoint,
WAL per row is much smaller and the index's percentage correspondingly larger. The absolute byte
figures are the robust part; the percentages are a lower bound.

Absolute numbers are indicative of this host only ([HLD-load-testing §5](../../HLD-load-testing.md)).
