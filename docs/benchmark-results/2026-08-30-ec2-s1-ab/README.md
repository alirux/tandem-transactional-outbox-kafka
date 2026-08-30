# Does the adaptive idle backoff cost throughput? Four ceiling searches, alternating

The adaptive backoff (LLD-jdbc §3.1) polls a busy bucket far more often than the fixed 100 ms
interval did. The Mac probe in [2026-08-30-poll-interval](../2026-08-30-poll-interval/) found no
capacity cost from a short interval, but that host has four idle cores to spare. This is the same
question asked on the two-core reference host, where it is much harder to hide.

## Configuration

`--duration=240 S1` four times, alternating the timing and reversing the order on the second pass so
a drifting host feeds both arms: **old, new, new, old**. `--poll-floor=100` is the pre-adaptive
timing, floor equal to the ceiling.

## Results

| order | timing | ceiling | bracketed below |
|---|---|---:|---:|
| 1 | fixed 100 ms | 1350 | 1400 |
| 2 | adaptive 10 → 100 ms | 1350 | 1400 |
| 3 | adaptive 10 → 100 ms | 1300 | 1350 |
| 4 | fixed 100 ms | 1200 | 1250 |

Means: **1275 fixed, 1325 adaptive.** The adaptive arm is 4% higher, which is less than the spread
inside the fixed arm (12.5%) and inside its own (3.8%). The first two searches, run back to back,
landed on the same number.

**No capacity cost this host can resolve**, which is the same verdict the Mac probe reached from the
other direction. What it rules out is a large effect: the extra claims a busy relay issues do not
come back as a lower ceiling.

It also puts the day's other ceiling searches in context. Seven ran in one hour on this machine —
1200, 1250, 1300, 1350, 1350, 1400 — with both timings appearing at both ends of that range. A
single ceiling from this host is a reading of its burst allowance as much as of the relay
([README](../README.md)).

## Files

| File | What it is |
|---|---|
| `ab-old-1.log`, `ab-old-2.log` | fixed 100 ms interval, first and last |
| `ab-new-1.log`, `ab-new-2.log` | adaptive backoff, second and third |
