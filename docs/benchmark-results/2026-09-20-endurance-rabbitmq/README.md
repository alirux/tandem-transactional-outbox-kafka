# S9 endurance over RabbitMQ: ninety minutes at 400 events/s

The endurance scenario held against the **AMQP connector** instead of Kafka, on a Mac: a fixed
moderate load under `LEASE` coordination, sliced into six fifteen-minute reporting windows, with
DONE-row cleanup deleting throughout. **Verdict: PASS, 6/6 windows.**

This is a correctness run, not a comparison. The harness reads AMQP through one queue and one
consumer, which is what keeps per-aggregate ordering observable and also caps delivered throughput at
what a single consumer thread drains (LLD-benchmark §3.1), so the latency and throughput figures here
describe that arrangement. The published numbers remain Kafka's.

## Configuration

400 events/s fixed (never ramped), 2 relay instances × 8 workers, 256 buckets, 1 KB payloads,
retention 10 min, cleanup every 30 s in batches of 20 000, 48-connection pool, no post-commit wakeup.
~2.15M events delivered.

```
--broker=rabbit --duration=5400 --window=900 --rate=400 --connections=48
--retention=600 --cleanup-interval=30 --cleanup-batch=20000 S9
```

## Results

Zero ordering violations, zero lost events, zero duplicates over the whole run. Zero `SEVERE` and zero
`WARNING` in the log, so the cleanup path ran ninety minutes with two instances deleting and never
failed a pass.

| Window | delivered/s | written/s | p50 | p99 | pending | coverage | outbox | dead tuples | heap | threads |
|---|---|---|---|---|---|---|---|---|---|---|
| 1 | 397.0 | 397.0 | 17 ms | 97 ms | 12 | 256/256 [128,128] | 421 MB | 5 656 | 50 MB | 70 |
| 2 | 399.4 | 399.4 | 18 ms | 95 ms | 11 | 256/256 [128,128] | 443 MB | 64 601 | 28 MB | 71 |
| 3 | 399.7 | 399.7 | 17 ms | 93 ms | 12 | 256/256 [128,128] | 436 MB | 88 912 | 40 MB | 70 |
| 4 | 399.7 | 399.7 | 17 ms | 93 ms | 13 | 256/256 [128,128] | 439 MB | 87 884 | 44 MB | 71 |
| 5 | 399.8 | 399.8 | 17 ms | 93 ms | 12 | 256/256 [128,128] | 435 MB | 78 131 | 28 MB | 70 |
| 6 | 393.9 | 395.8 | 18 ms | 626 ms | 2 062 | 256/256 [128,128] | 443 MB | 66 709 | 62 MB | 71 |

**Nothing drifted.** Delivered and written match to the decimal in windows 1 to 5, so the relay stayed
exactly at the write side's pace with a backlog of a dozen rows and an oldest row younger than a
second. The `tandem_outbox` table reached the size its retention implies (~245k rows) and stayed
there, dead tuples stabilised between 64k and 89k, and bucket coverage was complete and disjoint in
every window across the run's lease renewal cycles.

**Threads are the signal this run exists for, and they are flat at 70 to 71.** The AMQP client keeps
its own connection and consumer threads, and the connector tracks every unsettled publish in maps of
its own (`RabbitRelay.inFlightConfirms()`, LLD-rabbitmq §3): a resolution path that failed to release
an entry would show here as a count climbing window after window, which is invisible in any run short
enough to be a test. The Kafka runs sit at 49 to 50 for comparison of shape, not of size.

**Heap must be read as a series, not as endpoints.** The verdict line reports 50 MB → 62 MB, which
reads like a leak; the six samples are 50, 28, 40, 44, 28, 62, a sawtooth with no trend. It is *used*
heap sampled at arbitrary points of the GC cycle.

**Window 6 is the machine, not the run.** The Mac was in use while the run was finishing. In the final
minute `load1` went from 7.02 to 88.77 and `CPU_Speed_Limit` from 69% to 48%, and the window's p99 and
backlog follow that minute rather than any trend before it: the other five windows sit in a 93 to
97 ms p99 band with a dozen pending rows. The backlog drained during the scenario's own catch-up,
which is why the run still reconciles at zero loss. For the same reason the verdict line's
`p99 97ms → 626ms` is an endpoint reading of that one minute and should not be quoted; `host-samples.tsv`
carries the load column it came from.

## The host

An Intel Mac, which reduces clock under sustained load, so `CPU_Speed_Limit` was sampled every minute.
Median 69%, minimum 48%, hourly means 77.5% / 68.7% / 77.2%: no progressive thermal decline, so the
flat throughput across windows 1 to 5 is a property of the system under test rather than of a machine
that never slowed down. `load1` sat at a median of 8.0 with four excursions above 20, two of them in
the run's last two minutes and two at 10:45 and 10:46.

## Why the run was worth doing

The connector's own suite drives `dispatch` directly, one row at a time, waiting for each ack, and the
benchmark's AMQP smoke check lasts seconds. Neither can show what only time shows: that the publisher
confirm bookkeeping returns to its resting size indefinitely, that two instances keep dividing the
buckets cleanly over many renewal cycles while publishing over AMQP, that the single-queue topology
does not fall behind the write side (`rabbitmqctl list_queues` mid-run: 0 messages, 0 unacknowledged,
1 consumer), and that ninety minutes of concurrent cleanup produce no failed pass.

## Files

| File | What it is |
|---|---|
| `s9-endurance-rabbitmq.log` | the harness's own stdout at INFO: the six window lines and the verdict |
| `host-samples.tsv` | per-minute `CPU_Speed_Limit`, load average, and per-container CPU/memory/IO |
| `container-map.tsv` | which container name is PostgreSQL and which is RabbitMQ |

Absolute numbers are indicative, never KPIs: this is not the reference host
([HLD-load-testing §5](../../HLD-load-testing.md)), and the AMQP read side is the harness's
single-consumer arrangement rather than a deployment's.
