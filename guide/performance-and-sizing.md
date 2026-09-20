# Performance and Sizing

---

## 0. Who this page is for

Tandem is running, or about to be, and you want to know **how fast it is, what it costs your
database, and which settings to change (if any) for your load**. The short answer is that the
defaults are right for most services, and this page tells you how to check that and what to do when
they are not.

!!! question "The question this page answers"
    **How much can one relay deliver, how long does an event take to reach Kafka, what does polling
    cost my database, and how do I size it for my own traffic?**

It builds on [Getting Started](getting-started.md). The reasoning and the raw measurements behind
every figure are in the
[relay sizing note](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/docs/relay-sizing.md);
the published results are on [tandem.codingful.com/performance](https://tandem.codingful.com/performance/).

---

## 1. What to expect

The figures below were measured on a small **2 vCPU / 8 GB** cloud VM that also ran PostgreSQL,
Kafka and the load driver, with 1 KB events and the production-mandated producer settings
(`acks=all`, idempotence on).

| What | Measured |
|---|---|
| Delivery latency, from your `COMMIT` to Kafka's acknowledgement, at 600 events/s | median **13 ms**, p99 87 ms |
| Sustained throughput | **1200 to 1400 events/s**, with the backlog flat |
| Catching up after an outage | about 3 400 to 3 700 rows/s, the same at twice the backlog |
| Lost events, ordering violations | **zero**, in every scenario and at every rate, including rates the host could not keep up with |

Read them as a **floor**, not a promise:

- The two cores also carried the database, the broker and the load driver. A host with cores of its
  own should do better.
- The host is a burstable instance, and its ceiling has ranged from 725 to 1450 events/s with recent
  CPU use. Latency was stable across the same runs; throughput was not.
- What a host with dedicated cores sustains has not been measured yet.

Where the limit sat: at the ceiling the host's CPU was at 94% while the disk was under 1% busy. The
work that runs out is computation (encoding, publishing, polling), not the speed of the database's
disk.

Most services write far fewer than 1 000 events a second. If yours is one of them, you can stop
reading after [section 2](#2-the-two-numbers-you-will-ask-about); you have nothing to tune.

---

## 2. The two numbers you will ask about

### 2.1 Latency: how long until Kafka has the event

Latency is the wait for the relay to **notice** the row, plus a fixed cost of a few milliseconds to
claim, encode and publish it (`acks=all` included). The relay polls, so what a row waits depends on
whether its bucket is already busy:

| Your row arrives | It waits about | Why |
|---|---|---|
| Into a stream that is already flowing (the common case) | the **floor**, 10 ms by default, so a median of roughly 13 ms | The worker just found rows, so it is polling at its fastest. |
| After a quiet stretch on its bucket | up to the **ceiling**, 100 ms by default | The worker had backed off and is sleeping until its next poll. |

Two rules of thumb hold, with a few milliseconds of service time added:

```
median of a live stream  ≈  poll-interval-floor / 2  +  service time (2 to 10 ms)
worst case after silence ≈  1.4 to 2  ×  poll-interval
```

Below the poll term the distribution belongs to the host: garbage collection, CPU contention and a
broker pause set the p99.9, and no Tandem setting removes them.

!!! tip "If the first event after a quiet stretch must be fast, too"
    On PostgreSQL, `tandem.outbox.wakeup: pg-notify` (on both sides) signals the relay when a row is
    committed, so a row that lands in a quiet bucket is claimed at once instead of waiting for the
    next poll. Measured at 2 events/s, the median for such a row fell from 57 ms to 5.5 ms, for about a
    tenth of a millisecond added to a 1.3 ms write. It is off by default; see the
    [configuration reference](configuration.md#2-tandemoutbox) for what it needs.

### 2.2 Throughput: how many events a second

One relay sustained 1200 to 1400 events/s on the reference host. Up to its ceiling the relay
delivers one event for every event you write and the backlog stays at a handful of rows. **Past the
ceiling nothing fails and nothing is dropped**: the excess waits in the outbox and drains when the
rate falls back. That is the design working, not a fault, but a backlog that never drains means the
relay needs more capacity (see [section 4](#4-when-you-need-more-throughput)).

---

## 3. The settings, and which ones matter

Four settings shape the relay's polling. None does quite what its name suggests.

| Setting | Default | What it really controls |
|---|---|---|
| `tandem.relay.workers-per-instance` | twice the CPU count | **Throughput and blast radius, not latency.** Each worker owns a share of the buckets and runs its own claim and publish pipeline. More workers means more in parallel, and a smaller part of the outbox stuck behind any one wedged worker. |
| `tandem.relay.poll-interval-floor` | `10ms` | What a row of a **live stream** waits. |
| `tandem.relay.poll-interval` | `100ms` | The **ceiling**: what the first row after silence waits, and what a quiet relay costs. |
| `tandem.relay.poll-backoff-factor` | `2.0` | How fast the wait climbs from the floor to the ceiling. At 2.0 it goes 10, 20, 40, 80, 100 ms. |

Two consequences are worth stating plainly:

- **The worker count does not move the median.** Cutting workers by four times moved it by about
  3% in the measurements. Do not tune it for latency, in either direction.
- **The poll interval does.** With a fixed interval, 100, 50, 20 and 10 ms gave medians of 60, 31,
  16 and 9 ms. The adaptive default gets a live stream close to the 10 ms result while paying the idle
  cost of a 100 ms interval.

Every one of these is read **at startup**. Changing one needs a restart of the relay (pause and
resume through the [Admin API](admin-api.md) apply at runtime; these do not).

---

## 4. When you need more throughput

Work through these in order, and stop at the first that solves it.

1. **Confirm the relay is the bottleneck.** Watch `tandem.outbox.lag.age_seconds` and
   `tandem.outbox.lag.count` (see [Observability](observability.md#32-what-is-published)). A relay
   that keeps up shows a small, flat count; one that does not shows a count that climbs and stays up.
   A count that is high because of a `FAILED` row is a different problem, covered in
   [section 6](#6-what-quietly-costs-you-capacity).
2. **Check the host.** On the reference host the CPU was the limit. If the relay's machine is at
   its CPU ceiling, more workers cannot help; more CPU can, and so can moving the relay off the
   machine that also hosts the database (the split deployment in
   [Reliability](reliability.md#3-deployment-topology)).
3. **Raise `workers-per-instance`** if the CPU has room. Aim to run at **about half** the rate the
   relay can sustain: the slack is what drains a backlog after a burst, a restart or a Kafka
   hiccup, and it costs nothing while unused.
4. **Add relay instances** with `LEASE` coordination (see
   [Reliability](reliability.md#33-single-and-lease)). Each instance takes a share of the buckets, so
   capacity grows with the instance count, but so does the idle query load
   ([section 5](#5-what-polling-costs-your-database)).

Keep `workers-per-instance` at or below `bucket-count`: a worker owns the buckets whose number is
congruent to its index, so a surplus worker owns nothing and polls forever to find that out. Under
`LEASE` the split is over the buckets an instance currently owns, so prefer a worker count that
divides the bucket count. The bucket count itself is set once and never changed
([Reliability](reliability.md#4-the-bucket-count)); the default of 256 is right for most deployments.

!!! note "One aggregate is one lane"
    Events of one aggregate go out in order, so they are handled one after another by a single
    worker. Many aggregates spread over many buckets in parallel; **one very hot aggregate does not**.
    In the hot-partition scenario, 80% of the traffic went to one aggregate and its bucket queued a
    backlog behind it while every other aggregate kept flowing. If one aggregate carries most of your
    volume, that is the limit to plan around, and adding workers will not lift it.

---

## 5. What polling costs your database

A relay with nothing to deliver still asks the database for work. The load is easy to compute:

```
idle queries per second  =  instances × workers-per-instance / poll-interval
```

At the defaults (8 workers, 100 ms) that is about **80 queries a second per instance**, and it costs
about 4% of one core of the database. It scales with the number of instances, so the figure that is
negligible for one relay is ten times that for ten.

**Budget** roughly **a third of a core per thousand idle queries a second** on a healthy outbox.
While a stream is running the rate is set by the traffic instead, rising to at most
`workers / poll-interval-floor`, and it falls back within a few hundred milliseconds of the last row.
So the short floor is paid for only while there is something to find.

| Relay | Queries/s | PostgreSQL CPU, empty outbox |
|---|---:|---:|
| 8 workers, 100 ms ceiling (default) | about 78 | 3.5% of a core |
| 8 workers, 20 ms ceiling | about 333 | 12.1% |
| 8 workers, 10 ms ceiling | about 652 | 20.2% |

A short interval does **not** take capacity from delivery: the throughput ceiling measured at 10 ms
and at 100 ms was the same within the run-to-run spread.

### 5.1 Starting points

Change a setting only when a budget tells you to. These are the usual answers:

| Your situation | Suggested settings | Idle load |
|---|---|---|
| An ordinary service (most of you) | the defaults | about 80/s per instance |
| A latency-sensitive consumer | `poll-interval: 20ms` | about 400/s per instance |
| Mostly idle, latency matters only under load | `poll-interval: 1s` | about 8/s per instance |
| Many `LEASE` instances, mostly idle | `poll-interval: 500ms` | about 16/s per instance |
| High sustained throughput, latency secondary | the defaults, or more workers | about 80/s per instance |

A mostly idle outbox forces a choice: a bucket that goes quiet is either **cheap or fast** on its
first row back. The first row after silence waits up to the ceiling, so a longer ceiling saves
queries at the cost of that one row. Rows that arrive while the stream is flowing are unaffected
either way. With `wakeup: pg-notify` the choice goes away on PostgreSQL, since the cold row is
signalled rather than waited for.

Set the **floor** for the latency of a live stream (4 ms is about the service time on a small VM, so
nothing below it buys anything), and the **ceiling** for the cold row and for the idle cost.

---

## 6. What quietly costs you capacity

The poll interval is not what to watch. **Stuck rows are.**

- A delivered row leaves the index the relay polls, so 200 000 `DONE` rows add under a quarter to the
  cost of a poll. Finished rows are nearly free.
- A row that is `PENDING` but cannot be claimed stays in that index and is looked at on every poll.
  Two ordinary things produce them: a row backing off after a failure, and a row queued behind a
  `FAILED` one of the same aggregate. A handful per bucket roughly **triples** the cost of every poll
  that finds nothing, taking the budget from a third of a core to about **one core per thousand idle
  queries a second**.
- A blocked outbox also lowers the ceiling itself: about a fifth to a quarter in the measurements,
  at either poll interval.

So a poisoned aggregate costs throughput as well as attention. Alert on `blocked.count` and
`failed` rows ([Observability](observability.md#33-alerts-worth-having)) and resolve them
([Admin API](admin-api.md#33-recovering-messages)); that protects your capacity more than any
tuning.

!!! warning "Trading workers for a shorter interval turns against you"
    Fewer workers with a shorter interval (2 workers at 25 ms costs the same 80 queries/s as 8 at
    100 ms) worked on a clean outbox. But each claim then covers a wider slice of buckets and scans
    proportionally more stuck rows, so the same query rate cost **twice as much** at 2 workers as at 8
    on a blocked outbox, exactly when something is already wrong. Keep the workers sized for
    throughput and move the floor and ceiling for latency.

---

## 7. Recovering from a backlog

A relay that was down, or a Kafka that was unreachable, leaves rows waiting. Recovery **costs what
the backlog costs, not what the table costs**, because the relay reads pending rows through an index
of their own:

| Outage at 400 events/s | Undelivered rows | Cleared in |
|---|---:|---:|
| 15 minutes | about 358 000 | 146 s |
| 30 minutes | about 713 000 | 405 s |

On the 2 vCPU host, 240 000 and 479 000 rows cleared at 3 656 and 3 440 rows a second: the same
rate at twice the backlog. Nothing was lost, reordered or duplicated.

While the backlog drains, the relay competes with new writes for the same CPU, which is one more
reason to keep headroom (about half the sustained rate, [section 4](#4-when-you-need-more-throughput)).

---

## 8. What Tandem costs your write path

The write side is one `INSERT` in your own transaction. Measured on the reference host, a write
transaction took about **1.3 ms**. Two other costs are worth knowing:

| What | Cost |
|---|---|
| The index over pending rows that makes recovery cheap | **+0.6% WAL per insert** |
| `wakeup: pg-notify`, if you turn it on | about 0.1 ms per insert call, however many rows it wrote (one notification per call) |

Writing several events in one call (see [Advanced Integration](advanced-integration.md#3-writing-several-events-at-once))
is one round trip rather than many.

The outbox table itself does not grow forever: the relay deletes `DONE` and `DISCARDED` rows older
than `tandem.relay.retention` (14 days by default), and a six-hour run at 400 events/s with cleanup
on reached a steady table size with autovacuum keeping pace
([Reliability](reliability.md#53-cleanup-and-retention)). Size the database for
**the retention window times your event rate**, not for the lifetime of the service.

---

## 9. Measuring it on your own hardware

Do not trust these figures for your hardware; reproduce the ones you depend on. The load-test harness
takes every setting on this page, so you can measure your own database and broker:

```bash
./gradlew :tandem-benchmark:loadTest --args="--demo --duration=90 --workers=4 --poll-interval=25 --poll-floor=5 S2"
```

The scenario reports COMMIT to ack at p50, p95, p99 and p99.9 for a held load, and its first line
records the sizing it used. Quote a percentile only together with that line. Passing `--poll-floor`
equal to `--poll-interval` measures a fixed interval, which is how to compare against the
non-adaptive behaviour.

The cost of polling has its own probe, which holds the outbox in three states (empty, full of
delivered rows, and with stuck rows) and reports both the query rate and what it costs PostgreSQL:

```bash
./gradlew :tandem-benchmark:idlePollCostProbe --args="--seconds=60"
```

Both need Docker. Two cautions when you read the results:

- **Size against the median.** On a small or burstable host the median reproduces and the p99 does
  not; treat a single p99 as indicative.
- **Measure on the shape of your own hardware.** The tables here came from a cloud VM and a developer
  machine, and the *ratios* carried across both while the absolute numbers did not.

---

## 10. Checklist

- [ ] You have looked at `lag.age_seconds` and `lag.count` under real load, and they stay small and
  flat.
- [ ] You have left the poll settings at their defaults, or can name the budget that made you change
  them.
- [ ] For each instance you added, you have multiplied the idle query load and are content with it.
- [ ] The relay runs at about half of the rate it can sustain, so a backlog can drain.
- [ ] `workers-per-instance` is not above `bucket-count`.
- [ ] Alerts exist for blocked and failed rows, since they cost capacity as well as correctness.
- [ ] You know whether one aggregate carries most of your volume.
- [ ] The database is sized for the retention window, and you have chosen that window on purpose.
