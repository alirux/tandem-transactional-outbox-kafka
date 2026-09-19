# Virtual threads and the relay: decision & measurement

**Version:** 1.0  
**Status:** Decided  
**Companion to:** [relay-sizing.md](relay-sizing.md), [LLD-jdbc.md](LLD-jdbc.md) §3.1, HLD §1.3  
**Evidence:** [benchmark-results/2026-09-19-virtual-threads/](benchmark-results/2026-09-19-virtual-threads/)

Two questions live under one heading, and they have opposite answers. Should the **relay's own
workers** run on virtual threads? No. Can **an application that runs on virtual threads** use
Tandem? Yes, with nothing to configure and nothing to avoid. §1 to §5 settle the first; §6 answers
the second, which is the one adopters actually ask.

---

## 1. Decision

**The relay's workers stay platform threads, and the published modules stay on Java 17.**

Not because virtual threads misbehave. They were measured, and they deliver the same throughput with
the same correctness (§4). The decision rests on there being nothing to win: the dispatch port is
asynchronous by contract, so no thread is ever held per in-flight message, and relay concurrency is
bounded by the connection pool and by `batchSize`, never by the thread count. Against that, a
virtual-thread relay would cost a baseline move from Java 17 to 24, measurably more CPU at the same
delivered rate, and a change in what an interrupt does at shutdown (§5).

**An opt-in `ThreadFactory` on `RelayConfig` is also rejected for now**: it is new public API for a
case no adopter has raised and no measurement supports.

---

## 2. What the relay does with threads today

- `WorkerPool` starts `workersPerInstance` daemon platform threads, defaulting to
  `availableProcessors() * 2`, plus one scheduler thread for the periodic jobs and, when the
  `pg-notify` wakeup is wired, one thread holding the `LISTEN` connection.
- A worker's loop blocks in exactly three places, all of them JDBC: the claim, the flush of
  acknowledged rows, the flush of failures. Then it waits out its idle backoff.
- **The dispatch path blocks nothing.** `OutboxDispatcher.dispatch` returns a
  `CompletableFuture<Void>`; completions land on the adapter's own I/O thread (for Kafka, the
  producer's sender thread) and only enqueue the outcome, which the worker drains on its next cycle
  (LLD-jdbc §3.3). One worker therefore keeps `batchSize` messages in flight without a thread per
  message.

The pattern virtual threads make cheap, one blocked thread per unit of work in flight, does not
exist here. It was designed out for a different reason, keeping JDBC writes off the dispatcher's I/O
thread, and its absence is what decides this question.

---

## 3. Thread count is not what bounds the relay

Already measured, before this exercise, in [relay-sizing.md](relay-sizing.md) §2: at a fixed poll
interval, cutting workers from 8 to 2 moves the median by 3%, and the tails **improve** as the count
falls, because the service term gets cheaper with less contention. Confirmed on a second host, where
a fourfold cut in concurrency moved the median by 2%. Separately, LLD-benchmark §8.1 records the
relay keeping up comfortably with a nominal one million events/s offered, because the connection
semaphore throttles long before the relay does.

What bounds a relay is the connection pool and the in-flight window. Virtual threads add no
connections; they move the queue from the thread pool to the pool of connections, where it already
was.

---

## 4. The measurement

A throwaway seam selected the worker thread kind from a system property, with the thread created
reflectively so `tandem-jdbc` kept compiling on Java 17, and a probe in `tandem-benchmark` ran the
arms on JDK 25 against real PostgreSQL and Kafka containers. 18 arms: three cells, three replicates
each, the two arms swapping order on every replicate so host drift cannot land on one of them. A
fixed 400 events/s offered, 60-second window after 25 seconds of warm-up, 12-core developer Mac.
Medians of the replicates:

| workers | arm | p50 | p95 | p99 | p99.9 | delivered | process CPU | relay platform threads | JFR pinned events |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | platform | 11.8 | 40.6 | 69.7 | 108.4 | 398.5/s | 4.6% | 9 | 0 |
| 8 | virtual | 11.0 | 39.8 | 67.9 | 113.0 | 398.6/s | 5.0% | 1 | 0 |
| 24 | platform | 20.1 | 81.2 | 101.7 | 126.2 | 399.4/s | 4.7% | 25 | 0 |
| 24 | virtual | 18.5 | 78.0 | 96.5 | 116.5 | 399.6/s | 5.3% | 1 | 0 |
| 64 | platform | 32.5 | 98.9 | 116.7 | 128.4 | 399.8/s | 5.0% | 65 | 0 |
| 64 | virtual | 31.0 | 94.1 | 110.9 | 121.0 | 399.7/s | 6.5% | 1 | 0 |

COMMIT→ack milliseconds. The 64-worker cell is there because it is where virtual threads are
supposed to pay: more workers than cores, and twice as many as the 32-connection pool can serve.

What it says:

- **Correctness is untouched.** Zero ordering violations and zero missing events across all 18
  windows, and the whole scenario suite (sustained throughput, hot partition, worker failover, poison
  message, multi-instance lease, endurance, cold burst, outage recovery) passes with the workers on
  virtual threads.
- **No pinning on JDK 25.** Zero `jdk.VirtualThreadPinned` events with the threshold lowered to 1 ms,
  so neither the blocking claim nor the worker's `synchronized` idle wait holds a carrier. This is a
  property of JDK 24 and up (JEP 491); the measurement says nothing about 21 to 23.
- **Throughput is identical**, which is expected: the rate is offered, not a ceiling.
- **Latency favours virtual threads slightly and consistently**, by 0.8 to 1.6 ms at the median and 2
  to 6 ms at the p99. Real, but small against a distribution whose median is made of the poll wait.
- **CPU favours platform threads, and the gap widens with the worker count**: 9% relative at 8
  workers, 13% at 24, 30% at 64. The cell where virtual threads should have paid is the one where
  they cost most.
- **The only thing virtual threads genuinely buy is threads**, 65 down to 1 at the 64-worker sizing,
  in a process that carries a hundred either way.

### Memory, measured on its own and for a reason

Fewer threads is a memory claim, and the load matrix cannot price it: it runs every arm in one
long-lived JVM whose heap and resident size climb monotonically with arm order, so comparing the arms
there reads the position in the run rather than the thread kind. The cost of the threads was measured
instead as a **delta around starting an idle pool** in an otherwise unchanged process, which the
accumulation cannot reach. Three replicates, medians, no load offered (the workers still run their
claim cycle, so their stacks are touched to the depth they use):

| workers | arm | committed thread stacks | heap | resident |
|---:|---|---:|---:|---:|
| 8 | platform | +0.81 MB | +0.20 MB | +1 MB |
| 8 | virtual | +0.40 MB | +0.25 MB | +0 MB |
| 24 | platform | +2.32 MB | +0.29 MB | +3 MB |
| 24 | virtual | +0.27 MB | +0.33 MB | +1 MB |
| 64 | platform | +4.93 MB | +0.50 MB | +5 MB |
| 64 | virtual | +0.18 MB | +0.64 MB | +3 MB |

The platform arm's stacks scale with the worker count at about **77 KB committed per thread**, which
is the number that matters rather than the megabyte of address space each one reserves and never
touches. The virtual arm's stay flat, since only the carriers are real threads. The heap moves the
other way and by much less: the continuations cost between 0.05 and 0.14 MB more than the platform
arm's own per-worker objects.

**So the whole memory saving at 64 workers is about 4.8 MB of committed stack and 2 MB of resident
size, in a process that sits between 200 and 400 MB.** At the shipped default it is under a
megabyte. This is the benefit side of the trade in full, and it is why §1 says there is nothing to
win rather than that the win is outweighed.

---

## 5. What the measurement found that the reasoning had not

**Interrupting a worker means something different on a virtual thread.** Five of the nine virtual
arms ended their run with an `ERROR` that never appeared in any of the nine platform arms:

```
Relay worker iteration failed workerIndex:54
TandemException: claimBatch failed
  Caused by: PSQLException: An I/O error occurred while sending to the backend
  Caused by: SocketException: Closed by interrupt
```

`stop()` interrupts the workers. On a virtual thread the socket read runs on the interruptible NIO
implementation, so the interrupt aborts the query in flight and closes the connection; on a platform
thread the same interrupt sets the flag, the read completes, and the shutdown stays quiet. Nothing is
lost (the rows stay claimable and the lease reclaims them), but every shutdown under load would emit
ERROR lines that today mean something is wrong, and hand a dead connection back to the pool.

The consequence for this decision: moving the workers to virtual threads would not be a
configuration change. It would require the shutdown path to tell an interrupt-during-stop apart from
a real claim failure first.

---

## 6. Running Tandem inside an application on virtual threads

This is supported, it needs no configuration, and the parts of it that could break silently were
verified rather than assumed.

- **Tandem's write path holds no monitor.** Read from the compiled classes: `tandem-core`,
  `tandem-spring-producer`, `tandem-kafka` and `tandem-cloudevents` contain no `monitorenter` and no
  `synchronized` method at all, and the only three in `tandem-jdbc` (`WorkerPool`,
  `WorkerWakeups$Slot`, `PgNotifyWakeup`) are relay-side. The outbox insert, which is what runs on the
  caller's thread, can therefore never pin a carrier on any JDK from 21 up.
  `VirtualThreadFootprintTest` keeps it that way: it disassembles every compiled class of
  `tandem-core` and `tandem-jdbc` on each test run and fails on a monitor outside those three. The
  guard exists because this property breaks in silence, the same reason `JacksonFootprintTest` reads
  bytecode instead of sources.
- **The relay is unaffected by the host's thread configuration.** `WorkerPool` creates its own
  platform threads, so `spring.threads.virtual.enabled=true` changes nothing about it, in either
  direction. An application on virtual threads keeps a relay that behaves exactly as measured
  everywhere else.
- **`TandemContext` is a plain `ThreadLocal`, and virtual threads make it safer.** Its documented
  hazard is a stale correlation id left on a pooled thread; on a thread-per-request runtime the
  thread dies with the request and the hazard disappears.
- **The write side carries no Kafka client by construction** (HLD §1.3), so the producer's internal
  locks are never on an application thread. This is the minimal-footprint rule paying a dividend it
  was not designed for.
- **The driver, for the version Tandem tests against.** PgJDBC 42.7.12's query path is
  `ResourceLock`-based, not monitor-based: `PgConnection`, `PgStatement` and `PgPreparedStatement`
  hold no monitor, and the ten that remain in `QueryExecutorImpl` are on the binary-OID registry,
  around in-memory set operations with no I/O inside. HikariCP 5.1.0's borrow path is monitor-free
  too; its four `synchronized` methods are lifecycle only (`shutdown`, `suspendPool`, `resumePool`,
  `fillPool`). An older driver is a different matter: the migration away from `synchronized` landed
  in PgJDBC 42.5.1, and before it every JDBC call pinned the carrier on JDK 21 to 23.
- **CI has been exercising this all along.** `tandem-benchmark`'s load driver runs every insert on
  its own virtual thread, so every scenario run writes to the outbox from virtual threads. The
  property was covered before it was stated.

---

## 7. When to revisit

- **A dispatcher that blocks per message.** Today none exists: Kafka is asynchronous, and the
  RabbitMQ adapter maps publisher confirms onto the same future. An HTTP or webhook adapter that
  blocked a thread per publish would put a thread-per-work-unit pattern into the relay for the first
  time, and with it the only argument that would make virtual threads worth their cost.
- **Not the Java baseline.** If the published modules move to 24 or later for unrelated reasons, that
  removes the largest cost but changes nothing on the benefit side: the CPU figures in §4 and the
  shutdown semantics in §5 would still be the whole story.

---

## 8. Consequences

- The README's limitation entry says what "blocking JDBC" does and does not rule out, since an
  adopter on virtual threads reads it as a verdict on their runtime.
- [guide/adoption.md](../guide/adoption.md) §7 states that virtual threads are one more thing not to
  change.
- [LLD-benchmark.md](LLD-benchmark.md) §2 explains the module's JDK 25 toolchain by the driver's
  pinning on 21 to 23, which was true of the driver of the day and is no longer true of the pinned
  one. The toolchain choice is unaffected (virtual threads still need 21+), the reason is narrower.
- `VirtualThreadFootprintTest` (in `tandem-jdbc`) pins the monitor-free write path, covering
  `tandem-core` as well since that module has no test set of its own to hold it.
- **No change to the shutdown path.** The interrupt semantics in §5 cannot arise while the workers are
  platform threads, and interruption is already handled where it can occur today: a worker interrupted
  in its idle wait swallows the `InterruptedException` and re-arms the flag rather than reporting a
  failed cycle. Downgrading the iteration's `ERROR` on the way out would instead hide a genuine claim
  failure in the one window where rows are most likely to be left `IN_FLIGHT`. §5 stands as the
  prerequisite to satisfy first if the workers ever do move.
- The measurement's seam and probe were deliberately not kept. The archived run records both well
  enough to rebuild them.
