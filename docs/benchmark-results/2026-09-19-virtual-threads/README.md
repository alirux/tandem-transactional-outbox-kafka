# Platform threads against virtual threads, for the relay's workers

The measurement behind [virtual-threads-decision.md](../../virtual-threads-decision.md) (backlog item
9). Two questions: does anything break if the relay's workers run on virtual threads, and does
anything get faster. Neither, and the interesting finding is a third thing nobody was looking for
(§4).

## Configuration

A developer Mac, 12 cores, Docker Desktop, JDK 25, real PostgreSQL and Kafka containers. Relay
defaults otherwise: 256 buckets, batch 100, 100 ms poll ceiling with a 10 ms floor, 1 KB payloads,
32-connection pool, wakeup off, `acks=all`, `enable.idempotence=true`.

18 arms: three worker counts, three replicates each, both thread kinds. The arms **swap order on
every replicate** so drift in the host cannot land on one of them. Each arm resets the outbox, builds
its own relay instance, generator and consumer group, writes at a fixed **400 events/s**, discards a
25-second warm-up window and measures the next 60 seconds. COMMIT→ack latency (`ACCURATE` mode, one
clock on both sides), delivered rate, process CPU, live platform threads named `tandem-relay`, and
`jdk.VirtualThreadPinned` JFR events with the threshold lowered to 1 ms.

64 workers over a 32-connection pool is the cell that matters: more workers than cores, and twice as
many as the pool can serve at once, which is the shape virtual threads exist for.

## The seam, since it was not kept

Two throwaway edits, reverted after the run. In `WorkerPool.startWorkerThread`, the thread kind comes
from a system property, read per creation rather than into a `static final` (the probe flips it
between arms inside one JVM), and built reflectively so `tandem-jdbc` keeps compiling against Java
17:

```java
private static Thread newWorkerThread(Runnable body, String name) {
    if (!Boolean.getBoolean("tandem.relay.virtualWorkers")) {
        Thread t = new Thread(body, name);
        t.setDaemon(true);
        return t;
    }
    Object builder = Thread.class.getMethod("ofVirtual").invoke(null);
    Class<?> builderType = Class.forName("java.lang.Thread$Builder");
    builder = builderType.getMethod("name", String.class).invoke(builder, name);
    return (Thread) builderType.getMethod("unstarted", Runnable.class).invoke(builder, body);
}
```

The second edit passed the same property into `tandem-benchmark`'s `integrationTest` task, so the
whole scenario suite could be re-run on virtual workers. The probe itself was a `main` class in
`tandem-benchmark` driving the arms through `BenchmarkEnvironment.newRelayInstance`, the same way
`IdlePollCostProbe` does, with two modes: the load matrix, and the idle memory delta below. It read
committed thread stacks from Native Memory Tracking by running `jcmd VM.native_memory summary`
against its own pid, under `-XX:NativeMemoryTracking=summary`.

## Result

Medians of three replicates. Milliseconds, COMMIT→ack.

| workers | arm | p50 | p95 | p99 | p99.9 | delivered | process CPU | relay platform threads | pinned events |
|---:|---|---:|---:|---:|---:|---:|---:|---:|---:|
| 8 | platform | 11.8 | 40.6 | 69.7 | 108.4 | 398.5/s | 4.6% | 9 | 0 |
| 8 | virtual | 11.0 | 39.8 | 67.9 | 113.0 | 398.6/s | 5.0% | 1 | 0 |
| 24 | platform | 20.1 | 81.2 | 101.7 | 126.2 | 399.4/s | 4.7% | 25 | 0 |
| 24 | virtual | 18.5 | 78.0 | 96.5 | 116.5 | 399.6/s | 5.3% | 1 | 0 |
| 64 | platform | 32.5 | 98.9 | 116.7 | 128.4 | 399.8/s | 5.0% | 65 | 0 |
| 64 | virtual | 31.0 | 94.1 | 110.9 | 121.0 | 399.7/s | 6.5% | 1 | 0 |

Raw output: [`probe.log`](probe.log). The scenario suite on virtual workers, all eight passing:
[`smoke-suite.log`](smoke-suite.log). Memory is a separate run for a reason, below:
[`memory-idle.log`](memory-idle.log).

- **Zero ordering violations and zero missing events in all 18 windows.**
- **Zero pinning**, at a 1 ms threshold, in every virtual arm. Neither the blocking claim nor the
  worker's `synchronized` idle wait holds a carrier on JDK 25.
- **Delivered rate identical.** 400/s is offered, not a ceiling, so this says the relay kept up in
  both arms and nothing more.
- **Latency slightly better on virtual threads**, consistently: 0.8 to 1.6 ms at the median, 2 to 6 ms
  at the p99, in every cell and every replicate.
- **CPU consistently worse on virtual threads, and the gap grows with the worker count**: 9% relative
  at 8 workers, 13% at 24, 30% at 64.
- **Relay platform threads fall from 65 to 1** at the 64-worker sizing. That is the whole benefit,
  measured.

## Memory needed its own run

Fewer threads is a memory claim, and **the load matrix above cannot price it.** Every arm shares one
long-lived JVM, and its heap and resident size climb monotonically with arm order regardless of which
arm it is: at 8 workers the six consecutive arms read 40.8, 46.8, 50.5, 56.1, 57.9 and 65.6 MB of
settled heap. That is the process accumulating (a growing topic replayed by each new consumer group,
per-arm key sets, client buffers), not the thread kind. Alternating the arms does not rescue it: the
medians would still be reading position in the run. [`load-with-nmt.log`](load-with-nmt.log) is that
run, kept because it is the evidence for this and because its latency and CPU figures reproduce the
main table above.

So the thread cost was measured as a **delta around starting an idle pool**, which the accumulation
cannot reach: read committed thread stacks (NMT), settled heap and RSS, start a pool of N workers,
let it idle for 10 seconds, read them again. No load: the question is what N workers cost, and they
still run their claim cycle against an empty outbox, so their stacks are touched to the depth they
actually use (a stack commits lazily, so an untouched one would price at nothing). Three replicates,
arms alternating, medians ([`memory-idle.log`](memory-idle.log)):

| workers | arm | committed thread stacks | heap | resident |
|---:|---|---:|---:|---:|
| 8 | platform | +0.81 MB | +0.20 MB | +1 MB |
| 8 | virtual | +0.40 MB | +0.25 MB | +0 MB |
| 24 | platform | +2.32 MB | +0.29 MB | +3 MB |
| 24 | virtual | +0.27 MB | +0.33 MB | +1 MB |
| 64 | platform | +4.93 MB | +0.50 MB | +5 MB |
| 64 | virtual | +0.18 MB | +0.64 MB | +3 MB |

The platform stacks scale linearly at **about 77 KB committed per thread**, well under the megabyte
of address space each one reserves and never touches. The virtual arm's stay flat, since only the
carriers are threads. The heap moves the other way by much less, the continuations costing 0.05 to
0.14 MB more than the platform arm's own per-worker objects. Replicate 1 of each arm is the JVM still
warming up and reads erratically (one arm shows RSS falling by 33 MB); replicates 2 and 3 agree
closely, which is what the medians rest on.

**Total saving at 64 workers: under 5 MB of committed stack and about 2 MB of RSS**, in a process
that sits between 200 and 400 MB.

What this run does not capture: a virtual thread's continuation grows while it is unmounted
mid-cycle under load, and these arms are idle. That is heap, and it is bounded by the same few
kilobytes per worker, so it cannot turn single-digit megabytes into anything else.

### A short window does not measure this

The first pass used a 15-second window and one replicate, and produced a 64-worker virtual arm at
p99 239 ms against the platform arm's 120, with the delivered rate down to 294/s. None of it survived
three replicates at 60 seconds. It is recorded here because it would have been a tempting finding to
publish: it pointed the way the theory expected, and it was noise.

## The finding nobody was looking for

**Five of the nine virtual arms ended with an `ERROR` that none of the nine platform arms produced**
(24w virtual #1 and #3, 64w virtual #2 twice, 64w virtual #3; `grep "Relay worker iteration failed"`
in `probe.log`):

```
SEVERE: Relay worker iteration failed workerIndex:54
com.codingful.tandem.core.exception.TandemException: claimBatch failed
  Caused by: org.postgresql.util.PSQLException: An I/O error occurred while sending to the backend
  Caused by: java.net.SocketException: Closed by interrupt
      at java.base/java.lang.VirtualThread.run(VirtualThread.java:456)
```

`WorkerPool.stop()` interrupts its workers. A virtual thread's socket read runs on the interruptible
NIO implementation, so the interrupt aborts the in-flight query and closes the connection; a platform
thread's read completes and the shutdown stays quiet. No row is lost, the lease reclaims whatever was
IN_FLIGHT, but a shutdown under load would log ERROR lines that today mean something is wrong and
return a dead connection to the pool.

It does not appear in `smoke-suite.log` at all: those scenarios are short and lightly loaded, so a
worker is rarely inside a query when the pool stops. It took 400 events/s to make it show, in a bit
over half the arms.

## What this run does not say

- **Nothing about JDK 21 to 23.** The toolchain is 25, where JEP 491 has already removed monitor
  pinning. The arms cannot speak for a JVM that still has it.
- **Nothing about a throughput ceiling.** Every arm was rate-limited on purpose. The question was
  whether the thread kind changes the delivery of a load both kinds can carry, and the answer needs a
  fixed rate, not a ceiling search (which does not reproduce on any host available here, see the
  [archive README](../README.md)).
- **Nothing official.** A developer Mac is not the reference host (HLD-load-testing §5.1). The
  differences that matter here are within-run and within-host, which is what the alternating arms are
  for.
