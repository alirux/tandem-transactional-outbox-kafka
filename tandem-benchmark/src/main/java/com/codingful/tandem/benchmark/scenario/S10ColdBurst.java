package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.EventReceiver;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LatencySnapshot;
import com.codingful.tandem.benchmark.LoadGenerator;
import com.codingful.tandem.benchmark.RelayInstance;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.jdbc.Wakeup;
import com.codingful.tandem.jdbc.WakeupSource;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * S10 — the cold row, and the only scenario that measures the post-commit wakeup
 * (dispatch-latency.md §3.4). Every other scenario drives a stream, where the relay's adaptive backoff
 * sits near its floor and the wakeup has almost nothing left to remove. This one drives the opposite
 * shape: a rate so low that every event arrives into a worker slice that has been silent long enough
 * to be waiting at {@code pollInterval}, which is the one regime the backoff cannot price and a signal
 * can.
 *
 * <p><b>It measures both directions of the trade, because the answer could be either.</b> A wakeup can
 * only help discovery, but it is not free on the way in: with {@link Wakeup#PG_NOTIFY} the write side
 * runs one extra statement inside the caller's transaction. So each arm reports COMMIT→ack end to end
 * <i>and</i> the duration of the write transaction itself, and a run where the second grew more than
 * the first shrank is a run that says do not turn it on.
 *
 * <p><b>The arms alternate ABBA within one run</b> rather than being two runs compared afterwards. A
 * developer machine drifts over minutes (thermal, background work, a growing outbox), and a drift that
 * lands entirely on the second half of an AABB run reads exactly like an effect. Four windows in the
 * order poll, wakeup, wakeup, poll cancel a linear drift, and printing all four is what lets a reader
 * see whether the two windows of an arm even agree with each other.
 *
 * <p>Each window is a complete little run of its own: its own relay instance (with or without a
 * listening connection), its own write side (emitting or silent), a drain, and a correctness check.
 * Only the Kafka consumer and the latency recorder are shared, so that one subscription reads the whole
 * scenario and each window's snapshot is the interval since the previous one.
 */
public final class S10ColdBurst implements Scenario {

    /**
     * The offered rate, unless {@code --rate=} overrides it. Deliberately far below anything the host
     * could be limited by: at 2 events/s over the default 8 workers a slice sees an event every few
     * seconds, so its backoff has long since reached the ceiling. What is being measured is the wait,
     * not the throughput.
     */
    private static final double COLD_RATE_PER_SECOND = 2.0;

    /** How long each window settles before its measurement, so the ramp of a just-started relay is not in it. */
    private static final Duration SETTLE = Duration.ofSeconds(3);

    private static final Duration DRAIN_TIMEOUT = Duration.ofMinutes(2);

    /** The order the four windows run in: ABBA, so a linear drift over the run cancels between the arms. */
    private static final List<Wakeup> WINDOW_ORDER =
            List.of(Wakeup.NONE, Wakeup.PG_NOTIFY, Wakeup.PG_NOTIFY, Wakeup.NONE);

    @Override
    public String id() {
        return "S10";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        double rate = cfg.offeredRate() > 0 ? cfg.offeredRate() : COLD_RATE_PER_SECOND;
        Duration window = windowFor(cfg);

        LatencyRecorder latency = new LatencyRecorder(Duration.ofSeconds(30));
        List<Window> windows = new ArrayList<>();
        try (EventReceiver receiver = env.newReceiver("bench-s10");
             CorrelationConsumer consumer = new CorrelationConsumer(receiver, latency, null)) {
            consumer.start();
            System.out.printf(Locale.ROOT, "S10: %.1f events/s for %s per window, four windows in the order"
                            + " %s; pollInterval=%s, pollFloor=%s, workers=%d%n",
                    rate, window, WINDOW_ORDER, cfg.pollInterval(), cfg.pollIntervalFloor(), cfg.workers());

            for (int i = 0; i < WINDOW_ORDER.size(); i++) {
                windows.add(runWindow(ctx, consumer, latency, i + 1, WINDOW_ORDER.get(i), rate, window));
            }
        }

        Arm poll = Arm.of(Wakeup.NONE, windows);
        Arm wakeup = Arm.of(Wakeup.PG_NOTIFY, windows);
        boolean passed = windows.stream().allMatch(Window::passed);
        return new ScenarioResult(id(), passed, summary(poll, wakeup, rate, windows), metrics(poll, wakeup, rate));
    }

    /**
     * One window: a relay and a write side wired for {@code arm}, a fixed low rate held for
     * {@code window}, then a drain and a correctness check before the snapshot is taken — so no sample
     * of this window can land in the next one's interval.
     */
    private Window runWindow(ScenarioContext ctx, CorrelationConsumer consumer, LatencyRecorder latency,
            int index, Wakeup arm, double rate, Duration window) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        LatencyRecorder writeLatency = new LatencyRecorder(Duration.ofSeconds(30));
        RelayInstance relay = env.newRelayInstance(env.relayConfigBuilder().build(), TandemMetrics.NOOP,
                TandemSpanRecorder.NOOP, WakeupSource.forWakeup(arm, env.dataSource()));
        relay.pool().start();
        try (LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                // A namespace per window: the correctness check reconciles what this window wrote, and
                // aggregates shared with an earlier window would carry its keys into this one's diff.
                AggregateSelector.uniform(id() + "-w" + index, cfg.aggregateCardinality()), cfg.payloadBytes(),
                null, arm)) {
            generator.recordWriteLatency(writeLatency);
            generator.start(rate);
            Thread.sleep(SETTLE.toMillis());
            latency.snapshot();        // discard the settling events
            writeLatency.snapshot();

            Thread.sleep(window.toMillis());

            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id() + "-w" + index, DRAIN_TIMEOUT);
            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);
            Window measured = new Window(index, arm, latency.snapshot(), writeLatency.snapshot(),
                    report.orderingViolations(), report.missingKeys().size());
            System.out.println("S10: " + measured.describe());
            return measured;
        } finally {
            relay.pool().stop();
        }
    }

    /**
     * Each window holds the rate for a quarter of the run, minus its own settling time. Floored so a
     * smoke or demo run still exercises the wiring rather than measuring a window of zero.
     */
    private static Duration windowFor(BenchmarkConfig cfg) {
        return ScenarioSupport.maxDuration(Duration.ofSeconds(5), cfg.duration().dividedBy(WINDOW_ORDER.size()));
    }

    /** One measured window, and everything a reader needs to judge it on its own. */
    private record Window(int index, Wakeup arm, LatencySnapshot latency, LatencySnapshot write,
                          long orderingViolations, int missing) {

        boolean passed() {
            return orderingViolations == 0 && missing == 0;
        }

        String describe() {
            return String.format(Locale.ROOT,
                    "window %d (%s): commit→ack p50=%s p95=%s p99=%s, write p50=%s p99=%s, events=%d,"
                            + " violations=%d, missing=%d",
                    index, label(arm), latency.p50(), latency.p95(), latency.p99(),
                    write.p50(), write.p99(), latency.count(), orderingViolations, missing);
        }
    }

    /** The two windows of one arm, averaged — the figure the two arms are compared on. */
    private record Arm(Wakeup wakeup, double ackP50Millis, double ackP95Millis, double ackP99Millis,
                       double writeP50Millis, double writeP99Millis, long events) {

        static Arm of(Wakeup wakeup, List<Window> windows) {
            List<Window> mine = windows.stream().filter(w -> w.arm() == wakeup).toList();
            return new Arm(wakeup,
                    mean(mine, w -> w.latency().p50()), mean(mine, w -> w.latency().p95()),
                    mean(mine, w -> w.latency().p99()),
                    mean(mine, w -> w.write().p50()), mean(mine, w -> w.write().p99()),
                    mine.stream().mapToLong(w -> w.latency().count()).sum());
        }

        private static double mean(List<Window> windows, java.util.function.Function<Window, Duration> of) {
            return windows.stream().mapToDouble(w -> of.apply(w).toNanos() / 1_000_000.0).average().orElse(0);
        }
    }

    private String summary(Arm poll, Arm wakeup, double rate, List<Window> windows) {
        long violations = windows.stream().mapToLong(Window::orderingViolations).sum();
        int missing = windows.stream().mapToInt(Window::missing).sum();
        return String.format(Locale.ROOT,
                "cold rows at %.1f/s; commit→ack p50 %.1f→%.1f ms (%.1fx), p95 %.1f→%.1f, p99 %.1f→%.1f;"
                        + " write p50 %.2f→%.2f ms, p99 %.2f→%.2f; events %d/%d;"
                        + " ordering violations=%d, missing=%d",
                rate, poll.ackP50Millis(), wakeup.ackP50Millis(), ratio(poll.ackP50Millis(), wakeup.ackP50Millis()),
                poll.ackP95Millis(), wakeup.ackP95Millis(), poll.ackP99Millis(), wakeup.ackP99Millis(),
                poll.writeP50Millis(), wakeup.writeP50Millis(), poll.writeP99Millis(), wakeup.writeP99Millis(),
                poll.events(), wakeup.events(), violations, missing);
    }

    private Map<String, Object> metrics(Arm poll, Arm wakeup, double rate) {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("coldRatePerSecond", rate);
        metrics.put("pollAckP50Millis", poll.ackP50Millis());
        metrics.put("pollAckP95Millis", poll.ackP95Millis());
        metrics.put("pollAckP99Millis", poll.ackP99Millis());
        metrics.put("wakeupAckP50Millis", wakeup.ackP50Millis());
        metrics.put("wakeupAckP95Millis", wakeup.ackP95Millis());
        metrics.put("wakeupAckP99Millis", wakeup.ackP99Millis());
        metrics.put("ackP50Speedup", ratio(poll.ackP50Millis(), wakeup.ackP50Millis()));
        metrics.put("pollWriteP50Millis", poll.writeP50Millis());
        metrics.put("pollWriteP99Millis", poll.writeP99Millis());
        metrics.put("wakeupWriteP50Millis", wakeup.writeP50Millis());
        metrics.put("wakeupWriteP99Millis", wakeup.writeP99Millis());
        metrics.put("pollEvents", poll.events());
        metrics.put("wakeupEvents", wakeup.events());
        return metrics;
    }

    /** How many times better the wakeup arm is; {@code 0} where the poll arm recorded nothing to compare. */
    private static double ratio(double pollMillis, double wakeupMillis) {
        return wakeupMillis <= 0 ? 0 : pollMillis / wakeupMillis;
    }

    private static String label(Wakeup wakeup) {
        return wakeup == Wakeup.NONE ? "poll" : "wakeup";
    }
}
