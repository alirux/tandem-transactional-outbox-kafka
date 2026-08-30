package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LagProbe;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LatencySnapshot;
import com.codingful.tandem.benchmark.LoadGenerator;
import com.codingful.tandem.benchmark.RampController;
import com.codingful.tandem.benchmark.RelayInstance;
import com.codingful.tandem.benchmark.SequenceLedger;
import com.codingful.tandem.jdbc.Coordination;
import com.codingful.tandem.jdbc.RelayConfig;
import java.lang.management.ManagementFactory;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * S9 — endurance: a moderate load held for hours under {@code LEASE} coordination, reported window by
 * window, to answer the one question a burst cannot (HLD-load-testing.md §4).
 *
 * <p>Every other scenario measures the first few minutes of a relay's life. The failure modes an outbox
 * dies of in production are the slow ones — a leak, index bloat on a table churning
 * {@code PENDING → IN_FLIGHT → DONE}, autovacuum falling behind, a lease renewal path that misbehaves
 * only after many periods — and each of them shows up as a <b>slope</b>, which a four-minute window
 * reads as noise. So this scenario changes one thing about how it measures: instead of one aggregate
 * number for the run, it takes a snapshot per window and compares the last against the first.
 *
 * <p>Three properties make it an endurance test rather than a long S2:
 * <ul>
 *   <li><b>The rate is fixed, never ramped.</b> Two windows hours apart are only comparable at the
 *       same offered load; and a rate search is the one thing that pushes a host to its ceiling, where
 *       a burstable instance or a warm laptop stops being a measuring instrument. When no rate is
 *       given, a short bounded ramp seeds one — with a budget of its own, not one scaled to the run —
 *       and the run holds half of it.</li>
 *   <li><b>Correctness is tracked in bounded memory</b> ({@link SequenceLedger}): the per-event key
 *       sets the short scenarios reconcile with would, over hours, make the harness's own GC the
 *       loudest signal in the measurement.</li>
 *   <li><b>Bucket coverage is sampled every window</b>, not once: under {@code LEASE} the ownership
 *       partition is re-established on every renewal cycle, so "every bucket owned by exactly one live
 *       instance" is a property that has to keep being true, not one to check after startup.</li>
 * </ul>
 *
 * <p>Drift is reported, never asserted: {@code passed} stays correctness-only, as everywhere else
 * ({@link ScenarioResult}) — on a non-reference host a throughput slope can belong to the host, and a
 * scenario that failed the build over it would be measuring the laptop's cooling.
 */
public final class S9Endurance implements Scenario {

    private static final int INSTANCE_COUNT = 2;
    private static final Duration BUCKET_LEASE = Duration.ofSeconds(30);
    private static final Duration RECLAIM_INTERVAL = Duration.ofSeconds(2);
    private static final Duration PARTITION_TIMEOUT = Duration.ofMinutes(2);

    /**
     * The seed ramp's ceiling on its own time, deliberately independent of {@code duration}: the ramp
     * exists to place the load, and letting it scale with the run is what makes an hours-long scenario
     * spend hours at the ceiling. It still shrinks for a run too short to contain it (smoke/demo),
     * which then measures nothing about the ceiling and simply holds the seed rate — correct for a
     * wiring check.
     */
    private static final Duration SEED_RAMP_OBSERVATION = Duration.ofSeconds(5);
    private static final Duration SEED_RAMP_SUSTAIN = Duration.ofSeconds(30);
    private static final Duration SEED_RAMP_BUDGET = Duration.ofMinutes(3);
    private static final double SEED_RAMP_TOLERANCE = 0.15;
    private static final double SEED_RATE_PER_SECOND = 100.0;

    /** How much of the seeded ceiling the run holds: low enough that the host is not the thing under test. */
    private static final double LOAD_FRACTION = 0.5;

    private static final Duration DELIVERY_CATCH_UP = Duration.ofMinutes(2);

    @Override
    public String id() {
        return "S9";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();

        List<RelayInstance> instances = new ArrayList<>();
        for (int i = 1; i <= INSTANCE_COUNT; i++) {
            RelayInstance instance = env.newRelayInstance(leaseConfig(env, "s9-instance-" + i));
            instances.add(instance);
            instance.pool().start();
        }

        LatencyRecorder latency = new LatencyRecorder(Duration.ofSeconds(10));
        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s9");
             // Key tracking off: over hours the reconciliation set, not the outbox, would be what the
             // harness spends its memory on (SequenceLedger).
             CorrelationConsumer consumer = new CorrelationConsumer(kafkaConsumer, latency, null, false);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     AggregateSelector.uniform(id(), cfg.aggregateCardinality()), cfg.payloadBytes(), null, cfg.wakeup())) {
            generator.stopTrackingInsertedKeys();
            consumer.start();

            OfferedRate offered = offeredRate(env, cfg, generator);
            double rate = offered.ratePerSecond();
            generator.setRate(rate);
            ScenarioSupport.awaitUpTo(PARTITION_TIMEOUT,
                    () -> ScenarioSupport.coverage(instances).complete(cfg.bucketCount()));

            Thread.sleep(cfg.warmup().toMillis());
            latency.snapshot();   // discard the warm-up window (HLD-load-testing.md §3)

            List<Window> windows = holdAndSample(ctx, generator, consumer, latency, instances, offered);

            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(),
                    ScenarioSupport.maxDuration(Duration.ofMinutes(5), cfg.duration().dividedBy(4)));
            Map<String, Long> written = generator.lastInsertedSeqs();
            SequenceLedger ledger = consumer.ledger();
            // The outbox has drained, but the co-located consumer polls on its own thread and can lag
            // that by a cycle — the same bounded grace ScenarioSupport.verify gives the set-diff.
            awaitDelivery(ledger, written);

            long lost = ledger.lostEvents(written);
            boolean coverageHeld = windows.stream().allMatch(w -> w.coverage().complete(cfg.bucketCount()));
            boolean passed = ledger.orderingViolations() == 0 && lost == 0 && coverageHeld;
            return new ScenarioResult(id(), passed, summary(windows, offered, lost, ledger, coverageHeld, cfg),
                    metrics(windows, offered, lost, ledger, coverageHeld));
        } finally {
            for (RelayInstance instance : instances) {
                instance.pool().stop();
            }
        }
    }

    /** The load this run held, and what that figure is a fraction of ({@link ScenarioSupport.RateBasis}). */
    private record OfferedRate(double ratePerSecond, ScenarioSupport.RateBasis basis) {

        String describe() {
            return basis.describe(LOAD_FRACTION);
        }
    }

    /** One reporting window's observations — the row of the table this scenario prints as it runs. */
    private record Window(int index, long delivered, long written, LatencySnapshot latency, LagProbe.Lag lag,
                          LagProbe.Storage storage, ScenarioSupport.Coverage coverage, long heapMegabytes,
                          int threads, double loadAverage) {

        double deliveredPerSecond(Duration window) {
            return delivered / (double) window.toSeconds();
        }
    }

    /**
     * The run itself: hold the rate, and once per window snapshot everything that could drift. Each
     * window is printed as it completes rather than at the end — an hours-long run must be readable
     * while it is still going, and a run that dies in hour five should still leave four hours of
     * evidence behind.
     */
    private List<Window> holdAndSample(ScenarioContext ctx, LoadGenerator generator, CorrelationConsumer consumer,
            LatencyRecorder latency, List<RelayInstance> instances, OfferedRate offered) throws InterruptedException {
        BenchmarkConfig cfg = ctx.config();
        Duration window = windowFor(cfg);
        int windowCount = (int) (cfg.duration().toSeconds() / window.toSeconds());
        // The cleanup policy belongs in the run's own log: whether DONE rows were being deleted while
        // the run held decides whether a growing outbox is a finding or the configuration.
        System.out.printf(Locale.ROOT, "S9: holding %.1f events/s (%s) for %s in %d windows of %s; "
                        + "retention=%s, cleanup every %s in batches of %d%n",
                offered.ratePerSecond(), offered.describe(), cfg.duration(), windowCount, window,
                cfg.retention(), cfg.cleanupInterval(), cfg.cleanupBatchSize());

        List<Window> windows = new ArrayList<>();
        long previousDelivered = consumer.receivedCount();
        long previousWritten = generator.succeeded();
        for (int i = 1; i <= windowCount; i++) {
            Thread.sleep(window.toMillis());
            long delivered = consumer.receivedCount();
            long written = generator.succeeded();
            Window sample = new Window(i, delivered - previousDelivered, written - previousWritten,
                    latency.snapshot(), ctx.environment().lagProbe().overall(),
                    ctx.environment().lagProbe().storage(), ScenarioSupport.coverage(instances),
                    heapMegabytes(), ManagementFactory.getThreadMXBean().getThreadCount(),
                    ManagementFactory.getOperatingSystemMXBean().getSystemLoadAverage());
            previousDelivered = delivered;
            previousWritten = written;
            windows.add(sample);
            printWindow(sample, window, cfg.bucketCount());
        }
        return windows;
    }

    private static void printWindow(Window w, Duration window, int bucketCount) {
        System.out.printf(Locale.ROOT, "S9 window %2d @%s: %.1f/s delivered (%.1f/s written), p50=%dms p99=%dms, "
                        + "pending=%d oldest=%ds, buckets %s covered=%d/%d, outbox %dMB (%dMB idx, %d rows, "
                        + "%d dead), heap=%dMB threads=%d load=%.2f%n",
                w.index(), LocalTime.now().truncatedTo(ChronoUnit.SECONDS), w.deliveredPerSecond(window),
                w.written() / (double) window.toSeconds(), w.latency().p50().toMillis(), w.latency().p99().toMillis(),
                w.lag().pending(), w.lag().age().toSeconds(), w.coverage().perInstance(),
                w.coverage().covered(), bucketCount, w.storage().totalMegabytes(), w.storage().indexMegabytes(),
                w.storage().rowCount(), w.storage().deadTuples(), w.heapMegabytes(), w.threads(), w.loadAverage());
    }

    /**
     * The configured window, shrunk when the run is too short to hold two of them — the drift comparison
     * needs a first and a last, so a demo/smoke-sized run gets narrow windows rather than none.
     */
    private static Duration windowFor(BenchmarkConfig cfg) {
        Duration half = cfg.duration().dividedBy(2);
        return cfg.window().compareTo(half) <= 0 ? cfg.window() : half;
    }

    private static OfferedRate offeredRate(BenchmarkEnvironment env, BenchmarkConfig cfg, LoadGenerator generator) {
        if (cfg.offeredRate() > 0) {
            generator.start(cfg.offeredRate());
            return new OfferedRate(cfg.offeredRate(), ScenarioSupport.RateBasis.EXPLICIT);
        }
        Duration sustain = ScenarioSupport.minDuration(SEED_RAMP_SUSTAIN, cfg.duration().dividedBy(2));
        Duration observation = ScenarioSupport.maxDuration(Duration.ofSeconds(1),
                ScenarioSupport.minDuration(SEED_RAMP_OBSERVATION, sustain.dividedBy(3)));
        Duration budget = ScenarioSupport.minDuration(SEED_RAMP_BUDGET, cfg.duration().multipliedBy(2));
        RampController ramp = new RampController(env.lagProbe(), observation, sustain, SEED_RAMP_TOLERANCE,
                cfg.batchSize());
        RampController.RampResult seed = ramp.findSustainableMax(generator, SEED_RATE_PER_SECOND, budget);
        // A seed ramp that never found a failing rate reports a lower bound, not a ceiling, and half of
        // it is half of whatever the budget reached. The run is still valid — a fixed rate held for
        // hours is what this scenario measures — but it must not claim to be holding half of capacity.
        return new OfferedRate(Math.max(1.0, seed.sustainedRatePerSecond() * LOAD_FRACTION),
                ScenarioSupport.RateBasis.of(seed));
    }

    private static void awaitDelivery(SequenceLedger ledger, Map<String, Long> written) throws InterruptedException {
        Instant deadline = Instant.now().plus(DELIVERY_CATCH_UP);
        while (!ledger.caughtUpWith(written) && Instant.now().isBefore(deadline)) {
            Thread.sleep(500);
        }
    }

    private static String summary(List<Window> windows, OfferedRate offered, long lost, SequenceLedger ledger,
            boolean coverageHeld, BenchmarkConfig cfg) {
        Window first = windows.get(0);
        Window last = windows.get(windows.size() - 1);
        return String.format(Locale.ROOT, "held %.1f events/s (%s) for %s in %d windows; throughput %.1f/s → %.1f/s (%+.1f%%), "
                        + "p50 %dms → %dms, p99 %dms → %dms; outbox %dMB → %dMB, heap %dMB → %dMB; "
                        + "bucket coverage held=%s; ordering violations=%d, lost=%d, duplicates=%d",
                offered.ratePerSecond(), offered.describe(), cfg.duration(), windows.size(),
                first.deliveredPerSecond(windowFor(cfg)), last.deliveredPerSecond(windowFor(cfg)),
                drift(first.deliveredPerSecond(windowFor(cfg)), last.deliveredPerSecond(windowFor(cfg))),
                first.latency().p50().toMillis(), last.latency().p50().toMillis(),
                first.latency().p99().toMillis(), last.latency().p99().toMillis(),
                first.storage().totalMegabytes(), last.storage().totalMegabytes(),
                first.heapMegabytes(), last.heapMegabytes(), coverageHeld,
                ledger.orderingViolations(), lost, ledger.duplicates());
    }

    private static Map<String, Object> metrics(List<Window> windows, OfferedRate offered, long lost,
            SequenceLedger ledger, boolean coverageHeld) {
        Window first = windows.get(0);
        Window last = windows.get(windows.size() - 1);
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("offeredRatePerSecond", offered.ratePerSecond());
        metrics.put("offeredRateBasis", offered.basis().name());
        metrics.put("windows", windows.size());
        metrics.put("firstWindowDelivered", first.delivered());
        metrics.put("lastWindowDelivered", last.delivered());
        metrics.put("throughputDriftPercent", drift(first.delivered(), last.delivered()));
        metrics.put("firstWindowP50Millis", first.latency().p50().toMillis());
        metrics.put("lastWindowP50Millis", last.latency().p50().toMillis());
        metrics.put("firstWindowP99Millis", first.latency().p99().toMillis());
        metrics.put("lastWindowP99Millis", last.latency().p99().toMillis());
        metrics.put("firstWindowHeapMegabytes", first.heapMegabytes());
        metrics.put("lastWindowHeapMegabytes", last.heapMegabytes());
        metrics.put("firstWindowOutboxMegabytes", first.storage().totalMegabytes());
        metrics.put("lastWindowOutboxMegabytes", last.storage().totalMegabytes());
        metrics.put("lastWindowDeadTuples", last.storage().deadTuples());
        metrics.put("bucketCoverageHeld", coverageHeld);
        metrics.put("orderingViolations", ledger.orderingViolations());
        metrics.put("lostCount", lost);
        metrics.put("duplicateCount", ledger.duplicates());
        return metrics;
    }

    /** Percentage change from {@code first} to {@code last}; 0 when there is nothing to compare against. */
    private static double drift(double first, double last) {
        return first == 0 ? 0 : (last - first) / first * 100.0;
    }

    private static long heapMegabytes() {
        Runtime runtime = Runtime.getRuntime();
        return (runtime.totalMemory() - runtime.freeMemory()) / (1024 * 1024);
    }

    private static RelayConfig leaseConfig(BenchmarkEnvironment env, String instanceId) {
        return env.relayConfigBuilder()
                .coordination(Coordination.LEASE)
                .instanceId(instanceId)
                .bucketLease(BUCKET_LEASE)
                .reclaimInterval(RECLAIM_INTERVAL)
                .build();
    }
}
