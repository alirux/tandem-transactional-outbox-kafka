package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.EventReceiver;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LoadGenerator;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.OptionalLong;

/**
 * S12, broker outage (LLD-benchmark §8, §3.1). The <b>broker</b> is frozen while the relay and the write
 * side keep running, and then thawed: the question is whether every event still arrives, in order,
 * once it answers again.
 *
 * <p>S11 stops the relay, which is the failure an operator causes; this one is the failure that
 * happens to them. The two exercise opposite halves of the dispatcher: a stopped relay publishes
 * nothing, while a frozen broker leaves publishes in flight with no answer coming, which is where an
 * adapter that tracks them in a structure of its own can lose an entry, resolve one twice, or hand a
 * confirm to the wrong row once the connection comes back and the broker's own delivery numbering
 * restarts.
 *
 * <p>That is also why the in-flight count is checked at the end rather than only the delivered set:
 * an adapter can deliver everything and still leak the bookkeeping behind it, and a leak is invisible
 * in any window shorter than the process's life
 * ({@link com.codingful.tandem.benchmark.BrokerHarness#inFlightPublishes()}). Under
 * {@code --broker=kafka} the producer keeps its in-flight batches to itself, so the scenario gates
 * delivery alone; under {@code --broker=rabbit} it gates both.
 *
 * <p>The outage is deliberately sized well inside the retry budget: every failed publish costs the
 * row an attempt, and a row that burns all {@code maxAttempts} is quarantined to {@code FAILED},
 * which is correct behaviour and not a delivery this scenario could wait for. When it happens anyway
 * the count is reported rather than hidden, because "lost" and "quarantined" are different findings.
 */
public final class S12BrokerOutage implements Scenario {

    /** Modest and fixed: the subject is the interruption, not the ceiling. */
    private static final double NOMINAL_RATE_PER_SECOND = 200.0;

    private static final Duration NOMINAL_OUTAGE = Duration.ofSeconds(20);

    /** Outage length as a share of the run, so a smoke run freezes the broker for a moment, not for its whole duration. */
    private static final double OUTAGE_FRACTION = 0.2;

    @Override
    public String id() {
        return "S12";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        double rate = cfg.offeredRate() > 0 ? cfg.offeredRate() : NOMINAL_RATE_PER_SECOND;
        Duration outage = outageFor(cfg);

        env.relayPool().start();
        try (EventReceiver receiver = env.newReceiver("bench-s12");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     receiver, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     AggregateSelector.uniform(id(), cfg.aggregateCardinality()), cfg.payloadBytes(), null,
                     cfg.wakeup())) {
            consumer.start();
            generator.start(rate);
            Thread.sleep(cfg.warmup().toMillis());

            long inFlightBefore = freeze(env, outage);
            Thread.sleep(cfg.duration().toMillis());

            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(),
                    ScenarioSupport.maxDuration(Duration.ofMinutes(10), cfg.duration()));

            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);
            OptionalLong inFlightAfter = env.broker().inFlightPublishes();
            long quarantined = env.lagProbe().failedForNamespace(id());
            boolean settled = inFlightAfter.isEmpty() || inFlightAfter.getAsLong() == 0;

            return new ScenarioResult(id(), report.passed() && settled,
                    summary(outage, report, inFlightBefore, inFlightAfter, quarantined),
                    metrics(outage, rate, report, inFlightAfter, quarantined));
        } finally {
            env.relayPool().stop();
        }
    }

    /**
     * Freezes the broker for {@code outage} and thaws it again, reporting how many publishes were
     * unanswered at the moment it came back: the population the recovery path has to resolve, and
     * zero if the client had already given up on all of them.
     */
    private long freeze(BenchmarkEnvironment env, Duration outage) throws InterruptedException {
        System.out.printf("S12: freezing the broker for %ds%n", outage.toSeconds());
        env.broker().interruptBroker();
        try {
            Thread.sleep(outage.toMillis());
            return env.broker().inFlightPublishes().orElse(-1);
        } finally {
            env.broker().restoreBroker();
            System.out.println("S12: broker restored, draining what the outage left behind");
        }
    }

    private static Duration outageFor(BenchmarkConfig cfg) {
        Duration scaled = Duration.ofMillis(Math.round(cfg.duration().toMillis() * OUTAGE_FRACTION));
        return ScenarioSupport.minDuration(NOMINAL_OUTAGE, scaled);
    }

    private static String summary(Duration outage, ScenarioSupport.CorrectnessReport report,
            long inFlightBefore, OptionalLong inFlightAfter, long quarantined) {
        String unanswered = inFlightBefore < 0 ? "not observable" : String.valueOf(inFlightBefore);
        String settled = inFlightAfter.isEmpty() ? "not observable"
                : inFlightAfter.getAsLong() + " still unsettled";
        return String.format("%ds broker outage; %s publishes unanswered at thaw, %s afterwards; "
                        + "%d missing, %d ordering violations, %d duplicates, %d rows quarantined",
                outage.toSeconds(), unanswered, settled, report.missingKeys().size(),
                report.orderingViolations(), report.duplicateCount(), quarantined);
    }

    private static Map<String, Object> metrics(Duration outage, double rate,
            ScenarioSupport.CorrectnessReport report, OptionalLong inFlightAfter, long quarantined) {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("outageSeconds", outage.toSeconds());
        metrics.put("offeredRatePerSecond", rate);
        metrics.put("missingKeys", report.missingKeys().size());
        metrics.put("orderingViolations", report.orderingViolations());
        metrics.put("duplicates", report.duplicateCount());
        metrics.put("quarantinedRows", quarantined);
        metrics.put("inFlightAfterDrain", inFlightAfter.isEmpty() ? "n/a" : inFlightAfter.getAsLong());
        return metrics;
    }
}
