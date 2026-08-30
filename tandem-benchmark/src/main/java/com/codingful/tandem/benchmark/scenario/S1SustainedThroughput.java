package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LoadGenerator;
import com.codingful.tandem.benchmark.RampController;
import java.time.Duration;
import java.util.Map;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * S1 — sustained max throughput (HLD-load-testing.md §4): ramps the offered rate until the lag age
 * stops staying flat, and reports the highest rate held with a flat lag for the sustain window
 * (LLD-benchmark §7).
 */
public final class S1SustainedThroughput implements Scenario {

    /** How tightly the rate search brackets the ceiling before reporting it (LLD-benchmark §7). */
    private static final double RATE_TOLERANCE = 0.05;

    @Override
    public String id() {
        return "S1";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        env.relayPool().start();
        try (KafkaConsumer<String, byte[]> kafkaConsumer = env.newConsumer("bench-s1");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     kafkaConsumer, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     AggregateSelector.uniform(id(), cfg.aggregateCardinality()), cfg.payloadBytes(), null, cfg.wakeup())) {
            consumer.start();

            RampController ramp = new RampController(env.lagProbe(),
                    ScenarioSupport.observationWindowFor(cfg), ScenarioSupport.sustainWindowFor(cfg),
                    RATE_TOLERANCE, cfg.batchSize());
            Duration searchBudget = cfg.duration().multipliedBy(2);
            RampController.RampResult result = ramp.findSustainableMax(generator, 100.0, searchBudget);

            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(), ScenarioSupport.maxDuration(Duration.ofMinutes(2), cfg.duration()));
            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);

            double perWorker = result.sustainedRatePerSecond() / cfg.workers();
            // An unbracketed search never found a rate the host could not hold, so it measured the
            // budget and the seed rather than the host. Saying so is the whole point: a figure like
            // that reads exactly like a KPI once it reaches a README, and this scenario is where the
            // distinction is still visible.
            String throughput = result.bracketed()
                    ? String.format("sustained %.1f events/s aggregate (%.1f/s per worker); ceiling bracketed "
                                    + "below %.1f/s, so known to within %.0f%%",
                            result.sustainedRatePerSecond(), perWorker, result.lowestFailingRatePerSecond(),
                            result.relativeUncertainty() * 100)
                    : String.format("NOT A MAXIMUM — the search never found a rate this host could not hold, "
                                    + "so %.1f events/s aggregate (%.1f/s per worker) is only a lower bound set by the "
                                    + "search budget; re-run with a longer duration",
                            result.sustainedRatePerSecond(), perWorker);
            return new ScenarioResult(id(), report.passed(),
                    String.format("%s; ordering violations=%d, missing=%d",
                            throughput, report.orderingViolations(), report.missingKeys().size()),
                    Map.of(
                            "aggregateRatePerSecond", result.sustainedRatePerSecond(),
                            "perWorkerRatePerSecond", perWorker,
                            "bracketed", result.bracketed() ? 1 : 0,
                            "relativeUncertainty", result.relativeUncertainty(),
                            "orderingViolations", report.orderingViolations(),
                            "missingCount", report.missingKeys().size()));
        } finally {
            env.relayPool().stop();
        }
    }
}
