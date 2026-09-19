package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.AggregateSelector;
import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.BenchmarkEnvironment;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.EventReceiver;
import com.codingful.tandem.benchmark.LagProbe;
import com.codingful.tandem.benchmark.LatencyRecorder;
import com.codingful.tandem.benchmark.LoadGenerator;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * S11 — outage recovery (HLD-load-testing.md §4, LLD-benchmark §8). The relay is stopped while the
 * write side keeps going, so a backlog of ordinary pending rows builds up, and then it is started
 * again: the measurement is how long the outbox takes to come back to its steady state, and whether
 * it comes back at all.
 *
 * <p>Every other scenario measures a relay that is keeping up. This one measures the state an
 * operator actually gets paged for, and it exists because recovery is not simply "the backlog divided
 * by the drain rate": the claim has to find, among the pending rows, the ones the head-of-chain gate
 * lets it take, so the cost of delivering one row depends on how many rows it had to look past.
 *
 * <p><b>The write-side population is load-bearing here, more than the rate is.</b> This scenario uses
 * {@link AggregateSelector#lifecycle} rather than the fixed cardinality the other scenarios use.
 * Under a fixed cardinality each aggregate's chain grows for as long as the run lasts, so a bigger
 * backlog means longer chains, fewer claimable rows as a share of it, and a per-row cost that climbs
 * with the backlog, giving a recovery curve that is a property of the generator, not of the relay. A real
 * write side creates entities that emit a handful of events and go quiet, so chain length stays
 * bounded while the number of chains grows. Reading a recovery number without knowing which of the
 * two produced it tells you very little.
 *
 * <p><b>Scope: this measures a single relay.</b> It uses the shared {@code relayPool()} rather than
 * {@code LEASE} instances, so the recovery it reports is the claim's own behaviour and not the
 * coordination around it. That leaves one thing deliberately unmeasured: under {@code LEASE} the
 * post-commit wakeup is a broadcast, so every instance wakes a worker for every signal including the
 * buckets it does not own, and that worker claims nothing. A recovery is the worst moment for it,
 * because it is exactly when a wasted claim is expensive. Measuring that needs a {@code LEASE} variant
 * and the ownership filter it argues for, and belongs with the wakeup's own cost work rather than
 * here.
 *
 * <p>A cell either recovers, is <b>diverging</b> (pending is above the backlog the recovery started
 * from, sustained: the relay is no longer keeping up with the offered rate at all), or simply does not
 * finish inside its bound. None of the three is a failure: each is recorded and the ladder stops
 * there, because a backlog that is already diverging cannot inform the next cell and
 * would leave the final drain unreachable. {@code passed} stays correctness-only, as everywhere else
 * ({@link ScenarioResult}): an outage must not lose, duplicate or reorder anything, and that is the
 * part worth failing a build over.
 */
public final class S11OutageRecovery implements Scenario {

    /**
     * The rate the write side holds, unless {@code --rate=} overrides it. A write side that never
     * stops is this scenario's premise, so there is no meaningful zero: an outage over an outbox
     * nobody is filling leaves no backlog to recover from, and reports the recovery of one anyway.
     * Modest rather than near the host's ceiling, because what makes a recovery readable is the
     * headroom between the offered rate and the drain rate: offered close to capacity, the backlog
     * crawls down (or diverges) for reasons belonging to the host rather than to the claim.
     */
    private static final double NOMINAL_RATE_PER_SECOND = 200.0;

    /** Events one aggregate emits over its life, before it retires and is replaced by a new one. */
    private static final int EVENTS_PER_AGGREGATE = 100;

    /** How much that quota varies between aggregates, so chains are not all the same length. */
    private static final double QUOTA_JITTER = 0.2;

    /**
     * Ceiling on how many aggregates are active at once; the actual figure is four times the
     * configured cardinality, which is this value in the default config and scales down with it so a
     * smoke run does not seed thousands of ids for a handful of events. It must stay well above the
     * worker count: below it the workers queue on the head-of-chain gate rather than on the outbox,
     * and the run measures the generator's concurrency instead of the relay's.
     */
    private static final int MAX_CONCURRENT_AGGREGATES = 4096;

    /** Outage lengths, as fractions of the scenario's duration, clamped by {@link #outageFor}. */
    private static final List<Double> OUTAGE_FRACTIONS = List.of(0.05, 0.10, 0.20);

    private static final Duration MAX_OUTAGE = Duration.ofMinutes(5);
    private static final Duration NOMINAL_MIN_OUTAGE = Duration.ofSeconds(5);
    private static final Duration NOMINAL_SETTLE = Duration.ofSeconds(20);

    /** How often the recovery is checked against the backlog it started from, and how many such checks in a row end it. */
    private static final Duration DIVERGENCE_CHECK = Duration.ofSeconds(10);
    private static final int DIVERGENCE_CHECKS = 3;

    @Override
    public String id() {
        return "S11";
    }

    @Override
    public ScenarioResult run(ScenarioContext ctx) throws Exception {
        BenchmarkEnvironment env = ctx.environment();
        BenchmarkConfig cfg = ctx.config();
        double rate = cfg.offeredRate() > 0 ? cfg.offeredRate() : NOMINAL_RATE_PER_SECOND;

        env.relayPool().start();
        try (EventReceiver receiver = env.newReceiver("bench-s11");
             CorrelationConsumer consumer = new CorrelationConsumer(
                     receiver, new LatencyRecorder(Duration.ofSeconds(10)), null);
             LoadGenerator generator = new LoadGenerator(env.dataSource(), cfg.bucketCount(), cfg.maxConnections(),
                     population(cfg, rate), cfg.payloadBytes(), null, cfg.wakeup())) {
            consumer.start();
            generator.start(rate);
            System.out.printf(Locale.ROOT, "S11: %.1f events/s held throughout, outage ladder %s%n",
                    rate, ladderFor(cfg));
            Thread.sleep(cfg.warmup().toMillis());

            List<Cell> cells = new ArrayList<>();
            for (Duration outage : ladderFor(cfg)) {
                Cell cell = measureOutage(env, cfg, rate, outage);
                cells.add(cell);
                if (!cell.recovered()) {
                    break;
                }
                Thread.sleep(settleFor(cfg).toMillis());
            }

            generator.stop();
            ScenarioSupport.waitForDrain(env.lagProbe(), id(),
                    ScenarioSupport.maxDuration(Duration.ofMinutes(10), cfg.duration()));

            ScenarioSupport.CorrectnessReport report = ScenarioSupport.verify(generator, consumer);
            return new ScenarioResult(id(), report.passed(), summary(cells, report),
                    metrics(cells, rate, concurrentAggregates(cfg), report));
        } finally {
            env.relayPool().stop();
        }
    }

    /**
     * Stops the relay for {@code outage}, restarts it, and times the return to the steady state. The
     * backlog is read after the relay is down rather than predicted from the rate, because what the
     * recovery has to work off is what is actually there.
     */
    private Cell measureOutage(BenchmarkEnvironment env, BenchmarkConfig cfg, double rate, Duration outage)
            throws InterruptedException {
        LagProbe probe = env.lagProbe();
        long steadyPending = probe.inProgressForNamespace(id()).pending();

        // Progress goes to stdout, not to a logger: it is the scenario's report, same rule as every
        // other scenario's result line. Without it a run with a long ladder is silent for an hour.
        System.out.printf(Locale.ROOT, "S11 outage start: relay down for %ds (steady pending %d)%n",
                outage.toSeconds(), steadyPending);
        env.relayPool().stop();
        Thread.sleep(outage.toMillis());
        long backlog = probe.inProgressForNamespace(id()).pending();
        System.out.printf(Locale.ROOT, "S11 outage end: backlog %d rows; restarting the relay%n", backlog);

        // "Recovered" is a return to the steady state, not to zero: the write side never stopped.
        long target = Math.max(steadyPending * 2, Math.round(rate * 2));
        Duration bound = boundFor(outage);

        Instant start = Instant.now();
        env.relayPool().start();
        Instant deadline = start.plus(bound);
        long remaining = backlog;
        Instant nextCheck = start.plus(DIVERGENCE_CHECK);
        int worseThanStart = 0;
        boolean diverging = false;
        while (Instant.now().isBefore(deadline)) {
            remaining = probe.inProgressForNamespace(id()).pending();
            if (remaining <= target) {
                break;
            }
            // Diverging means the backlog is worse than the one the recovery started from, sustained.
            // Comparing each sample with the previous one instead (the first version of this) calls
            // a healthy recovery diverging: the write side never stopped, so pending wobbles upward
            // for a few seconds whenever the drain rate dips below the offered rate, and a run that
            // went from 359 119 rows down to 51 118 was reported as diverging on one of those wobbles.
            // Below the starting backlog the recovery is making progress however slowly, and it is the
            // deadline, not this check, that decides whether slow is too slow.
            if (!Instant.now().isBefore(nextCheck)) {
                worseThanStart = remaining > backlog ? worseThanStart + 1 : 0;
                nextCheck = nextCheck.plus(DIVERGENCE_CHECK);
                if (worseThanStart >= DIVERGENCE_CHECKS) {
                    diverging = true;
                    break;
                }
            }
            Thread.sleep(200);
        }
        Duration recovery = Duration.between(start, Instant.now());
        boolean recovered = remaining <= target;
        System.out.printf(Locale.ROOT, "S11 %s: %d rows, %.1fs, %d still pending%n",
                recovered ? "recovered" : (diverging ? "DIVERGING" : "did not recover"),
                backlog, recovery.toMillis() / 1000.0, remaining);
        return new Cell(outage, backlog, recovery, recovered, diverging, remaining, bound);
    }

    private AggregateSelector population(BenchmarkConfig cfg, double rate) {
        // Sized from the ladder the run will actually work through, not from `duration`: with an
        // explicit `--outages=` the duration is only a drain bound, and sizing off it seeds tens of
        // thousands of aggregates a run never reaches. Every phase of a cell is bounded (the outage,
        // then a recovery no longer than boundFor(outage), then the settle), so their sum is the
        // longest the write side can stay up, and an id space sized from it cannot run out. Running
        // out matters more than the spare ids cost: it throws on the generator's own thread, so the
        // write side stops silently, and it would do so during whichever cell ran long enough to
        // overshoot the estimate, the one cell whose measurement needs the write side still there.
        Duration wallClock = cfg.warmup();
        for (Duration outage : ladderFor(cfg)) {
            wallClock = wallClock.plus(outage).plus(boundFor(outage)).plus(settleFor(cfg));
        }
        long expectedEvents = Math.round(rate * wallClock.toSeconds()) + 1;
        int active = concurrentAggregates(cfg);
        int universe = AggregateSelector.universeSizeFor(
                expectedEvents, EVENTS_PER_AGGREGATE, QUOTA_JITTER, active);
        return AggregateSelector.lifecycle(id(), EVENTS_PER_AGGREGATE, QUOTA_JITTER, active, universe);
    }

    /**
     * The outage ladder: {@code --outages=} verbatim when given, otherwise fractions of the run's own
     * duration. An explicit list is deliberately not clamped: a caller naming a fifteen-minute outage
     * is describing the failure they want measured, and silently shortening it to the cap would report
     * a different experiment under the same name.
     */
    private static List<Duration> ladderFor(BenchmarkConfig cfg) {
        if (!cfg.outages().isEmpty()) {
            return cfg.outages();
        }
        return OUTAGE_FRACTIONS.stream().map(fraction -> outageFor(cfg, fraction)).toList();
    }

    private static Duration outageFor(BenchmarkConfig cfg, double fraction) {
        Duration scaled = Duration.ofMillis(Math.round(cfg.duration().toMillis() * fraction));
        // The floor scales with the run: a fixed 5 s minimum would make each cell of a 3 s smoke run
        // longer than the run itself, and the three cells would dominate CI for no measurement.
        Duration floor = ScenarioSupport.minDuration(NOMINAL_MIN_OUTAGE, cfg.duration().dividedBy(3));
        return ScenarioSupport.minDuration(MAX_OUTAGE, ScenarioSupport.maxDuration(floor, scaled));
    }

    /**
     * How long a cell waits for the recovery before recording it as unrecovered, and what the id
     * space is sized against ({@link #population}), so the two cannot drift apart.
     */
    private static Duration boundFor(Duration outage) {
        return ScenarioSupport.maxDuration(Duration.ofMinutes(1), outage.multipliedBy(20));
    }

    private static Duration settleFor(BenchmarkConfig cfg) {
        return ScenarioSupport.minDuration(NOMINAL_SETTLE, cfg.duration().dividedBy(2));
    }

    private static int concurrentAggregates(BenchmarkConfig cfg) {
        return Math.max(1, Math.min(MAX_CONCURRENT_AGGREGATES, cfg.aggregateCardinality() * 4));
    }

    private static String summary(List<Cell> cells, ScenarioSupport.CorrectnessReport report) {
        StringBuilder text = new StringBuilder();
        for (Cell cell : cells) {
            if (text.length() > 0) {
                text.append("; ");
            }
            if (cell.recovered()) {
                text.append(String.format(Locale.ROOT, "%ds outage -> %d rows recovered in %.1fs (%.0f rows/s)",
                        cell.outage().toSeconds(), cell.backlog(),
                        cell.recovery().toMillis() / 1000.0, cell.drainRatePerSecond()));
            } else if (cell.diverging()) {
                text.append(String.format(Locale.ROOT,
                        "%ds outage -> %d rows DIVERGING (%d pending and still growing after %.0fs)",
                        cell.outage().toSeconds(), cell.backlog(), cell.remaining(),
                        cell.recovery().toMillis() / 1000.0));
            } else {
                text.append(String.format(Locale.ROOT,
                        "%ds outage -> %d rows DID NOT recover within %ds (%d still pending)",
                        cell.outage().toSeconds(), cell.backlog(), cell.bound().toSeconds(), cell.remaining()));
            }
        }
        text.append(String.format(Locale.ROOT, "; ordering violations=%d, missing=%d",
                report.orderingViolations(), report.missingKeys().size()));
        return text.toString();
    }

    private static Map<String, Object> metrics(List<Cell> cells, double rate, int concurrentAggregates,
            ScenarioSupport.CorrectnessReport report) {
        Map<String, Object> metrics = new LinkedHashMap<>();
        metrics.put("offeredRatePerSecond", rate);
        metrics.put("eventsPerAggregate", EVENTS_PER_AGGREGATE);
        metrics.put("quotaJitter", QUOTA_JITTER);
        metrics.put("concurrentAggregates", concurrentAggregates);
        for (int i = 0; i < cells.size(); i++) {
            Cell cell = cells.get(i);
            String prefix = "cell" + (i + 1);
            metrics.put(prefix + "OutageSeconds", cell.outage().toSeconds());
            metrics.put(prefix + "Backlog", cell.backlog());
            metrics.put(prefix + "Recovered", cell.recovered());
            metrics.put(prefix + "Diverging", cell.diverging());
            metrics.put(prefix + "RecoverySeconds", cell.recovery().toMillis() / 1000.0);
            metrics.put(prefix + "DrainRatePerSecond", cell.drainRatePerSecond());
            metrics.put(prefix + "RemainingAtEnd", cell.remaining());
        }
        metrics.put("orderingViolations", report.orderingViolations());
        metrics.put("missingCount", report.missingKeys().size());
        return metrics;
    }

    private record Cell(Duration outage, long backlog, Duration recovery, boolean recovered,
                        boolean diverging, long remaining, Duration bound) {

        double drainRatePerSecond() {
            double seconds = recovery.toMillis() / 1000.0;
            return seconds <= 0 ? 0 : backlog / seconds;
        }
    }
}
