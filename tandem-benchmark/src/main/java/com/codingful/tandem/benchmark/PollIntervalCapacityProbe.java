package com.codingful.tandem.benchmark;

import com.codingful.tandem.jdbc.RelayConfig;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * What a short poll interval <b>costs</b> the work the relay is actually doing (LLD-benchmark §6.7).
 *
 * <p>{@code IdlePollCostProbe} (§6.6) priced the claims a drained relay makes against a database doing
 * nothing else, and found them cheap on an empty or drained outbox and roughly three times as expensive
 * on a <b>blocked</b> one — rows in {@code status = 0} that no claim can take, which every poll scans
 * and runs the head-of-chain {@code NOT EXISTS} against. Absolute cost is not contention, though: it
 * says nothing about whether those queries take capacity away from real traffic. This probe asks that
 * directly, by measuring the <b>sustainable throughput ceiling</b> at two poll intervals in the two
 * outbox states, and comparing.
 *
 * <p>A ceiling comparison is only as good as its reproducibility, which is why the cells are
 * <b>interleaved and replicated</b>: replicate 1 runs the cells in order, replicate 2 runs them in
 * reverse, so a host that drifts in one direction over the session (a laptop warming up, a burstable
 * instance spending its credits) contributes to both arms of every comparison instead of to one. Read
 * the spread between replicates of the same cell before reading the difference between cells: when the
 * former is as large as the latter, this host has answered "cannot tell", and that is the honest
 * result.
 *
 * <p>Measures; gates nothing.
 */
public final class PollIntervalCapacityProbe {

    /** One measurement: an outbox state and the relay's idle poll interval. */
    private record Cell(OutboxStateSeeder.State state, int pollMillis) {

        String label() {
            return state + " @ " + pollMillis + " ms";
        }
    }

    private record Ceiling(double ratePerSecond, boolean bracketed, double relativeUncertainty) {
    }

    public static void main(String[] args) throws Exception {
        Map<String, String> kv = parseArgs(args);
        Duration sustain = Duration.ofSeconds(Long.parseLong(kv.getOrDefault("--sustain", "45")));
        Duration budget = Duration.ofSeconds(Long.parseLong(kv.getOrDefault("--budget", "360")));
        double seedRate = Double.parseDouble(kv.getOrDefault("--seed-rate", "800"));
        int replicates = Integer.parseInt(kv.getOrDefault("--replicates", "2"));
        int doneRows = Integer.parseInt(kv.getOrDefault("--done-rows", "200000"));
        int blockedRows = Integer.parseInt(kv.getOrDefault("--blocked-rows", "2000"));

        List<Cell> cells = List.of(
                new Cell(OutboxStateSeeder.State.DRAINED, 100),
                new Cell(OutboxStateSeeder.State.DRAINED, 10),
                new Cell(OutboxStateSeeder.State.BLOCKED, 100),
                new Cell(OutboxStateSeeder.State.BLOCKED, 10));

        BenchmarkConfig config = BenchmarkConfig.defaults();
        Map<Cell, List<Ceiling>> results = new LinkedHashMap<>();
        cells.forEach(c -> results.put(c, new ArrayList<>()));

        try (BenchmarkEnvironment env = new BenchmarkEnvironment(config).start()) {
            System.out.printf(Locale.ROOT, "Poll interval capacity probe — workers=%d, sustain=%s, budget=%s, "
                            + "seedRate=%.0f/s, replicates=%d, doneRows=%d, blockedRows=%d%n",
                    config.workers(), sustain, budget, seedRate, replicates, doneRows, blockedRows);

            for (int replicate = 1; replicate <= replicates; replicate++) {
                // Reversed on every other pass: a host that drifts one way over the session then feeds
                // both arms of each comparison rather than only the arm that ran while it was fast.
                List<Cell> order = new ArrayList<>(cells);
                if (replicate % 2 == 0) {
                    order = order.reversed();
                }
                for (Cell cell : order) {
                    OutboxStateSeeder.seed(env, cell.state(), config.bucketCount(), doneRows, blockedRows);
                    System.out.printf(Locale.ROOT, "%n=== replicate %d · %s (%s) ===%n",
                            replicate, cell.label(), OutboxStateSeeder.describe(env));
                    Ceiling ceiling = measure(env, config, cell, sustain, budget, seedRate);
                    results.get(cell).add(ceiling);
                    System.out.printf(Locale.ROOT, "  -> %.1f events/s%s%n", ceiling.ratePerSecond(),
                            ceiling.bracketed() ? "" : "  (NOT bracketed — budget ran out, lower bound only)");
                }
            }
        }
        report(results);
    }

    /**
     * One ceiling search against a relay sized only by {@code cell}'s poll interval. No correlation
     * consumer runs: this measures capacity, and the harness's own Kafka consumer would take some of
     * the host it is trying to measure. Correctness is S1's job and is asserted there.
     */
    private static Ceiling measure(BenchmarkEnvironment env, BenchmarkConfig config, Cell cell,
            Duration sustain, Duration budget, double seedRate) throws Exception {
        RelayConfig relayCfg = env.relayConfigBuilder()
                .pollInterval(Duration.ofMillis(cell.pollMillis()))
                .build();
        RelayInstance instance = env.newRelayInstance(relayCfg);
        instance.pool().start();
        try (LoadGenerator generator = new LoadGenerator(env.dataSource(), config.bucketCount(),
                config.maxConnections(), AggregateSelector.uniform("P36", config.aggregateCardinality()),
                config.payloadBytes(), null, config.wakeup())) {
            generator.stopTrackingInsertedKeys();   // nothing reconciles here; keep the harness's memory flat
            RampController ramp = new RampController(env.lagProbe(), Duration.ofSeconds(5), sustain, 0.05,
                    config.batchSize());
            // The blocked state leaves a constant floor of unclaimable rows in the backlog reading. The
            // ramp judges growth against a baseline it takes at the start of each hold, so a constant
            // offset cancels — but a probe that compared absolute pending against zero would read every
            // blocked cell as saturated from the first second.
            RampController.RampResult result = ramp.findSustainableMax(generator, seedRate, budget);
            generator.stop();
            // The pacer has stopped offering, but inserts are still in flight; closing the generator
            // would interrupt them and print socket stacks into the middle of the log. They have
            // nothing to do with the measurement, which is already over — let them land.
            Thread.sleep(2_000);
            return new Ceiling(result.sustainedRatePerSecond(), result.bracketed(), result.relativeUncertainty());
        } finally {
            instance.pool().stop();
        }
    }

    private static void report(Map<Cell, List<Ceiling>> results) {
        System.out.println();
        System.out.println("=== Sustainable ceiling by poll interval ===");
        System.out.printf(Locale.ROOT, "%-22s %16s %12s %9s %s%n",
                "cell", "replicates", "mean", "spread", "bracketed");
        for (Map.Entry<Cell, List<Ceiling>> e : results.entrySet()) {
            List<Ceiling> runs = e.getValue();
            String each = runs.stream().map(c -> String.format(Locale.ROOT, "%.0f", c.ratePerSecond()))
                    .reduce((a, b) -> a + " / " + b).orElse("-");
            System.out.printf(Locale.ROOT, "%-22s %16s %12.1f %8.1f%% %s%n",
                    e.getKey().label(), each, mean(runs), spreadPercent(runs),
                    allBracketed(runs) ? "yes" : "NO — lower bounds only");
        }
        System.out.println();
        for (OutboxStateSeeder.State state : List.of(OutboxStateSeeder.State.DRAINED, OutboxStateSeeder.State.BLOCKED)) {
            Double slow = meanFor(results, state, 100);
            Double fast = meanFor(results, state, 10);
            if (slow == null || fast == null || slow == 0) {
                continue;
            }
            if (!bracketedFor(results, state, 100) || !bracketedFor(results, state, 10)) {
                System.out.printf(Locale.ROOT,
                        "%s: NO COMPARISON — at least one cell never found a rate this host could not hold, "
                                + "so its figure is a lower bound set by the search budget rather than a "
                                + "ceiling. Re-run with a longer --budget.%n", state);
                continue;
            }
            double deltaPercent = (fast - slow) / slow * 100;
            double worstSpread = Math.max(spreadFor(results, state, 100), spreadFor(results, state, 10));
            System.out.printf(Locale.ROOT,
                    "%s: 10 ms sustains %.1f%% %s than 100 ms (worst replicate spread %.1f%%) — %s%n",
                    state, Math.abs(deltaPercent), deltaPercent < 0 ? "less" : "more", worstSpread,
                    Math.abs(deltaPercent) > worstSpread
                            ? "difference exceeds the host's own spread"
                            : "WITHIN the host's own spread: this host cannot tell the two apart");
        }
    }

    private static Double meanFor(Map<Cell, List<Ceiling>> results, OutboxStateSeeder.State state, int pollMillis) {
        List<Ceiling> runs = results.get(new Cell(state, pollMillis));
        return runs == null || runs.isEmpty() ? null : mean(runs);
    }

    private static double spreadFor(Map<Cell, List<Ceiling>> results, OutboxStateSeeder.State state, int pollMillis) {
        List<Ceiling> runs = results.get(new Cell(state, pollMillis));
        return runs == null ? 0 : spreadPercent(runs);
    }

    /** Whether every replicate of a cell actually bracketed its ceiling (see {@code RampController}). */
    private static boolean allBracketed(List<Ceiling> runs) {
        return runs.stream().allMatch(Ceiling::bracketed);
    }

    private static boolean bracketedFor(Map<Cell, List<Ceiling>> results, OutboxStateSeeder.State state, int pollMillis) {
        List<Ceiling> runs = results.get(new Cell(state, pollMillis));
        return runs != null && !runs.isEmpty() && allBracketed(runs);
    }

    private static double mean(List<Ceiling> runs) {
        return runs.stream().mapToDouble(Ceiling::ratePerSecond).average().orElse(0);
    }

    /** Max-to-min spread as a percentage of the mean — the noise any cell-to-cell difference must clear. */
    private static double spreadPercent(List<Ceiling> runs) {
        if (runs.size() < 2) {
            return 0;
        }
        double max = runs.stream().mapToDouble(Ceiling::ratePerSecond).max().orElse(0);
        double min = runs.stream().mapToDouble(Ceiling::ratePerSecond).min().orElse(0);
        double mean = mean(runs);
        return mean == 0 ? 0 : (max - min) / mean * 100;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> kv = new LinkedHashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (arg.startsWith("--") && eq > 0) {
                kv.put(arg.substring(0, eq), arg.substring(eq + 1));
            } else {
                throw new IllegalArgumentException("Unknown argument: " + arg);
            }
        }
        return kv;
    }

    private PollIntervalCapacityProbe() {
    }
}
