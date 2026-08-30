package com.codingful.tandem.benchmark;

import com.codingful.tandem.benchmark.scenario.S1SustainedThroughput;
import com.codingful.tandem.benchmark.scenario.S2LatencyAtNormalLoad;
import com.codingful.tandem.benchmark.scenario.S3HotPartition;
import com.codingful.tandem.benchmark.scenario.S4Saturation;
import com.codingful.tandem.benchmark.scenario.S5WorkerFailover;
import com.codingful.tandem.benchmark.scenario.S6PoisonMessage;
import com.codingful.tandem.benchmark.scenario.S8MultiInstanceLease;
import com.codingful.tandem.benchmark.scenario.S9Endurance;
import com.codingful.tandem.benchmark.scenario.Scenario;
import com.codingful.tandem.benchmark.scenario.ScenarioContext;
import com.codingful.tandem.benchmark.scenario.ScenarioResult;
import java.time.Duration;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * The {@code ./gradlew :tandem-benchmark:loadTest} entrypoint (LLD-benchmark §9): runs the selected
 * scenarios against one {@link BenchmarkEnvironment}, then prints each result. S7 (causal-ordering
 * overhead) is deferred to the 2nd round (HLD-load-testing.md §4) and is not registered here. S8
 * (multi-instance {@code LEASE} coordination) builds its own additional relay instances on top of the
 * shared environment (LLD-benchmark §8) rather than using the environment's primary {@code SINGLE} pool.
 *
 * <p>Usage: {@code LoadTestRunner [--smoke|--demo] [--duration=<seconds>] [--workers=<n>]
 * [--poll-interval=<millis>] [--poll-floor=<millis>] [--rate=<events/s>] [--window=<seconds>] [--connections=<n>]
 * [--retention=<seconds>] [--cleanup-interval=<seconds>] [--cleanup-batch=<n>] [S1,S2,...]}:
 * <ul>
 *   <li>{@code --smoke} — tiny rate/duration, correctness only, no KPI numbers (HLD-load-testing.md §5.1).</li>
 *   <li>{@code --demo} — real relay concurrency (default workers/batchSize/bucketCount) but a short
 *       {@code duration}, so the harness runs against real Docker containers in under a couple of
 *       minutes instead of the multi-hour full-run default. Still not a KPI number on a developer
 *       machine (HLD-load-testing.md §5.1) — just faster to look at.</li>
 *   <li>{@code --duration=<seconds>} — overrides whichever base config's {@code duration} (applied
 *       after {@code --smoke}/{@code --demo}). Useful for a run longer than {@code --demo}'s 20s but
 *       shorter than the 10-minute full-run default — note S3 caps its own active drive phase
 *       independent of this (LLD-benchmark §8): its backlog is structurally serialized per aggregate,
 *       so it does not scale the same way as the other scenarios.</li>
 *   <li>{@code --workers=<n>} — relay {@code workersPerInstance}; {@code --poll-interval=<millis>} —
 *       the ceiling of the relay's idle backoff; {@code --poll-floor=<millis>} — where that backoff
 *       restarts after a claim that found work. They are the discovery-latency knobs
 *       (docs/dispatch-latency.md §1): a row of a live stream waits about half the floor, a row
 *       arriving after a quiet stretch up to the ceiling, and the idle query load a quiet relay costs
 *       is {@code workers / pollInterval}. Passing {@code --poll-floor=} equal to
 *       {@code --poll-interval=} measures the pre-adaptive fixed interval. All three are applied
 *       after {@code --smoke}/{@code --demo}, like {@code --duration=}, so an explicit value always
 *       wins over the preset's own.</li>
 *   <li>{@code --rate=<events/s>} and {@code --window=<seconds>} — the endurance scenario's fixed
 *       offered rate and reporting window (S9). Without {@code --rate=} it seeds one with a short
 *       ramp of its own; {@code --window=} is the unit its drift comparison is made between.</li>
 *   <li>{@code --retention=<seconds>}, {@code --cleanup-interval=<seconds>} and
 *       {@code --cleanup-batch=<n>} — the relay's DONE-row cleanup. The defaults (14 days, every 15
 *       minutes, 1000 rows) mean cleanup deletes nothing during any run shorter than a fortnight, so a
 *       run of hours grows the outbox without bound and never exercises the churn a production outbox
 *       actually lives in. Size the batch against the write rate: {@code rate × interval} rows at
 *       minimum.</li>
 *   <li>{@code --connections=<n>} — the pooled {@code DataSource}'s size, shared by the load
 *       generator, every relay instance and the lag probe. Worth raising for a run with more than one
 *       relay instance: the generator alone may hold as many connections as it has in-flight inserts,
 *       and a probe that cannot get one fails the run rather than waiting quietly.</li>
 *   <li>neither flag — the full-run default ({@code BenchmarkConfig.defaults()}, 10 min/scenario).</li>
 * </ul>
 * The scenario list defaults to all six. An unrecognised {@code --} argument is rejected rather than
 * ignored: a run that silently used the default sizing reads exactly like one that honoured the flag,
 * and the difference only surfaces once the numbers are already published.
 *
 * <p>Scenarios are isolated from one another in two respects, and both are needed. An exception thrown
 * by one (an assertion failure inside it, or a bounded wait — e.g. draining the backlog — timing out)
 * is caught, reported as a {@code FAIL} line, and counted into the run's overall correctness like any
 * other failure, rather than aborting the batch. And the environment is emptied before each scenario
 * ({@link BenchmarkEnvironment#resetBetweenScenarios()}), so one that ends with rows still in the
 * outbox cannot hand them to the next: without that, a scenario measured its own load plus whatever
 * the previous one left behind, which made results depend on the order they ran in.
 */
public final class LoadTestRunner {

    private static final Map<String, Scenario> ALL_SCENARIOS = registerScenarios();
    private static final Set<String> FLAGS = Set.of("--smoke", "--demo");
    private static final String DURATION_PREFIX = "--duration=";
    private static final String WORKERS_PREFIX = "--workers=";
    private static final String POLL_INTERVAL_PREFIX = "--poll-interval=";
    private static final String POLL_FLOOR_PREFIX = "--poll-floor=";
    private static final String RATE_PREFIX = "--rate=";
    private static final String WINDOW_PREFIX = "--window=";
    private static final String CONNECTIONS_PREFIX = "--connections=";
    private static final String RETENTION_PREFIX = "--retention=";
    private static final String CLEANUP_INTERVAL_PREFIX = "--cleanup-interval=";
    private static final String CLEANUP_BATCH_PREFIX = "--cleanup-batch=";
    private static final Set<String> VALUE_PREFIXES = Set.of(DURATION_PREFIX, WORKERS_PREFIX,
            POLL_INTERVAL_PREFIX, POLL_FLOOR_PREFIX, RATE_PREFIX, WINDOW_PREFIX, CONNECTIONS_PREFIX,
            RETENTION_PREFIX, CLEANUP_INTERVAL_PREFIX, CLEANUP_BATCH_PREFIX);

    public static void main(String[] args) throws Exception {
        List<String> argList = List.of(args);
        rejectUnknownFlags(argList);
        List<String> scenarioIds = scenarioIdsFrom(argList);
        BenchmarkConfig config = configFrom(argList);
        // The sizing goes in the run's own first line because these runs are archived and quoted from
        // (docs/benchmark-results/): a percentile is only attributable if the log says what produced it.
        System.out.println("Tandem load test — scenarios=" + scenarioIds
                + ", smoke=" + argList.contains("--smoke") + ", demo=" + argList.contains("--demo")
                + ", duration=" + config.duration() + ", workers=" + config.workers()
                + ", pollInterval=" + config.pollInterval()
                + ", pollFloor=" + config.pollIntervalFloor());

        try (BenchmarkEnvironment env = new BenchmarkEnvironment(config).start()) {
            ScenarioContext ctx = new ScenarioContext(env, config);
            boolean allPassed = true;
            for (String id : scenarioIds) {
                Scenario scenario = ALL_SCENARIOS.get(id);
                if (scenario == null) {
                    System.out.println("Unknown scenario: " + id + " (known: " + ALL_SCENARIOS.keySet() + ")");
                    continue;
                }
                System.out.println("--- Running " + id + " ---");
                env.resetBetweenScenarios();
                try {
                    ScenarioResult result = scenario.run(ctx);
                    System.out.println((result.passed() ? "PASS " : "FAIL ") + result.scenarioId() + ": " + result.summary());
                    allPassed &= result.passed();
                } catch (Exception e) {
                    System.out.println("FAIL " + id + ": threw " + e.getClass().getSimpleName() + ": " + e.getMessage());
                    allPassed = false;
                }
            }
            System.out.println(allPassed ? "All scenarios passed correctness." : "One or more scenarios FAILED correctness.");
            if (!allPassed) {
                System.exit(1);
            }
        }
    }

    /** The scenario list: the first argument that is not a flag, defaulting to every known scenario. */
    static List<String> scenarioIdsFrom(List<String> args) {
        return args.stream()
                .filter(a -> !a.startsWith("--"))
                .findFirst()
                .map(csv -> List.of(csv.split(",")))
                .orElse(List.copyOf(ALL_SCENARIOS.keySet()));
    }

    /**
     * The base preset chosen by {@code --smoke}/{@code --demo}, with each explicit knob layered on
     * top — so a flag given alongside a preset overrides that preset rather than being swallowed by it.
     */
    static BenchmarkConfig configFrom(List<String> args) {
        BenchmarkConfig base = args.contains("--smoke") ? BenchmarkConfig.defaults().toSmoke()
                : args.contains("--demo") ? BenchmarkConfig.defaults().toDemo()
                : BenchmarkConfig.defaults();
        BenchmarkConfig.Builder config = base.toBuilder();
        longValue(args, DURATION_PREFIX).ifPresent(seconds -> config.duration(Duration.ofSeconds(seconds)));
        longValue(args, WORKERS_PREFIX).ifPresent(workers -> config.workers(Math.toIntExact(workers)));
        longValue(args, POLL_INTERVAL_PREFIX).ifPresent(millis -> config.pollInterval(Duration.ofMillis(millis)));
        longValue(args, POLL_FLOOR_PREFIX).ifPresent(millis -> config.pollIntervalFloor(Duration.ofMillis(millis)));
        longValue(args, RATE_PREFIX).ifPresent(config::offeredRate);
        longValue(args, WINDOW_PREFIX).ifPresent(seconds -> config.window(Duration.ofSeconds(seconds)));
        longValue(args, CONNECTIONS_PREFIX).ifPresent(n -> config.maxConnections(Math.toIntExact(n)));
        longValue(args, RETENTION_PREFIX).ifPresent(seconds -> config.retention(Duration.ofSeconds(seconds)));
        longValue(args, CLEANUP_INTERVAL_PREFIX)
                .ifPresent(seconds -> config.cleanupInterval(Duration.ofSeconds(seconds)));
        longValue(args, CLEANUP_BATCH_PREFIX).ifPresent(n -> config.cleanupBatchSize(Math.toIntExact(n)));
        return config.build();
    }

    private static Optional<Long> longValue(List<String> args, String prefix) {
        return args.stream()
                .filter(a -> a.startsWith(prefix))
                .findFirst()
                .map(a -> parseLong(prefix, a.substring(prefix.length()).trim()));
    }

    private static long parseLong(String prefix, String value) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException notANumber) {
            // Rethrown naming the flag: the bare message is the offending text with no hint of which
            // argument carried it, and every one of these flags takes a bare number.
            throw new IllegalArgumentException(prefix + " expects a number, got: " + value, notANumber);
        }
    }

    private static void rejectUnknownFlags(List<String> args) {
        for (String arg : args) {
            if (arg.startsWith("--") && !FLAGS.contains(arg)
                    && VALUE_PREFIXES.stream().noneMatch(arg::startsWith)) {
                throw new IllegalArgumentException("Unknown argument: " + arg
                        + " (known: " + FLAGS + ", " + VALUE_PREFIXES + ")");
            }
        }
    }

    private static Map<String, Scenario> registerScenarios() {
        Map<String, Scenario> byId = new LinkedHashMap<>();
        for (Scenario s : List.<Scenario>of(
                new S1SustainedThroughput(),
                new S2LatencyAtNormalLoad(),
                new S3HotPartition(),
                new S4Saturation(),
                new S5WorkerFailover(),
                new S6PoisonMessage(),
                new S8MultiInstanceLease(),
                new S9Endurance())) {
            byId.put(s.id(), s);
        }
        return Collections.unmodifiableMap(byId);
    }

    /** Every scenario id this runner knows about, in HLD table order (S7 excluded — deferred). */
    public static Set<String> knownScenarioIds() {
        return ALL_SCENARIOS.keySet();
    }

    private LoadTestRunner() {
    }
}
