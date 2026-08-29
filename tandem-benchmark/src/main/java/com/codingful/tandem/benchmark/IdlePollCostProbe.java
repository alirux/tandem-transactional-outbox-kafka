package com.codingful.tandem.benchmark;

import com.codingful.tandem.jdbc.RelayConfig;
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Prices the <b>other half</b> of the poll-interval trade-off: what an idle relay costs the database
 * when there is nothing to find (relay-sizing.md §1, dispatch-latency.md Q-D). Every existing
 * scenario drives load, so all of them measure what a shorter poll interval <i>buys</i>; none
 * measures what it <i>costs</i>, and the guidance to keep the idle query load in mind rested on
 * arithmetic rather than a measurement.
 *
 * <p>Every cell is measured against <b>three outbox states</b>, because "a claim that finds nothing"
 * is not one thing. On an <b>empty</b> table the partial index {@code idx_tandem_outbox_dispatch}
 * ({@code WHERE status = 0}) is empty too, so the scan touches no entries and the head-of-chain
 * {@code NOT EXISTS} never runs for want of a candidate row: that is the cheapest claim that can
 * exist, a floor rather than a typical cost. A <b>drained</b> outbox — the normal state of a busy
 * relay — holds many {@code DONE} rows, which the partial index excludes by construction. The state
 * that actually costs something is <b>blocked</b>: rows sitting in {@code status = 0} that cannot be
 * claimed, either backing off ({@code next_attempt_at} in the future) or stuck behind a {@code FAILED}
 * head. Those <i>are</i> in the partial index, so every poll scans them and runs the {@code NOT
 * EXISTS} against each — real work, on every cycle, returning nothing. A single poisoned aggregate
 * (scenario S6, an ordinary occurrence) puts an outbox in that state indefinitely.
 *
 * <p>Each cell reports:
 *
 * <ul>
 *   <li><b>expected</b> queries/s, {@code workers / pollInterval} — the identity under test;</li>
 *   <li><b>measured</b> queries/s, from the {@code xact_commit} delta on the benchmark database.
 *       Each claim borrows a pooled connection in autocommit and runs one statement, so one claim is
 *       one committed transaction, and this counts them without a Postgres extension;</li>
 *   <li><b>PostgreSQL CPU</b>, sampled from {@code docker stats}. The container is located by
 *       <b>image</b>, never by name: Testcontainers leaves its containers with generated names
 *       ({@code adoring_rosalind}), which is what made the archived {@code containers.csv} samples
 *       hard to attribute (docs/benchmark-results/README.md).</li>
 * </ul>
 *
 * <p>The first cell runs with <b>no relay at all</b>, so every later figure can be read as a delta
 * over an environment that is otherwise identical. Measures; gates nothing.
 */
public final class IdlePollCostProbe {

    /** How much of the outbox is filled before a cell is measured (see the class doc). */
    private enum OutboxState {

        /** Nothing at all: the partial index is empty, so the claim is as cheap as it can be. */
        EMPTY,
        /** Many {@code DONE} rows — bulk the partial index excludes, which is the point of it being partial. */
        DRAINED,
        /** {@code DRAINED}, plus rows in {@code status = 0} that no claim can take: the realistic cost case. */
        BLOCKED
    }

    /** One cell: a relay sizing, or the no-relay baseline when {@code workers == 0}. */
    private record Cell(int workers, int pollMillis) {

        String label() {
            return workers == 0 ? "no relay (baseline)" : workers + "w @ " + pollMillis + " ms";
        }

        double expectedQueriesPerSecond() {
            return workers == 0 ? 0.0 : workers * 1000.0 / pollMillis;
        }
    }

    private record Sample(double queriesPerSecond, double postgresCpuPercent) {
    }

    private static final List<Cell> DEFAULT_CELLS = List.of(
            new Cell(0, 0),
            new Cell(8, 100),
            new Cell(8, 20),
            new Cell(8, 10),
            new Cell(2, 25));

    public static void main(String[] args) throws Exception {
        Map<String, String> kv = parseArgs(args);
        Duration window = Duration.ofSeconds(Long.parseLong(kv.getOrDefault("--seconds", "60")));
        List<Cell> cells = kv.containsKey("--cells") ? parseCells(kv.get("--cells")) : DEFAULT_CELLS;
        int doneRows = Integer.parseInt(kv.getOrDefault("--done-rows", "200000"));
        int blockedRows = Integer.parseInt(kv.getOrDefault("--blocked-rows", "2000"));

        BenchmarkConfig config = BenchmarkConfig.defaults();
        try (BenchmarkEnvironment env = new BenchmarkEnvironment(config).start()) {
            String postgresContainer = postgresContainerId();
            System.out.println("Idle poll cost probe — window=" + window + ", postgres container="
                    + postgresContainer + ", doneRows=" + doneRows + ", blockedRows=" + blockedRows);

            for (OutboxState state : OutboxState.values()) {
                // The environment builds a primary pool but never starts it — scenarios do. This probe
                // leaves it stopped and drives its own per-cell instances instead, so the only relay
                // polling during a window is the one whose sizing is being priced.
                seed(env, state, config.bucketCount(), doneRows, blockedRows);
                System.out.println();
                System.out.println("=== outbox state: " + state + " (" + describe(env) + ") ===");
                Map<Cell, Sample> results = new LinkedHashMap<>();
                for (Cell cell : cells) {
                    results.put(cell, measure(env, cell, window, postgresContainer));
                }
                report(results);
            }
        }
    }

    /**
     * Truncates and refills the outbox for {@code state}. {@code ANALYZE} at the end is not optional:
     * without fresh statistics the planner can pick a different plan for the claim than a real
     * database would, and the whole measurement would be of that wrong plan.
     */
    private static void seed(BenchmarkEnvironment env, OutboxState state, int bucketCount,
            int doneRows, int blockedRows) throws Exception {
        env.resetBetweenScenarios();
        if (state == OutboxState.EMPTY) {
            analyze(env);
            return;
        }
        int poisonAggregates = Math.max(1, blockedRows / 20);
        try (Connection conn = env.dataSource().getConnection();
             Statement stmt = conn.createStatement()) {
            // DONE bulk: excluded from the partial index, present in the table and the aggregate index.
            stmt.execute("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, status, seq_source) "
                    + "SELECT 'probe-done-' || (i % 5000), 'Probe', 'probe.done', (i % " + bucketCount + "), "
                    + "i, '{\"p\":1}'::jsonb, 2, 0 FROM generate_series(1, " + doneRows + ") AS i");
            if (state == OutboxState.BLOCKED) {
                // Half backing off: excluded by the next_attempt_at predicate, but still index entries
                // the bucket scan walks on every poll.
                stmt.execute("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, "
                        + "status, seq_source, next_attempt_at) SELECT 'probe-backoff-' || i, 'Probe', 'probe.retry', "
                        + "(i % " + bucketCount + "), 1000000 + i, '{\"p\":1}'::jsonb, 0, 0, now() + interval '1 hour' "
                        + "FROM generate_series(1, " + (blockedRows / 2) + ") AS i");
                // Half behind a FAILED head, which is what makes the claim run its NOT EXISTS per row.
                stmt.execute("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, status, seq_source) "
                        + "SELECT 'probe-poison-' || i, 'Probe', 'probe.poison', (i % " + bucketCount + "), "
                        + "2000000 + i, '{\"p\":1}'::jsonb, 3, 0 FROM generate_series(1, " + poisonAggregates + ") AS i");
                stmt.execute("INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, payload, status, seq_source) "
                        + "SELECT 'probe-poison-' || j, 'Probe', 'probe.blocked', (j % " + bucketCount + "), "
                        + "3000000 + i, '{\"p\":1}'::jsonb, 0, 0 FROM generate_series(1, " + (blockedRows / 2) + ") AS i, "
                        + "LATERAL (SELECT ((i - 1) % " + poisonAggregates + ") + 1 AS j) x");
            }
        }
        analyze(env);
    }

    /**
     * {@code VACUUM ANALYZE} plus a settle, run in the foreground on purpose. Fresh statistics keep the
     * planner honest, and doing the vacuum here keeps it out of the measurement windows. Left to
     * autovacuum it lands inside them — including the no-relay baseline, where it is then subtracted
     * from every cell of that state as though it were background noise.
     */
    private static void analyze(BenchmarkEnvironment env) throws Exception {
        try (Connection conn = env.dataSource().getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("VACUUM ANALYZE tandem_outbox");
        }
        Thread.sleep(10_000);
    }

    /** Row counts by status, so each state's table is described by what it holds, not by its label. */
    private static String describe(BenchmarkEnvironment env) throws Exception {
        try (Connection conn = env.dataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT count(*) FILTER (WHERE status = 0) AS pending,"
                             + " count(*) FILTER (WHERE status = 2) AS done,"
                             + " count(*) FILTER (WHERE status = 3) AS failed FROM tandem_outbox")) {
            return rs.next()
                    ? "pending:" + rs.getLong(1) + ", done:" + rs.getLong(2) + ", failed:" + rs.getLong(3)
                    : "unknown";
        }
    }

    private static Sample measure(BenchmarkEnvironment env, Cell cell, Duration window, String container)
            throws Exception {
        RelayInstance instance = null;
        if (cell.workers() > 0) {
            RelayConfig relayCfg = env.relayConfigBuilder()
                    .workersPerInstance(cell.workers())
                    .pollInterval(Duration.ofMillis(cell.pollMillis()))
                    .build();
            instance = env.newRelayInstance(relayCfg);
            instance.pool().start();
        }
        try {
            Thread.sleep(2_000);   // let the pool reach steady state before the window opens
            long commitsBefore = committedTransactions(env);
            long startNanos = System.nanoTime();
            double cpu = sampleCpuOver(window, container);
            double elapsedSeconds = (System.nanoTime() - startNanos) / 1e9;
            long commitsAfter = committedTransactions(env);
            return new Sample((commitsAfter - commitsBefore) / elapsedSeconds, cpu);
        } finally {
            if (instance != null) {
                instance.pool().stop();
            }
        }
    }

    /**
     * Committed transactions on the database the benchmark connects to. Read per-database rather than
     * cluster-wide so the sampler's own bookkeeping and any other database cannot inflate the count.
     */
    private static long committedTransactions(BenchmarkEnvironment env) throws Exception {
        try (Connection conn = env.dataSource().getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT xact_commit FROM pg_stat_database WHERE datname = current_database()")) {
            return rs.next() ? rs.getLong(1) : 0L;
        }
    }

    /** Mean CPU% of the container over the window, from repeated {@code docker stats --no-stream}. */
    private static double sampleCpuOver(Duration window, String container) throws Exception {
        List<Double> samples = new ArrayList<>();
        long deadline = System.nanoTime() + window.toNanos();
        while (System.nanoTime() < deadline) {
            String out = run("docker", "stats", "--no-stream", "--format", "{{.CPUPerc}}", container);
            String value = out.trim().replace("%", "");
            if (!value.isEmpty()) {
                samples.add(Double.parseDouble(value));
            }
        }
        return samples.stream().mapToDouble(Double::doubleValue).average().orElse(Double.NaN);
    }

    /**
     * The PostgreSQL container, found by image. Testcontainers does not name its containers, so the
     * name column carries a generated word pair and cannot tell PostgreSQL from Kafka.
     */
    private static String postgresContainerId() throws Exception {
        for (String line : run("docker", "ps", "--format", "{{.ID}} {{.Image}}").split("\n")) {
            String[] parts = line.trim().split("\\s+", 2);
            if (parts.length == 2 && parts[1].toLowerCase(Locale.ROOT).contains("postgres")) {
                return parts[0];
            }
        }
        throw new IllegalStateException("No running container whose image names postgres");
    }

    private static String run(String... command) throws Exception {
        Process process = new ProcessBuilder(command).redirectErrorStream(true).start();
        StringBuilder out = new StringBuilder();
        try (BufferedReader reader = new BufferedReader(
                new InputStreamReader(process.getInputStream(), StandardCharsets.UTF_8))) {
            for (String line = reader.readLine(); line != null; line = reader.readLine()) {
                out.append(line).append('\n');
            }
        }
        process.waitFor();
        return out.toString();
    }

    private static void report(Map<Cell, Sample> results) {
        Sample baseline = results.entrySet().stream()
                .filter(e -> e.getKey().workers() == 0).map(Map.Entry::getValue).findFirst().orElse(null);
        System.out.printf(Locale.ROOT, "%-22s %14s %14s %14s %14s%n",
                "cell", "expected q/s", "measured q/s", "postgres CPU%", "CPU% over base");
        for (Map.Entry<Cell, Sample> entry : results.entrySet()) {
            Sample sample = entry.getValue();
            String delta = baseline == null ? "-"
                    : String.format(Locale.ROOT, "%+.2f", sample.postgresCpuPercent() - baseline.postgresCpuPercent());
            System.out.printf(Locale.ROOT, "%-22s %14.1f %14.1f %14.2f %14s%n",
                    entry.getKey().label(), entry.getKey().expectedQueriesPerSecond(),
                    sample.queriesPerSecond(), sample.postgresCpuPercent(), delta);
        }
    }

    private static List<Cell> parseCells(String spec) {
        List<Cell> cells = new ArrayList<>();
        cells.add(new Cell(0, 0));
        for (String cell : spec.split(",")) {
            String[] parts = cell.trim().split("x");
            cells.add(new Cell(Integer.parseInt(parts[0]), Integer.parseInt(parts[1])));
        }
        return cells;
    }

    private static Map<String, String> parseArgs(String[] args) {
        Map<String, String> kv = new LinkedHashMap<>();
        for (String arg : args) {
            int eq = arg.indexOf('=');
            if (eq > 0) {
                kv.put(arg.substring(0, eq), arg.substring(eq + 1));
            }
        }
        return kv;
    }

    private IdlePollCostProbe() {
    }
}
