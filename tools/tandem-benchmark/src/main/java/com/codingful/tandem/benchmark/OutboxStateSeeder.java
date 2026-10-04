package com.codingful.tandem.benchmark;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

/**
 * Fills {@code tandem_outbox} with a named starting state, so a probe measures against a table that
 * looks like something real rather than an empty one (LLD-benchmark §6.6).
 *
 * <p>"A claim that finds nothing" is not one thing, and the difference is the partial index
 * {@code idx_tandem_outbox_dispatch} ({@code WHERE status = 0}). On an {@link State#EMPTY} table the
 * index is empty too, so a poll touches no entries — a floor, not a typical cost. A
 * {@link State#DRAINED} outbox, the normal state of a busy relay, holds {@code DONE} rows the partial
 * index excludes by construction. {@link State#BLOCKED} is the state that costs: rows in
 * {@code status = 0} that no claim can take, either backing off or queued behind a {@code FAILED}
 * head. Those are in the index, so every poll scans them and runs the head-of-chain
 * {@code NOT EXISTS} against each — real work, every cycle, returning nothing. One poisoned aggregate
 * (S6, an ordinary occurrence) puts an outbox there indefinitely.
 */
public final class OutboxStateSeeder {

    private OutboxStateSeeder() {
    }

    /** How much of the outbox is filled before a measurement (see the class doc). */
    public enum State {

        /** Nothing at all: the partial index is empty, so the claim is as cheap as it can be. */
        EMPTY,
        /** Many {@code DONE} rows — bulk the partial index excludes, which is the point of it being partial. */
        DRAINED,
        /** {@code DRAINED}, plus rows in {@code status = 0} that no claim can take: the realistic cost case. */
        BLOCKED
    }

    /**
     * Truncates and refills the outbox for {@code state}, then vacuums and settles.
     *
     * <p>The {@code VACUUM ANALYZE} is not optional: without fresh statistics the planner can pick a
     * different plan for the claim than a real database would, and the measurement would be of that
     * wrong plan. Running it here in the foreground also keeps it out of the measurement windows —
     * left to autovacuum it lands inside them.
     *
     * @param env         the environment whose outbox is refilled
     * @param state       the starting state to build
     * @param bucketCount the bucket count every seeded row is spread over
     * @param doneRows    how many {@code DONE} rows the non-empty states hold
     * @param blockedRows how many unclaimable rows {@link State#BLOCKED} adds
     * @throws SQLException         if the seeding statements fail
     * @throws InterruptedException if the post-vacuum settle is interrupted
     */
    public static void seed(BenchmarkEnvironment env, State state, int bucketCount, int doneRows, int blockedRows)
            throws SQLException, InterruptedException {
        env.resetBetweenScenarios();
        if (state == State.EMPTY) {
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
            if (state == State.BLOCKED) {
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

    private static void analyze(BenchmarkEnvironment env) throws SQLException, InterruptedException {
        try (Connection conn = env.dataSource().getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("VACUUM ANALYZE tandem_outbox");
        }
        Thread.sleep(10_000);
    }

    /**
     * Row counts by status, so a state is described by what its table holds rather than by its label.
     *
     * @param env the environment to inspect
     * @return a {@code pending:n, done:n, failed:n} summary
     * @throws SQLException if the query fails
     */
    public static String describe(BenchmarkEnvironment env) throws SQLException {
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
}
