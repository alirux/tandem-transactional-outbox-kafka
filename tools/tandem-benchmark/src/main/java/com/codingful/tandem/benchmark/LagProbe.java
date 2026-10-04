package com.codingful.tandem.benchmark;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.sql.DataSource;

/**
 * Direct-SQL lag observation over {@code tandem_outbox} (HLD-load-testing.md §2.1; LLD-benchmark §6,
 * §6.1): the product exposes no per-shard lag metric and no caller ever populates
 * {@code TandemMetrics.recordLagAgeSeconds}, so the harness measures the pending backlog itself —
 * overall (feeds {@link RampController}) and per-bucket (S3).
 */
public final class LagProbe {

    // status: 0=PENDING, 1=IN_FLIGHT, 3=FAILED — "not yet DONE" (LLD-jdbc schema comment).
    private static final String OVERALL_SQL =
            "SELECT count(*) AS pending, "
                    + "coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_age_seconds "
                    + "FROM tandem_outbox WHERE status IN (0, 1, 3)";

    // PENDING/IN_FLIGHT only — excludes FAILED, which is terminal and will never drain on its own
    // (LLD-jdbc §3.4.2): a permanently failed row must not stall waitForDrain — it should surface
    // promptly as a missing key in the correctness check instead. Scoped to one scenario's own
    // aggregate-id namespace (LLD-benchmark §8, AggregateSelector):
    // when several scenarios share one tandem_outbox table (SmokeLoadTest), another scenario's
    // permanently-stuck poisoned backlog (S6) must not make THIS scenario's drain wait hang forever.
    private static final String IN_PROGRESS_FOR_NAMESPACE_SQL =
            "SELECT count(*) AS pending, "
                    + "coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_age_seconds "
                    + "FROM tandem_outbox WHERE status IN (0, 1) AND aggregate_id LIKE ?";

    private static final String PER_BUCKET_SQL =
            "SELECT bucket, count(*) AS pending, "
                    + "coalesce(extract(epoch FROM now() - min(created_at)), 0) AS oldest_age_seconds "
                    + "FROM tandem_outbox WHERE status IN (0, 1, 3) GROUP BY bucket ORDER BY bucket";

    private static final String PENDING_EXCLUDING_SQL =
            "SELECT count(*) FROM tandem_outbox WHERE status IN (0, 1) AND aggregate_id <> ? AND aggregate_id LIKE ?";

    // Storage growth over a long run (S9): the table's total on-disk size, how much of it is index,
    // the exact row count, and the dead tuples autovacuum has not yet reclaimed. The dead-tuple count
    // is the one that turns "throughput sagged after four hours" into a diagnosis rather than a
    // mystery: a churn table whose vacuum falls behind slows down through bloat, not through anything
    // the relay is doing.
    private static final String STORAGE_SQL =
            "SELECT pg_total_relation_size('tandem_outbox') AS total_bytes, "
                    + "pg_indexes_size('tandem_outbox') AS index_bytes, "
                    + "(SELECT count(*) FROM tandem_outbox) AS row_count, "
                    + "coalesce((SELECT n_dead_tup FROM pg_stat_user_tables WHERE relname = 'tandem_outbox'), 0) "
                    + "AS dead_tuples";

    private static final String HAS_FAILED_ROW_SQL =
            "SELECT EXISTS(SELECT 1 FROM tandem_outbox WHERE aggregate_id = ? AND status = 3)";

    private static final String FAILED_FOR_NAMESPACE_SQL =
            "SELECT count(*) FROM tandem_outbox WHERE aggregate_id LIKE ? AND status = 3";

    private final DataSource dataSource;

    public LagProbe(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public record Lag(long pending, Duration age) {
    }

    /** {@code tandem_outbox}'s on-disk footprint at a point in time (S9). */
    public record Storage(long totalBytes, long indexBytes, long rowCount, long deadTuples) {

        public long totalMegabytes() {
            return totalBytes / (1024 * 1024);
        }

        public long indexMegabytes() {
            return indexBytes / (1024 * 1024);
        }
    }

    /** Total/index size, row count and unreclaimed dead tuples for {@code tandem_outbox}. */
    public Storage storage() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(STORAGE_SQL);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return new Storage(rs.getLong("total_bytes"), rs.getLong("index_bytes"),
                    rs.getLong("row_count"), rs.getLong("dead_tuples"));
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (storage) failed", e);
        }
    }

    /** Global pending count + age of the oldest pending row, across all buckets. */
    public Lag overall() {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(OVERALL_SQL);
             ResultSet rs = ps.executeQuery()) {
            rs.next();
            return toLag(rs);
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (overall) failed", e);
        }
    }

    /**
     * Pending + in-flight only (excludes {@code FAILED}), scoped to one scenario's {@code namespace}
     * (its aggregate-id prefix) — what {@code waitForDrain} polls. Scoping matters because several
     * scenarios can share one {@code tandem_outbox} table (SmokeLoadTest): another scenario's
     * permanently-stuck poisoned backlog (S6) must not count as "not yet drained" here.
     */
    public Lag inProgressForNamespace(String namespace) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(IN_PROGRESS_FOR_NAMESPACE_SQL)) {
            ps.setString(1, namespace + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return toLag(rs);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (in-progress-for-namespace) failed", e);
        }
    }

    /** Per-bucket pending count + oldest-row age; only buckets with pending rows appear. */
    public Map<Integer, Lag> perBucket() {
        Map<Integer, Lag> result = new LinkedHashMap<>();
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(PER_BUCKET_SQL);
             ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                result.put(rs.getInt("bucket"), toLag(rs));
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (per-bucket) failed", e);
        }
        return result;
    }

    /**
     * Pending (PENDING/IN_FLIGHT) rows in {@code namespace} belonging to any aggregate other than
     * {@code aggregateId} (S6: "did everyone else in my own scenario drain?"). Scoped to
     * {@code namespace} for the same reason as {@link #inProgressForNamespace}: another scenario's
     * own stuck backlog must not count as "not yet drained" here.
     */
    public long pendingExcludingAggregate(String namespace, String aggregateId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(PENDING_EXCLUDING_SQL)) {
            ps.setString(1, aggregateId);
            ps.setString(2, namespace + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (pendingExcludingAggregate) failed", e);
        }
    }

    /** Whether {@code aggregateId} has at least one {@code FAILED} row (S6: confirms the poison gate tripped). */
    public boolean hasFailedRow(String aggregateId) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(HAS_FAILED_ROW_SQL)) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getBoolean(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (hasFailedRow) failed", e);
        }
    }

    /**
     * How many rows of {@code namespace} were quarantined to {@code FAILED} (S12). A broker outage
     * long enough to burn a row's whole retry ladder ends with that row terminal rather than
     * delivered, which reads as a lost event in the correctness report; counting them says which of
     * the two happened without having to go to the table by hand.
     */
    public long failedForNamespace(String namespace) {
        try (Connection conn = dataSource.getConnection();
             PreparedStatement ps = conn.prepareStatement(FAILED_FOR_NAMESPACE_SQL)) {
            ps.setString(1, namespace + "-%");
            try (ResultSet rs = ps.executeQuery()) {
                rs.next();
                return rs.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("lag probe (failedForNamespace) failed", e);
        }
    }

    private static Lag toLag(ResultSet rs) throws SQLException {
        long millis = (long) (rs.getDouble("oldest_age_seconds") * 1000);
        return new Lag(rs.getLong("pending"), Duration.ofMillis(millis));
    }
}
