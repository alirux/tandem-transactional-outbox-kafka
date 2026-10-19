package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.BucketStatusView;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.RelayStatusView;
import com.codingful.tandem.core.WorkerView;
import com.codingful.tandem.core.port.RelayQuery;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import javax.sql.DataSource;

/**
 * Admin-API read side over relay-control state (HLD-admin-api §4.1, {@link RelayQuery}): coordination
 * mode, whole-relay status, and per-bucket/per-worker observability — {@code tandem_meta},
 * {@code tandem_bucket_lease}, {@code tandem_relay_member}. Named columns only, never {@code SELECT *}
 * (AGENTS, HLD §1.4).
 */
public final class JdbcRelayQuery implements RelayQuery {

    private static final String META_SQL =
            "SELECT key, value FROM tandem_meta"
                    + " WHERE key IN ('bucket_count', 'relay_paused', 'coordination', 'relay_heartbeat_interval_seconds')";

    // Judged on the database's clock, the one the relay stamps its heartbeat with: comparing the
    // stamp with this process's clock would read any skew between the two hosts as heartbeat age.
    private static final String HEARTBEAT_FRESH_SQL =
            "SELECT updated_at > now() - (? * interval '1 second') AS fresh"
                    + "  FROM tandem_meta WHERE key = 'coordination'";

    // Missed heartbeats tolerated before RelayStatus.state reports DOWN - a couple of slow ticks
    // shouldn't flip it, only a relay that has genuinely stopped touching tandem_meta.
    private static final int DOWN_THRESHOLD_MISSED_HEARTBEATS = 3;

    // Assumed heartbeat cadence when relay_heartbeat_interval_seconds is absent - an old relay build
    // that predates this feature never publishes it, so this is the best guess available; matches
    // RelayConfig's own documented reclaimInterval default (LLD-jdbc §4).
    private static final long DEFAULT_HEARTBEAT_INTERVAL_SECONDS = 5;

    // Same predicate BucketLeaseManager's own uncoveredBuckets() uses: free/expired AND has pending work.
    private static final String UNCOVERED_COUNT_SQL =
            "SELECT count(*) FROM tandem_bucket_lease bl"
                    + " WHERE (bl.owner IS NULL OR bl.lease_until < now())"
                    + "   AND EXISTS (SELECT 1 FROM tandem_outbox o WHERE o.bucket = bl.bucket AND o.status = 0)";

    private static final String LIVE_WORKER_COUNT_SQL =
            "SELECT count(*) FROM tandem_relay_member WHERE lease_until > now()";

    private static final String BUCKET_COLUMNS =
            "bl.bucket, bl.owner, bl.lease_until, bl.paused,"
                    + " (bl.owner IS NOT NULL AND bl.lease_until > now()) AS covered,"
                    + " COALESCE(o.pending_count, 0) AS pending_count, o.oldest_pending_at";

    private static final String BUCKETS_SQL =
            "SELECT " + BUCKET_COLUMNS
                    + "  FROM tandem_bucket_lease bl"
                    + "  LEFT JOIN (SELECT bucket, count(*) AS pending_count, min(created_at) AS oldest_pending_at"
                    + "               FROM tandem_outbox WHERE status = 0 GROUP BY bucket) o ON o.bucket = bl.bucket"
                    + " ORDER BY bl.bucket";

    private static final String UNCOVERED_BUCKETS_SQL =
            "SELECT " + BUCKET_COLUMNS
                    + "  FROM tandem_bucket_lease bl"
                    + "  LEFT JOIN (SELECT bucket, count(*) AS pending_count, min(created_at) AS oldest_pending_at"
                    + "               FROM tandem_outbox WHERE status = 0 GROUP BY bucket) o ON o.bucket = bl.bucket"
                    + " WHERE (bl.owner IS NULL OR bl.lease_until < now()) AND COALESCE(o.pending_count, 0) > 0"
                    + " ORDER BY bl.bucket";

    private static final String BUCKET_BY_ID_SQL =
            "SELECT " + BUCKET_COLUMNS
                    + "  FROM tandem_bucket_lease bl"
                    + "  LEFT JOIN (SELECT bucket, count(*) AS pending_count, min(created_at) AS oldest_pending_at"
                    + "               FROM tandem_outbox WHERE status = 0 GROUP BY bucket) o ON o.bucket = bl.bucket"
                    + " WHERE bl.bucket = ?";

    private static final String WORKERS_SQL =
            "SELECT m.owner, m.updated_at,"
                    + "       (SELECT count(*) FROM tandem_bucket_lease bl"
                    + "         WHERE bl.owner = m.owner AND bl.lease_until > now()) AS bucket_count"
                    + "  FROM tandem_relay_member m"
                    + " WHERE m.lease_until > now()"
                    + " ORDER BY m.owner";

    private final DataSource dataSource;

    /** @param dataSource used for every query; each call opens and closes its own connection */
    public JdbcRelayQuery(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public RelayCoordinationMode coordinationMode() {
        return parseMode(readMeta().get("coordination"));
    }

    @Override
    public RelayStatusView status() {
        Map<String, String> meta = readMeta();
        boolean paused = "true".equals(meta.get("relay_paused"));
        int bucketCount = meta.containsKey("bucket_count") ? Integer.parseInt(meta.get("bucket_count")) : 0;
        RelayCoordinationMode mode = parseMode(meta.get("coordination"));
        long heartbeatIntervalSeconds = meta.containsKey("relay_heartbeat_interval_seconds")
                ? Long.parseLong(meta.get("relay_heartbeat_interval_seconds"))
                : DEFAULT_HEARTBEAT_INTERVAL_SECONDS;
        boolean alive = heartbeatFresh(DOWN_THRESHOLD_MISSED_HEARTBEATS * heartbeatIntervalSeconds);
        if (mode != RelayCoordinationMode.LEASE) {
            return new RelayStatusView(alive, paused, bucketCount, 0, 0);
        }
        return new RelayStatusView(alive, paused, bucketCount, count(UNCOVERED_COUNT_SQL), count(LIVE_WORKER_COUNT_SQL));
    }

    /** A value written by a newer relay is tolerated, never rejected (HLD §1.4). */
    private static RelayCoordinationMode parseMode(String value) {
        if (value == null) {
            return RelayCoordinationMode.SINGLE;
        }
        for (RelayCoordinationMode mode : RelayCoordinationMode.values()) {
            if (mode != RelayCoordinationMode.UNKNOWN && mode.name().equals(value)) {
                return mode;
            }
        }
        return RelayCoordinationMode.UNKNOWN;
    }

    /** Whether the last heartbeat is younger than {@code maxAgeSeconds}; {@code false} when there is none. */
    private boolean heartbeatFresh(long maxAgeSeconds) {
        return Jdbc.run(dataSource, "coordination heartbeat query failed", conn ->
                Jdbc.withStatement(conn, HEARTBEAT_FRESH_SQL, ps -> {
                    ps.setLong(1, maxAgeSeconds);
                    // A NULL updated_at compares to NULL, which getBoolean reads as false: no heartbeat.
                    return Jdbc.withResultSet(ps, rs -> rs.next() && rs.getBoolean("fresh"));
                }));
    }

    @Override
    public List<BucketStatusView> buckets(boolean uncoveredOnly) {
        String sql = uncoveredOnly ? UNCOVERED_BUCKETS_SQL : BUCKETS_SQL;
        return Jdbc.run(dataSource, "buckets query failed", conn ->
                Jdbc.withStatement(conn, sql, ps -> Jdbc.withResultSet(ps, rs -> {
                    List<BucketStatusView> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(mapBucket(rs));
                    }
                    return rows;
                })));
    }

    @Override
    public Optional<BucketStatusView> bucket(int bucket) {
        return Jdbc.run(dataSource, "bucket query failed", conn ->
                Jdbc.withStatement(conn, BUCKET_BY_ID_SQL, ps -> {
                    ps.setInt(1, bucket);
                    return Jdbc.withResultSet(ps, rs -> rs.next() ? Optional.of(mapBucket(rs)) : Optional.empty());
                }));
    }

    @Override
    public List<WorkerView> workers() {
        return Jdbc.run(dataSource, "workers query failed", conn ->
                Jdbc.withStatement(conn, WORKERS_SQL, ps -> Jdbc.withResultSet(ps, rs -> {
                    List<WorkerView> rows = new ArrayList<>();
                    while (rs.next()) {
                        rows.add(new WorkerView(rs.getString("owner"), instantOrNull(rs, "updated_at"), rs.getInt("bucket_count")));
                    }
                    return rows;
                })));
    }

    private Map<String, String> readMeta() {
        return Jdbc.run(dataSource, "tandem_meta query failed", conn ->
                Jdbc.withStatement(conn, META_SQL, ps -> Jdbc.withResultSet(ps, rs -> {
                    Map<String, String> meta = new HashMap<>();
                    while (rs.next()) {
                        meta.put(rs.getString("key"), rs.getString("value"));
                    }
                    return meta;
                })));
    }

    private int count(String sql) {
        return Jdbc.run(dataSource, "count query failed", conn ->
                Jdbc.withStatement(conn, sql, ps -> Jdbc.withResultSet(ps, rs -> {
                    rs.next();
                    return rs.getInt(1);
                })));
    }

    private static BucketStatusView mapBucket(ResultSet rs) throws SQLException {
        Instant oldestPending = instantOrNull(rs, "oldest_pending_at");
        Double lagAgeSeconds = oldestPending == null
                ? null
                : Math.max(0, (Instant.now().toEpochMilli() - oldestPending.toEpochMilli()) / 1000d);
        return new BucketStatusView(
                rs.getInt("bucket"),
                rs.getString("owner"),
                instantOrNull(rs, "lease_until"),
                rs.getBoolean("covered"),
                rs.getBoolean("paused"),
                rs.getLong("pending_count"),
                lagAgeSeconds);
    }

    private static Instant instantOrNull(ResultSet rs, String column) throws SQLException {
        OffsetDateTime odt = rs.getObject(column, OffsetDateTime.class);
        return odt == null ? null : odt.toInstant();
    }
}
