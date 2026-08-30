package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.SeqSource;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.TracePropagator;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import javax.sql.DataSource;

/**
 * Write-side JDBC adapter (LLD-jdbc §2): inserts rows into {@code tandem_outbox} <b>using the
 * connection the {@link DataSource} hands back</b> and never opening or committing its own
 * transaction — so with a transaction-aware {@code DataSource} the insert joins the caller's
 * {@code @Transactional}, atomically with the business state change.
 *
 * <p>It depends only on {@code java.sql} (JDK): <b>no Kafka, no JSON library</b> (the {@code payload}
 * is the bytes the client already serialized; only {@code headers} is encoded, via {@link MiniJson}) —
 * the minimal client footprint (§1.3).
 *
 * <p><b>Optional write-side lock:</b> a message built with {@link OutboxMessage.Builder#lockedWrite()}
 * makes {@link #insert}/{@link #insertAll} take a transaction-scoped {@code pg_advisory_xact_lock} on
 * the aggregate, on the same connection, before the row's INSERT (HLD-managed-seq §4.2). Within one
 * {@link #insertAll} batch this adapter takes every such lock the batch needs in <b>sorted aggregate id
 * order</b> before inserting anything, which is the stated deadlock guard for a transaction that locks
 * several aggregates. It cannot enforce that ordering across separate {@link #insert} calls in one
 * caller transaction — a caller doing that is responsible for the same ordering itself.
 *
 * <p><b>Optional post-commit wakeup:</b> constructed with {@link Wakeup#PG_NOTIFY}, the insert is
 * followed by one {@code pg_notify} carrying the bucket numbers just written, on the same connection
 * and so inside the caller's transaction — the database delivers it on commit and discards it on
 * rollback, so a signal can never announce a row that does not exist (dispatch-latency §3.4). It costs
 * one round trip per {@code insert}/{@code insertAll} call, whatever the number of rows, and nothing at
 * all with the default {@link Wakeup#NONE}.
 *
 * <p><b>Payload constraint:</b> the {@code payload} column is PostgreSQL {@code jsonb}, so this
 * adapter requires {@link OutboxMessage#payload()} to be valid UTF-8 JSON. A non-JSON payload is
 * rejected by Postgres at insert time and surfaces as an {@link OutboxInsertException} (SQLSTATE
 * {@code 22P02}, {@code invalid_text_representation}). Callers that must carry opaque (non-JSON)
 * bytes should JSON-encode them (e.g. a base64 string) before insert, or use a schema whose
 * {@code payload} column is {@code bytea}/{@code text}. Note also that {@code jsonb} stores the
 * <b>parsed</b> value, not the original text: the payload read back (and published to Kafka) is
 * semantically identical but not byte-identical — key order, whitespace and number formatting are
 * normalized — so consumers must not verify signatures/hashes computed over the original bytes.
 */
public final class JdbcOutboxRepository implements OutboxRepository {

    /**
     * Carries {@code seq} explicitly, for an application-assigned number and for an
     * {@link OutboxMessage.Builder#unsequenced()} row alike — the latter binds SQL {@code NULL}.
     * Binding is what distinguishes them from a managed row: the column has a {@code DEFAULT}, so
     * <b>omitting</b> it would silently draw a managed number instead of storing none.
     */
    private static final String INSERT_SQL =
            "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq, seq_source, payload, headers, correlation_id) "
                    + "VALUES (?, ?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?)";

    /**
     * The same insert with {@code seq} left out, for a message that leaves the number to Tandem
     * ({@link OutboxMessage.Builder#managedSeq()}): omitting the column is what makes the
     * {@code tandem_seq} default fire, and it is the entire mechanism — no extra statement, no round
     * trip and no lock inside the caller's transaction (HLD-managed-seq §4.1).
     */
    private static final String INSERT_MANAGED_SEQ_SQL =
            "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, type, bucket, seq_source, payload, headers, correlation_id) "
                    + "VALUES (?, ?, ?, ?, ?, CAST(? AS jsonb), CAST(? AS jsonb), ?)";

    /**
     * The wakeup signal (dispatch-latency §3.4), emitted once per insert call rather than once per row:
     * a single statement over the array of distinct buckets written, so a fifty-row {@code insertAll}
     * costs the same one round trip as a single {@code insert}. PostgreSQL collapses same-channel,
     * same-payload notifications within a transaction, so a caller inserting into one bucket across
     * several calls still wakes the relay once.
     */
    private static final String NOTIFY_SQL = "SELECT pg_notify(?, bucket::text) FROM unnest(?) AS bucket";

    /** PostgreSQL {@code unique_violation} SQLSTATE (LLD-jdbc §2 → DuplicateSeqException). */
    private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

    /**
     * Blocks until granted, serialising concurrent writers to the same aggregate (HLD-managed-seq
     * §4.2); {@code hashtext} folds the id into the 32-bit key the single-argument form takes.
     * PostgreSQL releases it automatically at commit or rollback of the caller's transaction — there
     * is no corresponding unlock statement.
     */
    private static final String LOCK_AGGREGATE_SQL = "SELECT pg_advisory_xact_lock(hashtext(?))";

    /**
     * Width of the {@code correlation_id} column, which the value is truncated to at insert (LLD-jdbc §2).
     * The correlation id normally originates <b>outside</b> this application — an inbound HTTP header, a
     * consumed message — so it is untrusted input; an unbounded value would otherwise fail the insert (and
     * with it the caller's business transaction) or bloat the index it exists to serve. The header copy in
     * {@code headers} is left at full length: it is what reaches Kafka, and it is not indexed.
     */
    public static final int MAX_CORRELATION_ID_LENGTH = 255;

    private final DataSource dataSource;
    private final int bucketCount;
    private final TracePropagator tracePropagator;
    private final Wakeup wakeup;

    /**
     * <p>This constructor does <b>no</b> I/O: the {@code dataSource} may be a transaction-aware proxy
     * that only yields a connection inside a caller transaction (that is the point — {@link #insert}
     * joins the caller's {@code @Transactional}), so it cannot be queried at construction time. The
     * bucket-count guard (LLD-bucket-count-guard) is therefore an explicit assembly step run against a
     * plain {@code DataSource} — {@link BucketCountGuard} — not something this constructor performs.
     *
     * <p>Delegates to the 3-arg constructor with {@link TracePropagator#NOOP} — trace/correlation
     * capture disabled (HLD-tracing.md §7).
     *
     * @param dataSource  the write-side connection source; the insert joins whatever transaction the
     *                     returned {@link Connection} is already part of
     * @param bucketCount must match the relay's {@link RelayConfig#bucketCount()} — baked into every
     *                     row inserted, never change after first deployment
     * @throws IllegalArgumentException if {@code bucketCount <= 0} or above
     *                                  {@link RelayConfig#MAX_BUCKET_COUNT} (the {@code SMALLINT} column bound)
     */
    public JdbcOutboxRepository(DataSource dataSource, int bucketCount) {
        this(dataSource, bucketCount, TracePropagator.NOOP);
    }

    /**
     * @param dataSource      the write-side connection source; see {@link #JdbcOutboxRepository(DataSource, int)}
     * @param bucketCount     must match the relay's {@link RelayConfig#bucketCount()}; see
     *                        {@link #JdbcOutboxRepository(DataSource, int)}
     * @param tracePropagator captures trace/correlation headers at insert time when
     *                        {@link TracePropagator#isEnabled()} (HLD-tracing.md §5); real adapters ship
     *                        in {@code tandem-spring-producer} / {@code tandem-tracing-otel}
     * @throws IllegalArgumentException if {@code bucketCount <= 0} or above
     *                                  {@link RelayConfig#MAX_BUCKET_COUNT} (the {@code SMALLINT} column bound)
     * @throws NullPointerException     if {@code dataSource} or {@code tracePropagator} is {@code null}
     */
    public JdbcOutboxRepository(DataSource dataSource, int bucketCount, TracePropagator tracePropagator) {
        this(dataSource, bucketCount, tracePropagator, Wakeup.NONE);
    }

    /**
     * @param dataSource      the write-side connection source; see {@link #JdbcOutboxRepository(DataSource, int)}
     * @param bucketCount     must match the relay's {@link RelayConfig#bucketCount()}; see
     *                        {@link #JdbcOutboxRepository(DataSource, int)}
     * @param tracePropagator captures trace/correlation headers at insert time when
     *                        {@link TracePropagator#isEnabled()} (HLD-tracing.md §5)
     * @param wakeup          whether each insert also signals the relay that the buckets it wrote have
     *                        work ({@link Wakeup#PG_NOTIFY}), or leaves discovery to the relay's poll
     *                        loop ({@link Wakeup#NONE}, the default). The relay must be listening for
     *                        the same mechanism, or the signal is simply never consumed
     * @throws IllegalArgumentException if {@code bucketCount <= 0} or above
     *                                  {@link RelayConfig#MAX_BUCKET_COUNT} (the {@code SMALLINT} column bound)
     * @throws NullPointerException     if any argument is {@code null}
     */
    public JdbcOutboxRepository(DataSource dataSource, int bucketCount, TracePropagator tracePropagator,
            Wakeup wakeup) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
        this.bucketCount = RelayConfig.boundedBucketCount(bucketCount);
        this.tracePropagator = Objects.requireNonNull(tracePropagator, "tracePropagator");
        this.wakeup = Objects.requireNonNull(wakeup, "wakeup");
    }

    @Override
    public void insert(OutboxMessage message) {
        Objects.requireNonNull(message, "message");
        Jdbc.exec(dataSource, conn -> {
            if (message.lockedWrite()) {
                lockAggregate(conn, message.aggregateId().value());
            }
            Jdbc.exec(conn, sqlFor(message), ps -> {
                bind(ps, message);
                ps.executeUpdate();
            });
            signalWakeup(conn, message);
        }, e -> translate(e, message));
    }

    @Override
    public void insertAll(Collection<OutboxMessage> messages) {
        Objects.requireNonNull(messages, "messages");
        if (messages.isEmpty()) {
            return;
        }
        // Mutated inside the lambda below and read from the exception mapper on failure — an array
        // (not a plain local) because the mapper closes over it after the lambda returns/throws.
        OutboxMessage[] current = new OutboxMessage[1];
        Jdbc.exec(dataSource, conn -> {
            lockAggregatesInOrder(conn, messages);
            // The two modes need different column lists, so they cannot share one batch. Split on
            // CONSECUTIVE runs rather than partitioning by mode: the tiers promise rows land in the
            // collection's order, and grouping all managed messages together would reorder them.
            List<OutboxMessage> run = new ArrayList<>();
            for (OutboxMessage message : messages) {
                if (!run.isEmpty() && isManaged(message) != isManaged(run.get(0))) {
                    insertBatch(conn, run, current);
                    run.clear();
                }
                run.add(message);
            }
            insertBatch(conn, run, current);
            signalWakeup(conn, bucketsOf(messages));
        }, e -> translate(e, current[0]));
    }

    private void insertBatch(Connection conn, List<OutboxMessage> batch, OutboxMessage[] current)
            throws SQLException {
        Jdbc.exec(conn, sqlFor(batch.get(0)), ps -> {
            for (OutboxMessage message : batch) {
                current[0] = message;
                bind(ps, message);
                ps.addBatch();
            }
            ps.executeBatch();
        });
    }

    private static String sqlFor(OutboxMessage message) {
        return isManaged(message) ? INSERT_MANAGED_SEQ_SQL : INSERT_SQL;
    }

    /**
     * The only mode needing the other column list, and so the only split a batch has to make: an
     * application-assigned and an unsequenced row share one statement and batch together freely.
     */
    private static boolean isManaged(OutboxMessage message) {
        return message.seqSource() == SeqSource.MANAGED;
    }

    /**
     * Takes every {@link OutboxMessage#lockedWrite()} lock the batch needs, in <b>sorted aggregate id
     * order</b>, before any row in the batch is inserted — the ordering that avoids a deadlock between
     * two transactions locking the same two aggregates in different order (HLD-managed-seq §5).
     */
    private static void lockAggregatesInOrder(Connection conn, Collection<OutboxMessage> messages) throws SQLException {
        SortedSet<String> aggregateIds = new TreeSet<>();
        for (OutboxMessage message : messages) {
            if (message.lockedWrite()) {
                aggregateIds.add(message.aggregateId().value());
            }
        }
        for (String aggregateId : aggregateIds) {
            lockAggregate(conn, aggregateId);
        }
    }

    /**
     * Tells the relay that {@code buckets} have work, in the caller's own transaction so the signal
     * lives or dies with the rows it announces (dispatch-latency §3.4). Nothing here can affect
     * delivery: a relay that is not listening, or a pooler that swallowed the subscription, only means
     * the rows are found by the next poll instead.
     *
     * <p>What it <i>can</i> do is fail, and then it fails the caller's transaction like any other
     * statement in it (the documented case being a full cluster-wide notification queue). Swallowing
     * the exception would not avoid that: a statement that errored has already poisoned the
     * transaction, so there is nothing left to save.
     */
    private void signalWakeup(Connection conn, OutboxMessage message) throws SQLException {
        if (wakeup != Wakeup.NONE) {
            signalWakeup(conn, Set.of(bucketOf(message)));
        }
    }

    private void signalWakeup(Connection conn, Set<Integer> buckets) throws SQLException {
        if (wakeup == Wakeup.NONE || buckets.isEmpty()) {
            return;
        }
        Jdbc.exec(conn, NOTIFY_SQL, ps -> {
            ps.setString(1, Wakeup.CHANNEL);
            ps.setArray(2, conn.createArrayOf("integer", buckets.toArray()));
            ps.execute();
        });
    }

    /** Distinct buckets a batch writes into — what the single notify statement is bound to. */
    private Set<Integer> bucketsOf(Collection<OutboxMessage> messages) {
        if (wakeup == Wakeup.NONE) {
            return Set.of();   // the hash is cheap, but there is no reason to run it per row for nothing
        }
        Set<Integer> buckets = new LinkedHashSet<>();
        for (OutboxMessage message : messages) {
            buckets.add(bucketOf(message));
        }
        return buckets;
    }

    /** The bucket a message lands in — the same hash {@link #bind} writes into the row. */
    private int bucketOf(OutboxMessage message) {
        return BucketHash.bucketFor(message.aggregateId().value(), bucketCount);
    }

    private static void lockAggregate(Connection conn, String aggregateId) throws SQLException {
        Jdbc.exec(conn, LOCK_AGGREGATE_SQL, ps -> {
            ps.setString(1, aggregateId);
            ps.execute();
        });
    }

    /** Parameter positions are walked rather than fixed: a managed-{@code seq} insert has one fewer. */
    private void bind(PreparedStatement ps, OutboxMessage message) throws SQLException {
        SeqSource seqSource = message.seqSource();
        int index = 1;
        ps.setString(index++, message.aggregateId().value());
        ps.setString(index++, message.aggregateType());
        if (message.type() == null) {
            ps.setNull(index++, Types.VARCHAR);
        } else {
            ps.setString(index++, message.type());
        }
        ps.setInt(index++, BucketHash.bucketFor(message.aggregateId().value(), bucketCount));
        // A managed row omits the column so its DEFAULT can fire; the other two bind it, an
        // unsequenced row to NULL — which the column's CHECK ties to seq_source = NONE.
        if (seqSource != SeqSource.MANAGED) {
            if (message.hasSeq()) {
                ps.setLong(index++, message.seq());
            } else {
                ps.setNull(index++, Types.BIGINT);
            }
        }
        ps.setInt(index++, seqSource.code());
        ps.setString(index++, new String(message.payload(), StandardCharsets.UTF_8));
        Map<String, String> headers = effectiveHeaders(message);
        ps.setString(index++, MiniJson.writeObject(headers));
        String correlationId = truncate(headers.get(TandemHeaders.CORRELATION_ID));
        if (correlationId == null) {
            ps.setNull(index, Types.VARCHAR);
        } else {
            ps.setString(index, correlationId);
        }
    }

    /** Bounds an untrusted, externally-originated correlation id to the column width; {@code null} stays null. */
    private static String truncate(String correlationId) {
        return correlationId == null || correlationId.length() <= MAX_CORRELATION_ID_LENGTH
                ? correlationId
                : correlationId.substring(0, MAX_CORRELATION_ID_LENGTH);
    }

    /**
     * The headers actually stored: the message headers with {@code contentType} folded into
     * {@code headers["content-type"]} (the typed field is the single source of truth — it overrides
     * any {@code content-type} already in the map), plus any trace/correlation context captured from
     * {@link #tracePropagator} for a key not already present — an explicit header the caller set is
     * never overwritten by captured context (LLD-jdbc §2, HLD-tracing.md §5).
     *
     * <p>Also the source of the {@code correlation_id} column: whatever ends up under
     * {@code headers["correlation-id"]} here is copied into it, so the column can never disagree with
     * the header that actually reaches Kafka.
     */
    private Map<String, String> effectiveHeaders(OutboxMessage message) {
        Map<String, String> headers = new LinkedHashMap<>(message.headers());
        if (message.contentType() != null) {
            headers.put(TandemHeaders.CONTENT_TYPE, message.contentType());
        }
        if (tracePropagator.isEnabled()) {
            for (Map.Entry<String, String> captured : tracePropagator.capture().entrySet()) {
                headers.putIfAbsent(captured.getKey(), captured.getValue());
            }
        }
        return headers;
    }

    private static OutboxInsertException translate(SQLException e, OutboxMessage message) {
        String aggregate = message == null ? "?" : message.aggregateId().toString();
        // Never a number for a managed message: it has none until this insert succeeds, and printing
        // one would send whoever reads the failure looking for a row that does not exist. An
        // unsequenced row renders as "none" for the same reason — there is nothing to point at.
        String seq = message == null ? "?" : message.hasSeq()
                ? Long.toString(message.seq())
                : message.seqSource().toString();
        if (SQLSTATE_UNIQUE_VIOLATION.equals(e.getSQLState())) {
            return new DuplicateSeqException(
                    "duplicate (aggregate_id, seq) = (" + aggregate + ", " + seq + ')', e);
        }
        return new OutboxInsertException(
                "failed to insert outbox row for (aggregate_id, seq) = (" + aggregate + ", " + seq + ')', e);
    }
}
