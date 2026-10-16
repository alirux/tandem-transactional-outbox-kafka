package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.test.RecordingDispatcher;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * A worker that stalls between the broker's answer and its database write can find its lease reclaimed
 * and the row taken over by another worker. Its outcome, processed late, belongs to a superseded attempt
 * and must change nothing (LLD-jdbc §3.4.1, §3.4.2). The broker has already answered in every scenario here:
 * only the stalled worker's write is delayed, so none needs a publish exceeding its delivery timeout.
 */
class RelayWorkerStaleOutcomeIT extends AbstractPostgresIT {

    private static final String AGGREGATE = "order-with-delayed-outcome";
    private static final int BUCKETS = 256;
    private final JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void GIVEN_a_head_another_worker_delivered_after_failover_WHEN_the_old_failure_is_processed_THEN_delivery_is_not_reversed(
            boolean retriable) throws SQLException {
        long head = insertHead();
        RelayWorker stalled = workerWhoseAttemptFails(retriable);
        expireLeasesAndReclaim();

        RelayWorker successor = deliveringWorker("successor");
        assertThat(successor.claimAndDispatch()).isEqualTo(1);
        assertThat(successor.flushDone()).isEqualTo(1);

        stalled.flushFailures();

        assertThat(statusOf(head)).as("A superseded attempt cannot reverse successful delivery")
                .isEqualTo(OutboxStatus.DONE.code());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void GIVEN_a_head_another_worker_is_still_delivering_WHEN_the_old_failure_is_processed_THEN_the_newer_attempt_keeps_the_row(
            boolean retriable) throws SQLException {
        long head = insertHead();
        RelayWorker stalled = workerWhoseAttemptFails(retriable);
        expireLeasesAndReclaim();

        RelayWorker successor = deliveringWorker("successor");
        assertThat(successor.claimAndDispatch()).isEqualTo(1);   // acked, not yet written

        stalled.flushFailures();

        assertThat(statusOf(head)).as("The row stays with the attempt that holds it now")
                .isEqualTo(OutboxStatus.IN_FLIGHT.code());
        assertThat(successor.flushDone()).isEqualTo(1);
        assertThat(statusOf(head)).isEqualTo(OutboxStatus.DONE.code());
    }

    @ParameterizedTest
    @ValueSource(booleans = {true, false})
    void GIVEN_a_head_replayed_and_claimed_again_WHEN_the_old_failure_is_processed_THEN_the_newest_attempt_keeps_the_row(
            boolean retriable) throws SQLException {
        long head = insertHead();
        RelayWorker stalled = workerWhoseAttemptFails(retriable);
        expireLeasesAndReclaim();
        RelayWorker successor = deliveringWorker("successor");
        successor.claimAndDispatch();
        successor.flushDone();

        // A replay starts the row over with no attempts behind it, the count the stalled attempt was
        // claimed with: an outcome fenced on attempts alone would land on the newest claim.
        new JdbcReplayService(DATA_SOURCE).replay(
                new ReplayCriteria(AggregateId.of(AGGREGATE), null, null, null, Set.of(), false));
        RelayWorker newest = deliveringWorker("newest");
        assertThat(newest.claimAndDispatch()).isEqualTo(1);

        stalled.flushFailures();

        assertThat(statusOf(head)).isEqualTo(OutboxStatus.IN_FLIGHT.code());
    }

    private long insertHead() throws SQLException {
        insertEvent(1);
        try (var connection = DATA_SOURCE.getConnection();
             var statement = connection.prepareStatement("SELECT id FROM tandem_outbox");
             var result = statement.executeQuery()) {
            assertThat(result.next()).isTrue();
            return result.getLong(1);
        }
    }

    @Test
    void GIVEN_a_delivered_event_requested_for_replay_WHEN_an_ack_from_before_the_replay_is_flushed_THEN_the_replay_remains_pending()
            throws SQLException {
        long head = insertHead();
        RelayWorker stalled = deliveringWorker("stalled");
        assertThat(stalled.claimAndDispatch()).isEqualTo(1);
        expireLeasesAndReclaim();

        RelayWorker successor = deliveringWorker("successor");
        assertThat(successor.claimAndDispatch()).isEqualTo(1);
        assertThat(successor.flushDone()).isEqualTo(1);
        var replay = new JdbcReplayService(DATA_SOURCE).replay(
                new ReplayCriteria(AggregateId.of(AGGREGATE), null, null, null, Set.of(), false));
        assertThat(replay.replayed()).isEqualTo(1);
        assertThat(statusOf(head)).isEqualTo(OutboxStatus.PENDING.code());

        // Both deliveries preceded the operator's request. Neither fulfils the new replay.
        stalled.flushDone();

        assertThat(statusOf(head)).as("An earlier delivery must not cancel a requested replay")
                .isEqualTo(OutboxStatus.PENDING.code());
        assertThat(deliveringWorker("replay-worker").claimAndDispatch()).isEqualTo(1);
    }

    @Test
    void GIVEN_a_head_another_worker_is_still_delivering_WHEN_the_old_ack_is_flushed_THEN_the_aggregate_waits_for_the_newer_attempt()
            throws SQLException {
        long head = insertHead();
        insertEvent(2);
        RelayWorker stalled = deliveringWorker("stalled");
        assertThat(stalled.claimAndDispatch()).isEqualTo(1);
        expireLeasesAndReclaim();
        RelayWorker successor = deliveringWorker("successor");
        assertThat(successor.claimAndDispatch()).isEqualTo(1);   // acked, not yet written

        stalled.flushDone();

        assertThat(statusOf(head)).isEqualTo(OutboxStatus.IN_FLIGHT.code());
        assertThat(deliveringWorker("next").claimAndDispatch())
                .as("The next event must not go out while the newer attempt may still publish the head")
                .isZero();
    }

    private void insertEvent(long seq) {
        new JdbcOutboxRepository(DATA_SOURCE, BUCKETS).insert(OutboxMessage.builder()
                .aggregateId(AGGREGATE).aggregateType("Order")
                .seq(seq).payload("{}".getBytes(StandardCharsets.UTF_8)).build());
    }

    /** A worker that claimed the head and got the broker's failure, but has not written it yet. */
    private RelayWorker workerWhoseAttemptFails(boolean retriable) {
        RelayWorker worker = worker("stalled", new RecordingDispatcher().failAll(retriable));
        assertThat(worker.claimAndDispatch()).isEqualTo(1);
        return worker;
    }

    private RelayWorker deliveringWorker(String workerId) {
        return worker(workerId, new RecordingDispatcher());
    }

    private void expireLeasesAndReclaim() {
        execute("UPDATE tandem_outbox SET locked_until = now() - interval '1 second'");
        assertThat(store.reclaimExpiredLeases()).isEqualTo(1);
    }

    private RelayWorker worker(String workerId, RecordingDispatcher dispatcher) {
        return new RelayWorker(store, dispatcher, RelayConfig.builder().bucketCount(BUCKETS).build(),
                attempts -> Duration.ZERO, TandemMetrics.NOOP, Clock.systemUTC(), workerId,
                () -> Set.of(BucketHash.bucketFor(AGGREGATE, BUCKETS)));
    }

    private int statusOf(long id) throws SQLException {
        try (var connection = DATA_SOURCE.getConnection();
             var statement = connection.prepareStatement("SELECT status FROM tandem_outbox WHERE id = ?")) {
            statement.setLong(1, id);
            try (var result = statement.executeQuery()) {
                assertThat(result.next()).isTrue();
                return result.getInt(1);
            }
        }
    }
}
