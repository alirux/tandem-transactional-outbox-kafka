package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TracePropagator;
import com.codingful.tandem.test.RecordingDispatcher;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The post-commit wakeup end to end against a real PostgreSQL (dispatch-latency §3.4): the write-side
 * emission in {@link JdbcOutboxRepository} and the relay-side subscription in {@link PgNotifyWakeup},
 * with the database in between. What is worth pinning here is not that a message travels — that is the
 * database's job — but the two properties the mechanism's soundness rests on: a signal names the bucket
 * the row actually landed in, and a signal exists only if its transaction committed.
 */
class PgNotifyWakeupIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;

    /** Generous: these assertions are about what arrives, never about how fast. */
    private static final Duration ARRIVAL = Duration.ofSeconds(10);

    /** How long an "and nothing else" assertion waits before it accepts the silence. */
    private static final Duration SILENCE = Duration.ofMillis(500);

    private final CollectingListener listener = new CollectingListener();
    private PgNotifyWakeup wakeup;

    @BeforeEach
    void startListening() throws InterruptedException {
        wakeup = new PgNotifyWakeup(DATA_SOURCE);
        wakeup.start(listener);
        listener.awaitSubscribed();   // LISTEN is in force; anything committed from here on is delivered
    }

    @AfterEach
    void stopListening() {
        wakeup.stop();
    }

    @Test
    void GIVEN_a_relay_listening_for_wakeups_WHEN_a_row_is_written_THEN_the_bucket_it_landed_in_is_signalled()
            throws InterruptedException {
        signallingRepository(DATA_SOURCE).insert(message("order-wakeup-1", 1));

        assertThat(listener.awaitSignal()).isEqualTo(storedBucketOf("order-wakeup-1"));
    }

    @Test
    void GIVEN_a_write_that_is_rolled_back_WHEN_a_later_write_commits_THEN_only_the_committed_one_is_signalled()
            throws Exception {
        // The property that makes signalling from inside the transaction sound rather than merely
        // convenient: a signal can never announce a row that does not exist.
        try (Connection rolledBack = DATA_SOURCE.getConnection()) {
            rolledBack.setAutoCommit(false);
            signallingRepository(pinnedTo(rolledBack)).insert(message("order-rolled-back", 1));
            rolledBack.rollback();
        }

        signallingRepository(DATA_SOURCE).insert(message("order-committed", 1));

        assertThat(listener.awaitSignal()).isEqualTo(storedBucketOf("order-committed"));
        assertThat(listener.poll(SILENCE)).as("the rolled-back write must have signalled nothing").isNull();
    }

    @Test
    void GIVEN_a_batch_spanning_several_aggregates_WHEN_it_is_written_THEN_each_bucket_it_wrote_is_signalled_once()
            throws InterruptedException {
        // Two rows of one aggregate and one of another: the relay has to hear about both buckets, and
        // exactly once each — the duplicate is collapsed rather than waking a worker twice for one write.
        signallingRepository(DATA_SOURCE).insertAll(List.of(
                message("order-batch-a", 1), message("order-batch-a", 2), message("order-batch-b", 1)));

        assertThat(listener.awaitSignals(2))
                .containsExactlyInAnyOrder(storedBucketOf("order-batch-a"), storedBucketOf("order-batch-b"));
        assertThat(listener.poll(SILENCE)).as("the repeated bucket must not be signalled twice").isNull();
    }

    @Test
    void GIVEN_a_write_side_that_does_not_signal_WHEN_it_writes_THEN_the_relay_hears_nothing_about_it()
            throws InterruptedException {
        // The default, and what every existing deployment keeps doing: the row is found by polling, and
        // the write pays for no extra statement.
        new JdbcOutboxRepository(DATA_SOURCE, BUCKETS).insert(message("order-silent", 1));

        signallingRepository(DATA_SOURCE).insert(message("order-loud", 1));

        assertThat(listener.awaitSignal()).isEqualTo(storedBucketOf("order-loud"));
        assertThat(listener.poll(SILENCE)).isNull();
    }

    @Test
    void GIVEN_a_notification_this_relay_cannot_read_WHEN_it_arrives_THEN_it_is_skipped_and_later_signals_still_land()
            throws InterruptedException {
        // The channel is a contract that may grow a richer payload later (HLD §1.4). A listener that
        // died on an unrecognised one would turn that additive change into an outage.
        execute("SELECT pg_notify('" + Wakeup.CHANNEL + "', 'a-future-payload')");

        signallingRepository(DATA_SOURCE).insert(message("order-after-garbage", 1));

        assertThat(listener.awaitSignal()).isEqualTo(storedBucketOf("order-after-garbage"));
    }

    @Test
    void GIVEN_the_listening_connection_is_dropped_WHEN_a_row_is_written_afterwards_THEN_the_source_has_resubscribed_and_still_signals()
            throws Exception {
        // A relay listens for months; connections do not last that long. What matters is that the gap
        // is bounded and self-healing, and that the sweep on resubscribing covers whatever was written
        // while nobody was listening.
        terminateListeningBackend();

        assertThat(listener.awaitSweep()).as("the source must resubscribe and ask for a sweep").isTrue();
        signallingRepository(DATA_SOURCE).insert(message("order-after-reconnect", 1));

        assertThat(listener.awaitSignal()).isEqualTo(storedBucketOf("order-after-reconnect"));
    }

    @Test
    void GIVEN_a_relay_whose_next_poll_is_half_a_minute_away_WHEN_a_row_is_written_THEN_it_is_dispatched_at_once()
            throws Exception {
        // The mechanism as an operator sees it, over a real database and a real relay: with both ends of
        // the idle backoff pinned at 30s, a dispatch inside a second cannot be a poll finding the row.
        RelayConfig cfg = RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(2)
                .pollIntervalFloor(Duration.ofSeconds(30)).pollInterval(Duration.ofSeconds(30)).build();
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        CountDownLatch relaySubscribed = new CountDownLatch(1);
        WorkerPool relay = new WorkerPool(new JdbcOutboxStore(DATA_SOURCE, cfg.maxAttempts()), dispatcher, cfg,
                TandemMetrics.NOOP, Clock.systemUTC(), BackoffStrategy.fullJitter(),
                BucketSource.embedded(BUCKETS), RelayControlSource.NOOP, announcing(relaySubscribed));

        relay.start();
        try {
            // Writing before the relay's own LISTEN is in force would leave the row to the 30s poll —
            // a real risk for a relay that starts under load, and a flaky test here. The source
            // announces its subscription by asking for a full sweep, so that is what this waits on.
            assertThat(relaySubscribed.await(ARRIVAL.toSeconds(), TimeUnit.SECONDS))
                    .as("the relay must be listening before the write")
                    .isTrue();
            long writtenAtNanos = System.nanoTime();
            signallingRepository(DATA_SOURCE).insert(message("order-relayed", 1));

            awaitUpTo(ARRIVAL, () -> dispatcher.dispatchCount() == 1);

            assertThat(Duration.ofNanos(System.nanoTime() - writtenAtNanos)).isLessThan(Duration.ofSeconds(5));
        } finally {
            relay.stop();
        }
    }

    /**
     * A {@link PgNotifyWakeup} that counts {@code subscribed} down as soon as its subscription is in
     * force, so the test can write at a moment where a signal is guaranteed to be delivered.
     */
    private static WakeupSource announcing(CountDownLatch subscribed) {
        PgNotifyWakeup delegate = new PgNotifyWakeup(DATA_SOURCE);
        return new WakeupSource() {
            @Override
            public void start(Listener listener) {
                delegate.start(new Listener() {
                    @Override
                    public void wake(int bucket) {
                        listener.wake(bucket);
                    }

                    @Override
                    public void wakeAll() {
                        subscribed.countDown();
                        listener.wakeAll();
                    }
                });
            }

            @Override
            public void stop() {
                delegate.stop();
            }
        };
    }

    private static void awaitUpTo(Duration timeout, java.util.function.BooleanSupplier condition)
            throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (!condition.getAsBoolean()) {
            assertThat(System.nanoTime()).as("timed out after %s", timeout).isLessThan(deadline);
            Thread.sleep(10);
        }
    }

    /** Kills the backend serving the source's subscription, the way a restart or a timeout would. */
    private static void terminateListeningBackend() {
        execute("SELECT pg_terminate_backend(pid) FROM pg_stat_activity"
                + " WHERE query = 'LISTEN " + Wakeup.CHANNEL + "' AND pid <> pg_backend_pid()");
    }

    private static JdbcOutboxRepository signallingRepository(DataSource dataSource) {
        return new JdbcOutboxRepository(dataSource, BUCKETS,
                TracePropagator.NOOP, Wakeup.PG_NOTIFY);
    }

    private static OutboxMessage message(String aggregateId, long seq) {
        return OutboxMessage.builder()
                .aggregateId(aggregateId).aggregateType("Order").type("OrderChanged").seq(seq)
                .payload("{\"v\":1}".getBytes(StandardCharsets.UTF_8))
                .build();
    }

    /**
     * The bucket the database actually holds for that aggregate's rows — read back rather than
     * recomputed, so the assertion compares the signal against the stored row instead of against the
     * same hash that produced both.
     */
    private static int storedBucketOf(String aggregateId) {
        try (Connection conn = DATA_SOURCE.getConnection();
             PreparedStatement ps = conn.prepareStatement(
                     "SELECT DISTINCT bucket FROM tandem_outbox WHERE aggregate_id = ?")) {
            ps.setString(1, aggregateId);
            try (ResultSet rs = ps.executeQuery()) {
                assertThat(rs.next()).as("a row for %s", aggregateId).isTrue();
                return rs.getInt(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test query failed", e);
        }
    }

    /**
     * A {@link DataSource} always handing back the given connection, ignoring {@code close()} — the
     * test's stand-in for Spring's {@code TransactionAwareDataSourceProxy}, so the insert and its
     * signal join the transaction this test controls.
     */
    private static DataSource pinnedTo(Connection connection) {
        Connection nonClosing = (Connection) Proxy.newProxyInstance(
                Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class},
                (proxy, method, args) -> "close".equals(method.getName()) ? null : method.invoke(connection, args));
        return new DataSource() {
            @Override
            public Connection getConnection() {
                return nonClosing;
            }

            @Override
            public Connection getConnection(String username, String password) {
                return nonClosing;
            }

            @Override
            public java.io.PrintWriter getLogWriter() {
                return null;
            }

            @Override
            public void setLogWriter(java.io.PrintWriter out) {
            }

            @Override
            public void setLoginTimeout(int seconds) {
            }

            @Override
            public int getLoginTimeout() {
                return 0;
            }

            @Override
            public java.util.logging.Logger getParentLogger() {
                return java.util.logging.Logger.getGlobal();
            }

            @Override
            public <T> T unwrap(Class<T> iface) {
                return iface.cast(this);
            }

            @Override
            public boolean isWrapperFor(Class<?> iface) {
                return iface.isInstance(this);
            }
        };
    }

    /** Collects what the source delivers, so a test can assert on arrivals and on silence alike. */
    private static final class CollectingListener implements WakeupSource.Listener {

        private final LinkedBlockingQueue<Integer> buckets = new LinkedBlockingQueue<>();
        private final LinkedBlockingQueue<Object> sweeps = new LinkedBlockingQueue<>();

        @Override
        public void wake(int bucket) {
            buckets.add(bucket);
        }

        @Override
        public void wakeAll() {
            sweeps.add(new Object());   // the source calls this the moment its LISTEN is in force
        }

        void awaitSubscribed() throws InterruptedException {
            assertThat(sweeps.poll(ARRIVAL.toSeconds(), TimeUnit.SECONDS))
                    .as("the wakeup source must have subscribed")
                    .isNotNull();
        }

        /** @return whether a further sweep arrived, which is how a resubscription announces itself */
        boolean awaitSweep() throws InterruptedException {
            return sweeps.poll(ARRIVAL.toSeconds(), TimeUnit.SECONDS) != null;
        }

        int awaitSignal() throws InterruptedException {
            return awaitSignals(1).get(0);
        }

        List<Integer> awaitSignals(int count) throws InterruptedException {
            List<Integer> received = new ArrayList<>();
            for (int i = 0; i < count; i++) {
                Integer bucket = poll(ARRIVAL);
                assertThat(bucket).as("signal %d of %d", i + 1, count).isNotNull();
                received.add(bucket);
            }
            return received;
        }

        Integer poll(Duration timeout) throws InterruptedException {
            return buckets.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
        }
    }
}
