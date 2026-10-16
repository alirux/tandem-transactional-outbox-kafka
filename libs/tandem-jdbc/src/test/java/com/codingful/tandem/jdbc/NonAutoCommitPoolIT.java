package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.exception.TandemException;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * Tandem's own operations over a pool the application configured with {@code autoCommit=false}, a common
 * setting where Hibernate manages transactions. The relay's operations and an operator's are each a
 * transaction of their own (LLD-jdbc §3.1, Q9); on such a pool nothing commits unless Tandem does it, and
 * the pool rolls the work back when the connection is returned.
 */
class NonAutoCommitPoolIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;
    private static final String AGGREGATE = "order-1";

    private final HikariDataSource pool = nonAutoCommitPool();

    private static HikariDataSource nonAutoCommitPool() {
        HikariConfig config = new HikariConfig();
        config.setDataSource(DATA_SOURCE);
        config.setAutoCommit(false);
        return new HikariDataSource(config);
    }

    @AfterEach
    void closePool() {
        pool.close();
    }

    @Test
    void GIVEN_a_pool_without_autocommit_WHEN_the_relay_claims_and_acks_an_event_THEN_the_event_is_done() throws SQLException {
        insertEvent();
        JdbcOutboxStore store = new JdbcOutboxStore(pool, 10);
        Set<Integer> buckets = IntStream.range(0, BUCKETS).boxed().collect(Collectors.toSet());

        List<OutboxRecord> claimed = store.claimBatch(buckets, "worker", Duration.ofSeconds(60), 10);
        assertThat(claimed).hasSize(1);
        assertThat(statusOf(claimed.get(0).id())).as("the claim is committed").isEqualTo(OutboxStatus.IN_FLIGHT.code());

        store.markDoneBatch(claimed);

        assertThat(statusOf(claimed.get(0).id())).as("the ack is committed").isEqualTo(OutboxStatus.DONE.code());
    }

    @Test
    void GIVEN_a_pool_without_autocommit_WHEN_a_lease_coordinated_relay_beats_THEN_its_bucket_ownership_is_committed() {
        BucketLeaseManager leases = new BucketLeaseManager(pool, "instance-a", BUCKETS, Duration.ofSeconds(30));

        leases.heartbeat();

        // Read through a separate, autocommitting source: only committed ownership is visible there.
        assertThat(new BucketLeaseManager(DATA_SOURCE, "instance-a", BUCKETS, Duration.ofSeconds(30)).ownedBuckets())
                .hasSize(BUCKETS);
    }

    @Test
    void GIVEN_a_pool_without_autocommit_WHEN_the_relay_listens_for_wakeups_THEN_a_committed_write_still_signals_it()
            throws InterruptedException {
        CountDownLatch subscribed = new CountDownLatch(1);
        CountDownLatch woken = new CountDownLatch(1);
        WakeupSource wakeup = WakeupSource.forWakeup(Wakeup.PG_NOTIFY, pool);
        wakeup.start(new WakeupSource.Listener() {
            @Override
            public void wake(int bucket) {
                if (bucket == BucketHash.bucketFor(AGGREGATE, BUCKETS)) {
                    woken.countDown();
                }
            }

            @Override
            public void wakeAll() {
                subscribed.countDown();
            }
        });
        try {
            assertThat(subscribed.await(10, TimeUnit.SECONDS)).isTrue();

            JdbcOutboxRepository.builder(DATA_SOURCE, BUCKETS).wakeup(Wakeup.PG_NOTIFY).build().insert(event());

            assertThat(woken.await(10, TimeUnit.SECONDS)).as("the subscription was committed").isTrue();
        } finally {
            wakeup.stop();
        }
    }

    @Test
    void GIVEN_a_pool_without_autocommit_WHEN_an_operator_replays_a_delivered_event_THEN_it_is_pending_again() throws SQLException {
        insertEvent();
        execute("UPDATE tandem_outbox SET status = 2");

        new JdbcReplayService(pool).replay(new ReplayCriteria(AggregateId.of(AGGREGATE), null, null, null, Set.of(), false));

        assertThat(statusOf(1)).isEqualTo(OutboxStatus.PENDING.code());
    }

    @Test
    void GIVEN_a_pool_without_autocommit_WHEN_an_operator_discards_a_failed_event_THEN_it_is_discarded() throws SQLException {
        insertEvent();
        execute("UPDATE tandem_outbox SET status = 3");

        assertThat(new JdbcDiscardService(pool).discard(1, "duplicate of order-2")).isTrue();

        assertThat(statusOf(1)).isEqualTo(OutboxStatus.DISCARDED.code());
    }

    @Test
    void GIVEN_a_pool_without_autocommit_WHEN_an_operator_pauses_the_relay_THEN_the_relay_sees_it_paused() {
        new JdbcRelayControl(pool).pauseAll();

        JdbcRelayControlSource relay = new JdbcRelayControlSource(DATA_SOURCE, Coordination.SINGLE, Duration.ofSeconds(5));
        relay.refresh();
        assertThat(relay.wholeRelayPaused()).isTrue();
    }

    @Test
    void GIVEN_a_pool_without_autocommit_WHEN_the_bucket_count_is_first_established_THEN_a_different_one_is_refused_afterwards() {
        BucketCountGuard.check(pool, BUCKETS);

        assertThatThrownBy(() -> BucketCountGuard.check(DATA_SOURCE, BUCKETS / 2))
                .isInstanceOf(TandemConfigurationException.class);
    }

    @Test
    void GIVEN_a_pool_that_hands_back_connections_unreset_WHEN_a_relay_operation_fails_partway_THEN_the_next_one_still_runs()
            throws SQLException {
        insertEvent();
        try (Connection shared = DATA_SOURCE.getConnection()) {
            shared.setAutoCommit(false);
            DataSource unresetting = reusing(shared);

            assertThatThrownBy(() -> Jdbc.execCommitted(unresetting, "failing operation", connection -> {
                Jdbc.exec(connection, "UPDATE tandem_outbox SET status = 2", ps -> ps.executeUpdate());
                Jdbc.exec(connection, "SELECT no_such_column FROM tandem_outbox", ps -> ps.executeQuery());
            })).isInstanceOf(TandemException.class);

            // Left pending, the failed transaction would refuse every later statement on this connection.
            assertThat(new JdbcOutboxStore(unresetting, 10).failedCount()).hasValue(0);
            assertThat(statusOf(1)).as("none of the failed operation's work is kept").isEqualTo(OutboxStatus.PENDING.code());
        }
    }

    /**
     * A pool reduced to the case this needs: one connection, lent again as it was returned, its
     * transaction state not reset on return the way HikariCP's is.
     */
    private static DataSource reusing(Connection shared) {
        Connection lent = (Connection) Proxy.newProxyInstance(Connection.class.getClassLoader(),
                new Class<?>[] {Connection.class}, (proxy, method, args) -> {
                    if (method.getName().equals("close")) {
                        return null;
                    }
                    try {
                        return method.invoke(shared, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
        return (DataSource) Proxy.newProxyInstance(DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class}, (proxy, method, args) -> {
                    if (method.getName().equals("getConnection")) {
                        return lent;
                    }
                    throw new UnsupportedOperationException(method.getName());
                });
    }

    private static OutboxMessage event() {
        return OutboxMessage.builder().aggregateId(AGGREGATE).aggregateType("Order").unsequenced()
                .payload("{}".getBytes(StandardCharsets.UTF_8)).build();
    }

    private static void insertEvent() {
        new JdbcOutboxRepository(DATA_SOURCE, BUCKETS).insert(event());
    }

    private static int statusOf(long id) throws SQLException {
        try (var connection = DATA_SOURCE.getConnection();
             var statement = connection.prepareStatement("SELECT status FROM tandem_outbox WHERE id = ?")) {
            statement.setLong(1, id);
            try (var result = statement.executeQuery()) {
                result.next();
                return result.getInt(1);
            }
        }
    }
}
