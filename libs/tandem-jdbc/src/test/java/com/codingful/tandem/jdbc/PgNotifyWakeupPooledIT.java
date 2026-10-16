package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

/**
 * The wakeup over the kind of {@code DataSource} applications hand the relay: a pool, where closing a
 * connection returns its session to the pool rather than ending it (LLD-jdbc §3.10).
 */
class PgNotifyWakeupPooledIT extends AbstractPostgresIT {

    @Test
    void GIVEN_a_pooled_data_source_WHEN_the_wakeup_stops_THEN_the_pool_gets_back_a_connection_that_listens_to_nothing()
            throws Exception {
        HikariConfig config = new HikariConfig();
        config.setDataSource(DATA_SOURCE);
        config.setMaximumPoolSize(1);   // one session, so the next borrower gets the wakeup's own
        try (HikariDataSource pool = new HikariDataSource(config)) {
            CountDownLatch listening = new CountDownLatch(1);
            WakeupSource wakeup = WakeupSource.forWakeup(Wakeup.PG_NOTIFY, pool);
            wakeup.start(new WakeupSource.Listener() {
                @Override
                public void wake(int bucket) {
                }

                @Override
                public void wakeAll() {
                    listening.countDown();   // signalled once the subscription is in place
                }
            });
            assertThat(listening.await(10, TimeUnit.SECONDS)).isTrue();

            wakeup.stop();

            // An application borrowing it would otherwise be subscribed to every insert's notification,
            // queued on its connection by the driver and never read.
            try (Connection borrowed = pool.getConnection()) {
                assertThat(listeningChannels(borrowed)).isZero();
            }
        }
    }

    private static int listeningChannels(Connection connection) throws SQLException {
        try (Statement statement = connection.createStatement();
             ResultSet result = statement.executeQuery("SELECT count(*) FROM pg_listening_channels()")) {
            result.next();
            return result.getInt(1);
        }
    }
}
