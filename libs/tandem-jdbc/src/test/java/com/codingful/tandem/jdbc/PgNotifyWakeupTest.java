package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

/**
 * The lifecycle of the listening thread, against a database that is not there. Needs no Docker: an
 * unroutable address fails the connection immediately, which is exactly the state a relay starting
 * while its database is down would be in, and the state its shutdown must survive.
 */
class PgNotifyWakeupTest {

    // Port 1 is reserved and never listening, so getConnection() fails at once with no network timeout.
    private final SimpleDataSource unreachable =
            new SimpleDataSource("jdbc:postgresql://127.0.0.1:1/none", "none", "none");

    private final CountingListener listener = new CountingListener();

    @Test
    void GIVEN_a_database_that_cannot_be_reached_WHEN_the_relay_shuts_down_THEN_the_listener_stops_promptly_and_signalled_nothing() {
        // A wakeup source that cannot subscribe must cost the relay nothing: no signal it did not
        // receive, and no shutdown it holds up while it retries.
        PgNotifyWakeup wakeup = new PgNotifyWakeup(unreachable);
        wakeup.start(listener);

        long startedAt = System.nanoTime();
        wakeup.stop();

        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).isLessThan(Duration.ofSeconds(5));
        assertThat(listener.wakes()).isZero();
        assertThat(listener.sweeps()).isZero();
    }

    @Test
    void GIVEN_a_wakeup_source_WHEN_started_or_stopped_twice_THEN_the_second_call_changes_nothing() {
        // Both halves are reachable from a relay's own lifecycle: stop() is called by both stop() and
        // kill(), and a supervised restart can re-enter start().
        PgNotifyWakeup wakeup = new PgNotifyWakeup(unreachable);

        wakeup.start(listener);
        assertThatCode(() -> wakeup.start(listener)).doesNotThrowAnyException();

        wakeup.stop();
        assertThatCode(wakeup::stop).doesNotThrowAnyException();
    }

    private static final class CountingListener implements WakeupSource.Listener {
        private final AtomicInteger wakes = new AtomicInteger();
        private final AtomicInteger sweeps = new AtomicInteger();

        @Override
        public void wake(int bucket) {
            wakes.incrementAndGet();
        }

        @Override
        public void wakeAll() {
            sweeps.incrementAndGet();
        }

        int wakes() {
            return wakes.get();
        }

        int sweeps() {
            return sweeps.get();
        }
    }
}
