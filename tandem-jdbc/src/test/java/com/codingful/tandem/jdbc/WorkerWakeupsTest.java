package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import org.junit.jupiter.api.Test;

class WorkerWakeupsTest {

    private static final int WORKERS = 4;

    /** Long enough that a test observing it end early cannot be observing the timeout instead. */
    private static final long A_LONG_WAIT_MS = Duration.ofSeconds(30).toMillis();

    @Test
    void GIVEN_a_signal_arriving_while_its_worker_is_still_claiming_WHEN_that_worker_next_waits_THEN_it_does_not_wait_at_all()
            throws InterruptedException {
        // The case the mechanism exists for, and the one a signal-as-notification-only design loses:
        // the write lands while the worker is mid-cycle, so nothing is waiting to be woken yet.
        WorkerWakeups wakeups = new WorkerWakeups(WORKERS);

        wakeups.wake(0);

        long startedAt = System.nanoTime();
        assertThat(wakeups.await(0, A_LONG_WAIT_MS)).isTrue();
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void GIVEN_a_worker_already_waiting_WHEN_a_bucket_of_its_slice_is_written_to_THEN_the_wait_ends_early()
            throws InterruptedException {
        WorkerWakeups wakeups = new WorkerWakeups(WORKERS);
        Thread writer = new Thread(() -> {
            sleep(50);
            wakeups.wake(WORKERS + 1);   // bucket 5 → worker 1 under the pool's bucket % workerCount split
        });

        writer.start();
        long startedAt = System.nanoTime();
        boolean woken = wakeups.await(1, A_LONG_WAIT_MS);
        writer.join();

        assertThat(woken).isTrue();
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).isLessThan(Duration.ofSeconds(5));
    }

    @Test
    void GIVEN_a_write_to_one_bucket_WHEN_the_workers_wait_THEN_only_the_worker_owning_that_slice_is_woken()
            throws InterruptedException {
        // What keeps a wakeup's cost independent of the worker count: waking every worker on every
        // write would put the whole pool through a claim that only one of them can satisfy.
        WorkerWakeups wakeups = new WorkerWakeups(WORKERS);

        wakeups.wake(2);

        assertThat(wakeups.await(2, 1)).isTrue();
        for (int index = 0; index < WORKERS; index++) {
            if (index != 2) {
                assertThat(wakeups.await(index, 1)).as("worker %d", index).isFalse();
            }
        }
    }

    @Test
    void GIVEN_a_source_that_cannot_say_which_buckets_it_missed_WHEN_it_wakes_them_all_THEN_every_worker_polls_at_once()
            throws InterruptedException {
        WorkerWakeups wakeups = new WorkerWakeups(WORKERS);

        wakeups.wakeAll();

        for (int index = 0; index < WORKERS; index++) {
            assertThat(wakeups.await(index, 1)).as("worker %d", index).isTrue();
        }
    }

    @Test
    void GIVEN_a_signal_already_consumed_WHEN_the_worker_waits_again_THEN_it_waits_out_its_backoff()
            throws InterruptedException {
        // A remembered signal must be a one-shot: left set, the worker would spin through claims that
        // find nothing for as long as nobody writes.
        WorkerWakeups wakeups = new WorkerWakeups(WORKERS);
        wakeups.wake(0);
        wakeups.await(0, 1);

        assertThat(wakeups.await(0, 1)).isFalse();
    }

    @Test
    void GIVEN_nothing_is_written_WHEN_a_worker_waits_THEN_it_waits_its_whole_backoff_and_polls() throws InterruptedException {
        WorkerWakeups wakeups = new WorkerWakeups(WORKERS);

        long startedAt = System.nanoTime();
        boolean woken = wakeups.await(0, 100);

        assertThat(woken).isFalse();
        assertThat(Duration.ofNanos(System.nanoTime() - startedAt)).isGreaterThanOrEqualTo(Duration.ofMillis(90));
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
