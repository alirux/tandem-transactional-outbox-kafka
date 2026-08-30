package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class PollBackoffTest {

    private static final Duration FLOOR = Duration.ofMillis(10);
    private static final Duration POLL = Duration.ofMillis(100);
    private static final double FACTOR = 2.0;
    private static final Duration CAP = Duration.ofSeconds(5);
    private static final int SAMPLES = 200;

    private static PollBackoff backoff() {
        return new PollBackoff(FLOOR, POLL, FACTOR, CAP);
    }

    @Test
    void GIVEN_a_bucket_under_a_live_stream_WHEN_a_worker_waits_after_delivering_rows_THEN_it_waits_the_floor() {
        PollBackoff backoff = backoff();
        for (int i = 0; i < SAMPLES; i++) {
            backoff.waitAfterCycle(20, 0);

            assertThat(backoff.waitAfterCycle(0, 0)).isBetween(8L, 12L);
        }
    }

    @Test
    void GIVEN_a_bucket_that_has_gone_quiet_WHEN_claim_after_claim_finds_nothing_THEN_the_wait_climbs_to_the_configured_interval() {
        // The point of the ramp: a worker must not keep querying at the floor for a bucket nobody is
        // writing to, which is what the idle query load is paid for.
        PollBackoff backoff = backoff();

        List<Long> climbing = new ArrayList<>();
        for (int i = 0; i < 4; i++) {
            climbing.add(backoff.waitAfterCycle(0, 0));   // 10, 20, 40, 80, all below the ceiling
        }

        // ±20% jitter stays under the doubling factor, so the climb is strictly monotonic.
        assertThat(climbing).isSorted().doesNotHaveDuplicates();
        assertThat(climbing.get(0)).isBetween(8L, 12L);
        List<Long> settled = new ArrayList<>();
        for (int i = 0; i < SAMPLES; i++) {
            settled.add(backoff.waitAfterCycle(0, 0));
        }
        assertThat(settled).allSatisfy(wait -> assertThat(wait).isBetween(80L, 120L));
    }

    @Test
    void GIVEN_a_worker_that_had_gone_quiet_WHEN_a_claim_finally_returns_rows_THEN_the_next_wait_is_the_floor_again() {
        PollBackoff backoff = backoff();
        for (int i = 0; i < 20; i++) {
            backoff.waitAfterCycle(0, 0);   // climb to the ceiling and stay there
        }

        backoff.waitAfterCycle(20, 0);

        assertThat(backoff.waitAfterCycle(0, 0)).isBetween(8L, 12L);
    }

    @Test
    void GIVEN_a_worker_that_had_gone_quiet_WHEN_a_wakeup_ends_its_wait_THEN_the_next_wait_is_the_floor_again() {
        // What makes the backoff and the wakeup one mechanism rather than two competing for the sleep:
        // a signal says the bucket is being written to again, so the ramp it climbed while quiet is
        // stale, exactly as it is after a claim that returned rows.
        PollBackoff backoff = backoff();
        for (int i = 0; i < 20; i++) {
            backoff.waitAfterCycle(0, 0);   // climb to the ceiling and stay there
        }

        backoff.resetIdle();

        assertThat(backoff.waitAfterCycle(0, 0)).isBetween(8L, 12L);
    }

    @Test
    void GIVEN_a_floor_configured_above_the_interval_WHEN_a_worker_waits_THEN_the_interval_still_bounds_the_wait() {
        // The ceiling is what an operator sizes the idle load against, so it wins over a floor set
        // carelessly above it rather than silently multiplying that load.
        PollBackoff backoff = new PollBackoff(Duration.ofMillis(500), POLL, FACTOR, CAP);
        backoff.waitAfterCycle(20, 0);

        for (int i = 0; i < SAMPLES; i++) {
            assertThat(backoff.waitAfterCycle(0, 0)).isBetween(80L, 120L);
        }
    }

    @Test
    void GIVEN_a_floor_equal_to_the_interval_WHEN_a_worker_waits_THEN_every_wait_is_that_interval() {
        // The documented way back to a fixed poll interval: no ramp, exactly the pre-adaptive timing.
        PollBackoff backoff = new PollBackoff(POLL, POLL, FACTOR, CAP);

        for (int i = 0; i < SAMPLES; i++) {
            assertThat(backoff.waitAfterCycle(0, 0)).isBetween(80L, 120L);
        }
    }

    @Test
    void GIVEN_a_gentler_growth_factor_WHEN_a_worker_keeps_finding_nothing_THEN_it_climbs_in_smaller_steps() {
        PollBackoff backoff = new PollBackoff(FLOOR, POLL, 1.5, CAP);

        long third = 0;
        for (int i = 0; i < 3; i++) {
            third = backoff.waitAfterCycle(0, 0);   // 10, 15, 23
        }

        // Deliberately no monotonicity assertion here: at 1.5 the jitter windows of consecutive steps
        // overlap, which is the trade the factor makes and not a defect.
        assertThat(third).isBetween(18L, 28L);   // a doubling factor would be at 40 by now
    }

    @Test
    void GIVEN_a_claim_that_returned_rows_WHEN_the_cycle_ends_THEN_the_worker_claims_again_without_waiting() {
        // The busy regime: work still remains, so the loop must not sleep at all between batches. A
        // per-cycle sleep here would cap a worker at batchSize/wait rows per second.
        PollBackoff backoff = backoff();

        assertThat(backoff.waitAfterCycle(20, 0)).isZero();
    }

    @Test
    void GIVEN_publishes_still_in_flight_WHEN_a_claim_finds_nothing_THEN_the_worker_pauses_briefly_instead_of_backing_off() {
        // Nothing to claim only because this worker's own publishes have not acked yet. That is not
        // idleness: it waits just long enough for completions to land, and the idle backoff must not
        // advance, or a worker draining a burst would be treated as a bucket nobody is writing to.
        PollBackoff backoff = backoff();

        for (int i = 0; i < SAMPLES; i++) {
            assertThat(backoff.waitAfterCycle(0, 3)).isEqualTo(5L);
        }

        assertThat(backoff.waitAfterCycle(0, 0)).isBetween(8L, 12L);
    }

    @Test
    void GIVEN_several_idle_workers_WHEN_they_wait_between_empty_claims_THEN_they_do_not_all_wait_the_same_time() {
        // The point of the jitter: workers started in the same instant must not keep polling in
        // lockstep for the lifetime of the relay. A fixed sleep would make this set a singleton.
        PollBackoff backoff = backoff();

        Set<Long> distinctWaits = new HashSet<>();
        for (int i = 0; i < SAMPLES; i++) {
            distinctWaits.add(backoff.waitAfterCycle(0, 0));
        }

        assertThat(distinctWaits).hasSizeGreaterThan(1);
    }

    @Test
    void GIVEN_a_worker_failing_every_cycle_WHEN_it_keeps_failing_THEN_each_wait_is_longer_than_the_last() {
        PollBackoff backoff = backoff();

        List<Long> waits = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            waits.add(backoff.waitAfterFailedCycle());
        }

        assertThat(waits.get(0)).isBetween(80L, 120L);   // the first failure waits the poll interval
        assertThat(waits).isSorted();                    // ±20% jitter stays under the doubling factor
        assertThat(waits).doesNotHaveDuplicates();
    }

    @Test
    void GIVEN_a_database_that_stays_down_WHEN_a_worker_has_failed_many_times_THEN_the_wait_stops_at_the_cap() {
        PollBackoff backoff = backoff();
        for (int i = 0; i < 20; i++) {
            backoff.waitAfterFailedCycle();   // saturate
        }

        List<Long> saturated = new ArrayList<>();
        for (int i = 0; i < SAMPLES; i++) {
            saturated.add(backoff.waitAfterFailedCycle());
        }

        // The cap is a hard ceiling: jitter is applied before the clamp, so a saturated worker waits
        // within 20% below it and never past it.
        assertThat(saturated).allSatisfy(wait -> assertThat(wait).isBetween(4_000L, CAP.toMillis()));
        assertThat(Collections.max(saturated)).isEqualTo(CAP.toMillis());
    }

    @Test
    void GIVEN_a_worker_that_had_been_failing_WHEN_one_cycle_completes_THEN_the_next_failure_waits_from_the_floor_again() {
        // Recovery must be as fast as the first failure was: a transient blip that resolves must not
        // leave the worker sleeping at the cap the next time it stumbles.
        PollBackoff backoff = backoff();
        for (int i = 0; i < 10; i++) {
            backoff.waitAfterFailedCycle();
        }

        backoff.waitAfterCycle(0, 0);   // one cycle completes, however little it found

        assertThat(backoff.waitAfterFailedCycle()).isBetween(80L, 120L);
    }

    @Test
    void GIVEN_a_cap_below_the_poll_interval_WHEN_a_worker_fails_THEN_it_still_waits_the_poll_interval() {
        // reclaimInterval can legitimately be configured shorter than pollInterval; a failure backoff
        // shorter than the idle one would retry a broken database faster than a healthy idle poll.
        PollBackoff backoff = new PollBackoff(FLOOR, POLL, FACTOR, Duration.ofMillis(10));

        for (int i = 0; i < 5; i++) {
            assertThat(backoff.waitAfterFailedCycle()).isBetween(80L, 120L);
        }
    }

    @Test
    void GIVEN_a_sub_millisecond_poll_interval_WHEN_a_worker_waits_THEN_it_still_yields_rather_than_spinning() {
        PollBackoff backoff = new PollBackoff(Duration.ZERO, Duration.ZERO, FACTOR, Duration.ZERO);

        assertThat(backoff.waitAfterCycle(0, 0)).isEqualTo(1);
        assertThat(backoff.waitAfterFailedCycle()).isEqualTo(1);
    }
}
