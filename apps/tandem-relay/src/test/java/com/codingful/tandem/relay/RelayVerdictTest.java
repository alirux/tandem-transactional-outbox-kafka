package com.codingful.tandem.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.jdbc.Coordination;
import com.codingful.tandem.jdbc.RelayStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

/**
 * The verdict of LLD-relay §5.1, row by row. A reading is a plain record, so every case is built
 * directly: no relay, no database, and a fixed instant in place of a clock.
 */
class RelayVerdictTest {

    private static final String INSTANCE = "relay-a";
    private static final Instant NOW = Instant.parse("2026-01-01T12:00:00Z");
    private static final Duration STALLED_AFTER = Duration.ofSeconds(60);
    private static final int WORKERS = 4;

    private static RelayStatus running(int workersAlive, Duration sinceLastCycle) {
        return new RelayStatus(INSTANCE, RelayStatus.State.RUNNING, Coordination.LEASE, WORKERS, workersAlive,
                Optional.of(NOW.minus(sinceLastCycle)));
    }

    private static RelayStatus inState(RelayStatus.State state) {
        return new RelayStatus(INSTANCE, state, Coordination.SINGLE, WORKERS, WORKERS,
                Optional.of(NOW.minusSeconds(1)));
    }

    private static Health judge(RelayStatus status) {
        return RelayVerdict.of(status, false, NOW, STALLED_AFTER);
    }

    @Test
    void GIVEN_every_worker_alive_and_cycling_WHEN_the_relay_is_judged_THEN_it_is_up() {
        assertThat(judge(running(WORKERS, Duration.ofSeconds(1))).getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void GIVEN_a_relay_that_never_started_WHEN_it_is_judged_THEN_it_is_down() {
        assertThat(judge(inState(RelayStatus.State.STOPPED)).getStatus()).isEqualTo(Status.DOWN);
    }

    @Test
    void GIVEN_a_relay_draining_at_shutdown_WHEN_it_is_judged_THEN_it_is_out_of_service_rather_than_down() {
        assertThat(judge(inState(RelayStatus.State.STOPPING)).getStatus()).isEqualTo(Status.OUT_OF_SERVICE);
    }

    @Test
    void GIVEN_no_worker_alive_WHEN_the_relay_is_judged_THEN_it_is_down_and_reports_no_cycle_age() {
        RelayStatus noWorkers = new RelayStatus(INSTANCE, RelayStatus.State.RUNNING, Coordination.SINGLE, WORKERS,
                0, Optional.empty());

        Health health = judge(noWorkers);

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).doesNotContainKey(RelayVerdict.CYCLE_AGE_SECONDS);
    }

    @Test
    void GIVEN_a_worker_without_a_completed_cycle_for_longer_than_the_threshold_WHEN_the_relay_is_judged_THEN_it_is_down() {
        Duration pastTheThreshold = STALLED_AFTER.plusMillis(1);

        Health health = judge(running(WORKERS, pastTheThreshold));

        assertThat(health.getStatus()).isEqualTo(Status.DOWN);
        assertThat(health.getDetails()).containsEntry(RelayVerdict.CYCLE_AGE_SECONDS,
                pastTheThreshold.toMillis() / 1000d);
    }

    @Test
    void GIVEN_a_last_cycle_exactly_as_old_as_the_threshold_WHEN_the_relay_is_judged_THEN_it_is_still_up() {
        assertThat(judge(running(WORKERS, STALLED_AFTER)).getStatus()).isEqualTo(Status.UP);
    }

    @Test
    void GIVEN_some_workers_missing_WHEN_the_relay_is_judged_THEN_it_is_up_with_the_deficit_in_the_details() {
        int alive = WORKERS - 1;

        Health health = judge(running(alive, Duration.ofSeconds(1)));

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails())
                .containsEntry(RelayVerdict.WORKERS_CONFIGURED, WORKERS)
                .containsEntry(RelayVerdict.WORKERS_ALIVE, alive);
    }

    @Test
    void GIVEN_a_deliberately_paused_relay_WHEN_it_is_judged_THEN_it_is_up_and_says_it_is_paused() {
        Health health = RelayVerdict.of(running(WORKERS, Duration.ofSeconds(1)), true, NOW, STALLED_AFTER);

        assertThat(health.getStatus()).isEqualTo(Status.UP);
        assertThat(health.getDetails()).containsEntry(RelayVerdict.PAUSED, true);
    }

    @Test
    void GIVEN_any_reading_WHEN_the_relay_is_judged_THEN_the_details_identify_the_instance() {
        RelayStatus status = running(WORKERS, Duration.ofSeconds(1));

        Health health = judge(status);

        assertThat(health.getDetails())
                .containsEntry(RelayVerdict.INSTANCE_ID, status.instanceId())
                .containsEntry(RelayVerdict.STATE, status.state().name())
                .containsEntry(RelayVerdict.COORDINATION, status.coordination().name())
                .containsEntry(RelayVerdict.PAUSED, false);
    }
}
