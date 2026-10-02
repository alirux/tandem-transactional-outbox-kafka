package com.codingful.tandem.relay;

import com.codingful.tandem.jdbc.RelayStatus;
import java.time.Duration;
import java.time.Instant;
import org.springframework.boot.health.contributor.Health;
import org.springframework.boot.health.contributor.Status;

/**
 * The health verdict the library deliberately leaves to the embedding application (LLD-relay §5.1): a
 * pure function of one {@link RelayStatus} reading, the pause flag, the current instant and the stall
 * threshold. Nothing here reads a clock or a database, which is what lets every row of the verdict be
 * exercised by building a reading.
 */
final class RelayVerdict {

    static final String INSTANCE_ID = "instanceId";
    static final String STATE = "state";
    static final String COORDINATION = "coordination";
    static final String PAUSED = "paused";
    static final String WORKERS_CONFIGURED = "workersConfigured";
    static final String WORKERS_ALIVE = "workersAlive";
    static final String CYCLE_AGE_SECONDS = "cycleAgeSeconds";

    private RelayVerdict() {
    }

    /**
     * @param status       the relay's in-process reading
     * @param paused       whether the relay is deliberately paused; reported, never a reason to be down
     * @param now          the instant the reading is judged at
     * @param stalledAfter how long the least recently progressing worker may go without completing a cycle
     */
    static Health of(RelayStatus status, boolean paused, Instant now, Duration stalledAfter) {
        Health.Builder health = Health.status(statusOf(status, now, stalledAfter))
                .withDetail(INSTANCE_ID, status.instanceId())
                .withDetail(STATE, status.state().name())
                .withDetail(COORDINATION, status.coordination().name())
                .withDetail(PAUSED, paused)
                .withDetail(WORKERS_CONFIGURED, status.workersConfigured())
                .withDetail(WORKERS_ALIVE, status.workersAlive());
        // Absent, not zero, when no worker is alive: there is no cycle to give the age of.
        if (status.oldestWorkerCycle().isPresent()) {
            health.withDetail(CYCLE_AGE_SECONDS, status.oldestWorkerCycleAgeSecondsAt(now));
        }
        return health.build();
    }

    private static Status statusOf(RelayStatus status, Instant now, Duration stalledAfter) {
        return switch (status.state()) {
            // Leaving on purpose, kept apart from a failure for the same reason RelayStatus keeps
            // STOPPING apart from STOPPED.
            case STOPPING -> Status.OUT_OF_SERVICE;
            case STOPPED -> Status.DOWN;
            case RUNNING -> runningStatus(status, now, stalledAfter);
        };
    }

    private static Status runningStatus(RelayStatus status, Instant now, Duration stalledAfter) {
        // A deficit is not down: a died worker is restarted. None alive at all is.
        if (status.workersAlive() == 0) {
            return Status.DOWN;
        }
        boolean stalled = status.oldestWorkerCycle()
                .map(lastCycle -> Duration.between(lastCycle, now).compareTo(stalledAfter) > 0)
                .orElse(false);
        return stalled ? Status.DOWN : Status.UP;
    }
}
