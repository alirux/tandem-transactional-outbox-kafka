package com.codingful.tandem.jdbc;

import java.time.Duration;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Every wait the relay's poll loop can take (LLD-jdbc §3.1), and the whole decision of which one
 * applies. A worker reports the outcome of each cycle and is told how long to wait, so the loop
 * itself holds no timing policy: {@link #waitAfterCycle(int, int)} for a cycle that completed and
 * {@link #waitAfterFailedCycle()} for one that threw, exactly one call per iteration.
 *
 * <p>A completed cycle has three outcomes, deliberately kept apart because they answer different
 * questions:
 *
 * <ul>
 *   <li><b>Busy</b>: the claim returned rows. No wait at all, and the idle backoff drops back to
 *       {@code pollIntervalFloor}: the bucket has just proved it is being written to, so the next
 *       row is likely near.</li>
 *   <li><b>Draining</b>: the claim returned nothing but publishes are still in flight. A brief fixed
 *       pause, long enough for async completions to land before the loop re-flushes, and it leaves
 *       the idle backoff where it is: outstanding work is not idleness.</li>
 *   <li><b>Idle</b>: the claim returned nothing and nothing is in flight. Starts at
 *       {@code pollIntervalFloor} after the last productive claim and grows by
 *       {@code pollBackoffFactor} on each consecutive empty claim, up to {@code pollInterval}, each
 *       wait carrying ±20% jitter. So a worker under a live stream rediscovers work at the floor, and
 *       one whose bucket has genuinely gone quiet settles at the ceiling instead of querying at the
 *       floor forever.</li>
 * </ul>
 *
 * <p>A configured {@link WakeupSource} cuts across the idle case: a signal ends the wait early and
 * {@link #resetIdle()} puts the backoff back at the floor, so the ramp a worker climbed while its
 * slice was quiet does not price the first row that arrives out of that silence.
 *
 * <p>A <b>failed cycle</b> is the fourth case: an exponentially growing delay from
 * {@code pollInterval} up to a cap, reset by the first cycle that completes. A dead database
 * otherwise has every worker re-querying and logging a stack trace ten times a second. It is
 * anchored on the ceiling, not on the floor: a broken database is the one case where the relay
 * should back away rather than lean in.
 *
 * <p>The ±20% jitter is narrow on purpose: it is below the growth factor, so successive delays still
 * grow strictly monotonically, and a wait can never collapse towards zero and turn the loop into a
 * spin. It is applied around the ceiling rather than clamped to it, so the idle worst case is what
 * it always was, {@code pollInterval} + 20%. Not thread-safe: each worker thread owns its own
 * instance, which is what makes both counters per-worker rather than shared and contended.
 */
final class PollBackoff {

    /** Fraction by which a computed delay is randomly stretched or shrunk. */
    private static final double JITTER = 0.2;

    /** Failure count past which the exponential can only be the cap; the counter stops there. */
    private static final int SATURATED = 62;

    /**
     * The draining pause: short and fixed, because it is not a poll cadence. The claim found nothing
     * only because this worker's own publishes have not acked yet, and the next thing the loop does is
     * re-flush their outcomes.
     */
    private static final long DRAINING_MILLIS = 5;

    private final long idleFloorMillis;
    private final long idleCeilingMillis;
    private final double growthFactor;
    private final long errorCapMillis;

    private int consecutiveIdle;
    private int consecutiveFailures;

    /**
     * @param pollIntervalFloor where the idle backoff restarts after a productive claim; a value above
     *                          {@code pollInterval} simply never applies, since every wait is capped at
     *                          the ceiling
     * @param pollInterval      ceiling of the idle backoff, and the first delay after a failed cycle
     * @param growthFactor      multiplier applied to the idle backoff per consecutive empty claim
     * @param errorCap          ceiling for the failure backoff; raised to {@code pollInterval} when
     *                          smaller, so the failure delay is never shorter than the idle one
     */
    PollBackoff(Duration pollIntervalFloor, Duration pollInterval, double growthFactor, Duration errorCap) {
        this.idleCeilingMillis = Math.max(1, pollInterval.toMillis());
        this.idleFloorMillis = Math.max(1, pollIntervalFloor.toMillis());
        this.growthFactor = growthFactor;
        this.errorCapMillis = Math.max(this.idleCeilingMillis, errorCap.toMillis());
    }

    /**
     * How long to wait after a cycle that completed without throwing, and the only place the three
     * outcomes are told apart. Also marks the progress that resets the failure backoff.
     *
     * @param claimed  rows this cycle claimed
     * @param inFlight publishes still outstanding for this worker
     * @return milliseconds to wait, or {@code 0} to claim again immediately
     */
    long waitAfterCycle(int claimed, int inFlight) {
        consecutiveFailures = 0;
        if (claimed > 0) {
            consecutiveIdle = 0;
            return 0;
        }
        if (inFlight > 0) {
            return DRAINING_MILLIS;
        }
        long delay = idleDelay(consecutiveIdle);
        if (delay < idleCeilingMillis) {
            consecutiveIdle++;   // stops once the ceiling is reached: counting further changes nothing
        }
        return jitter(delay);
    }

    /**
     * Puts the idle backoff back at {@code pollIntervalFloor}, exactly as a claim returning rows would.
     * Called when a {@link WakeupSource} signal, rather than the backoff itself, ended a worker's wait:
     * the bucket has just been written to, so the ramp climbed while it was quiet no longer describes it.
     */
    void resetIdle() {
        consecutiveIdle = 0;
    }

    /** How long to wait after a cycle that threw; grows on each successive call until the cap. */
    long waitAfterFailedCycle() {
        long delay = jitter(boundedExponential(consecutiveFailures));
        if (consecutiveFailures < SATURATED) {
            consecutiveFailures++;   // stops there: counting further would only overflow the shift
        }
        // Jitter is applied before the clamp so the cap is a hard ceiling: an operator reading
        // "capped at reclaimInterval" must not find waits 20% past it.
        return Math.min(delay, errorCapMillis);
    }

    /** {@code min(ceiling, floor * factor^steps)}, computed in floating point so the factor need not be integral. */
    private long idleDelay(int steps) {
        double scaled = idleFloorMillis * Math.pow(growthFactor, steps);
        if (!(scaled < idleCeilingMillis)) {   // also catches an overflowed or NaN product
            return idleCeilingMillis;
        }
        return Math.max(1, Math.round(scaled));
    }

    /** {@code min(cap, pollInterval * 2^failures)}, overflow-safe: any large count clamps to the cap. */
    private long boundedExponential(int failures) {
        if (failures >= SATURATED) {
            // The shift below silently truncates from here on. 100 << 62 is 0, not a huge number,
            // which would turn a saturated backoff back into a spin.
            return errorCapMillis;
        }
        long scaled = idleCeilingMillis << failures;
        if (scaled < 0 || scaled > errorCapMillis) {
            return errorCapMillis;
        }
        return scaled;
    }

    /** {@code random[millis * (1 - JITTER), millis * (1 + JITTER)]}, never below 1 ms. */
    private static long jitter(long millis) {
        double spread = millis * JITTER;   // > 0: millis is at least 1
        double jittered = millis + ThreadLocalRandom.current().nextDouble(-spread, spread);
        return Math.max(1, Math.round(jittered));
    }
}
