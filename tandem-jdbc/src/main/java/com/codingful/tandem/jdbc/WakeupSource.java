package com.codingful.tandem.jdbc;

import javax.sql.DataSource;

/**
 * The relay's subscription to post-commit wakeups (dispatch-latency §6): where a running
 * {@link WorkerPool} learns that a bucket has just been written to, instead of waiting for its next
 * poll. Deliberately a relay-side seam of its own, like {@link BucketSource} (ownership) and
 * {@link RelayControlSource} (pause): a wakeup says nothing about who owns a bucket or whether it is
 * paused, and it must stay optional in a way neither of those is.
 *
 * <p><b>Never on the correctness path.</b> A signal carries a bucket number and no work, so the
 * database remains the only source of truth for what is pending; and the worker's wait is a
 * <i>bounded</i> one, so {@link RelayConfig#pollInterval()} still bounds discovery latency in every
 * configuration. A source that is absent, broken, or silently disabled by a connection pooler
 * therefore leaves the relay behaving exactly as it does with {@link #NONE}, only slower to notice a
 * row.
 */
public interface WakeupSource {

    /** The default: nothing is listened for, and every worker discovers work by polling. */
    WakeupSource NONE = new WakeupSource() {
    };

    /**
     * What a source signals into — implemented by the {@link WorkerPool}, which owns the mapping from
     * bucket to worker thread. Both methods are called from the source's own thread, may be called
     * before or during a worker's wait, and must never block.
     */
    interface Listener {

        /**
         * Signals that {@code bucket} may have pending rows. A signal for a bucket this instance does
         * not own, or that is paused, is not an error: the woken worker claims nothing and goes back
         * to waiting.
         *
         * @param bucket the bucket just written to
         */
        void wake(int bucket);

        /**
         * Signals that <b>some</b> bucket may have pending rows, without saying which. Used where a
         * source cannot know what it missed — after (re)subscribing, since nothing emitted while it
         * was disconnected was delivered.
         */
        void wakeAll();
    }

    /**
     * Begin delivering signals to {@code listener}. Idempotent, and expected to return promptly:
     * a source that needs a connection or a thread establishes it in the background, so a database
     * that is unreachable at relay startup delays wakeups rather than the relay. No-op by default.
     *
     * @param listener where signals are delivered, for as long as the source runs
     */
    default void start(Listener listener) {
    }

    /** Stop delivering signals and release whatever the source holds. Idempotent. No-op by default. */
    default void stop() {
    }

    /**
     * Select the source for a {@link Wakeup} mode — the relay-side counterpart of the mode the
     * write-side emits with, and the assembly step {@code tandem-spring-relay} performs from
     * {@code tandem.outbox.wakeup}.
     *
     * @param wakeup     the configured mode; {@link Wakeup#NONE} yields {@link #NONE}
     * @param dataSource the relay's connection source, used by {@link Wakeup#PG_NOTIFY} to hold one
     *                   dedicated listening connection for the relay's lifetime
     * @return the matching source, never {@code null}
     * @throws NullPointerException if {@code wakeup} is {@code null}, or if the selected mode needs a
     *                              {@code dataSource} and none was given
     */
    static WakeupSource forWakeup(Wakeup wakeup, DataSource dataSource) {
        return switch (wakeup) {
            case NONE -> NONE;
            case PG_NOTIFY -> new PgNotifyWakeup(dataSource);
        };
    }
}
