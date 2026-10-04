package com.codingful.tandem.relay;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import java.time.Duration;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.bind.DefaultValue;

/**
 * The one knob of the relay's health verdict (LLD-relay §5.2). Under Actuator's own
 * {@code management.health.<name>} convention rather than {@code tandem.*}, because it configures a
 * health contributor and not the relay.
 *
 * @param stalledAfter how long the least recently progressing worker may go without completing a
 *                     cycle before the relay is reported as not delivering; 60 seconds by default
 */
@ConfigurationProperties(RelayHealthProperties.PREFIX)
record RelayHealthProperties(@DefaultValue("60s") Duration stalledAfter) {

    static final String PREFIX = "management.health.tandem-relay";
    static final String STALLED_AFTER_KEY = PREFIX + ".stalled-after";
    static final String POLL_INTERVAL_KEY = "tandem.relay.poll-interval";

    /**
     * How many idle waits the threshold must outlast: two consecutive ones, each at its longest, since
     * the relay adds up to 20% of jitter to {@code poll-interval}.
     */
    static final double IDLE_WAITS = 2.4;

    /**
     * An idle worker completes a cycle once per {@code poll-interval} at the slowest, so a threshold
     * at or below that reports a healthy relay with nothing to deliver as down. A long poll interval is
     * a legitimate setting, the recommended one with a wakeup, hence a check instead of a fixed number.
     *
     * @throws TandemConfigurationException if the threshold leaves no margin over the idle wait
     */
    void requireMarginOver(Duration pollInterval) {
        if (stalledAfter.toMillis() <= pollInterval.toMillis() * IDLE_WAITS) {
            throw new TandemConfigurationException(STALLED_AFTER_KEY + " (" + stalledAfter + ") must exceed "
                    + IDLE_WAITS + " times " + POLL_INTERVAL_KEY + " (" + pollInterval + "): an idle relay"
                    + " completes a cycle only once per poll interval, and would be reported as stalled");
        }
    }
}
