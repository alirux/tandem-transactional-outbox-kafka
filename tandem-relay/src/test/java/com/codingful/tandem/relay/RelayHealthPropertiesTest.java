package com.codingful.tandem.relay;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.jdbc.RelayConfig;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The stall threshold against the relay's own idle wait (LLD-relay §5.2): a threshold an idle worker
 * cannot keep up with would report a healthy relay as down, so it is refused at startup.
 */
class RelayHealthPropertiesTest {

    private static final Duration POLL_INTERVAL = Duration.ofSeconds(10);
    private static final Duration TWO_IDLE_WAITS =
            Duration.ofMillis((long) (POLL_INTERVAL.toMillis() * RelayHealthProperties.IDLE_WAITS));

    @Test
    void GIVEN_a_threshold_an_idle_relay_cannot_keep_up_with_WHEN_it_is_checked_THEN_it_is_refused_naming_both_settings() {
        RelayHealthProperties tooTight = new RelayHealthProperties(POLL_INTERVAL);

        assertThatThrownBy(() -> tooTight.requireMarginOver(POLL_INTERVAL))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining(RelayHealthProperties.STALLED_AFTER_KEY)
                .hasMessageContaining(RelayHealthProperties.POLL_INTERVAL_KEY);
    }

    @Test
    void GIVEN_a_threshold_of_exactly_two_idle_waits_WHEN_it_is_checked_THEN_it_is_refused() {
        RelayHealthProperties noMargin = new RelayHealthProperties(TWO_IDLE_WAITS);

        assertThatThrownBy(() -> noMargin.requireMarginOver(POLL_INTERVAL))
                .isInstanceOf(TandemConfigurationException.class);
    }

    @Test
    void GIVEN_a_threshold_just_past_two_idle_waits_WHEN_it_is_checked_THEN_it_is_accepted() {
        RelayHealthProperties withMargin = new RelayHealthProperties(TWO_IDLE_WAITS.plusMillis(1));

        assertThatCode(() -> withMargin.requireMarginOver(POLL_INTERVAL)).doesNotThrowAnyException();
    }

    @Test
    void GIVEN_the_relays_default_poll_interval_WHEN_the_default_threshold_is_checked_THEN_it_is_accepted() {
        RelayHealthProperties defaults = new RelayHealthProperties(Duration.ofSeconds(60));

        assertThatCode(() -> defaults.requireMarginOver(RelayConfig.defaults().pollInterval()))
                .doesNotThrowAnyException();
    }
}
