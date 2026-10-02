package com.codingful.tandem.relay;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.RelayControlSource;
import com.codingful.tandem.jdbc.WorkerPool;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.health.contributor.Status;

/**
 * What the indicator does around the verdict (LLD-relay §5.1, §5.2): it exists in every role, and it is
 * where the stall threshold meets the relay's configuration. A real bean factory stands in for the
 * application context, empty or holding the one bean a scenario needs.
 */
class RelayHealthIndicatorTest {

    private static final RelayHealthProperties DEFAULT_THRESHOLD = new RelayHealthProperties(Duration.ofSeconds(60));

    private static RelayHealthIndicator indicatorIn(DefaultListableBeanFactory context) {
        return new RelayHealthIndicator(context.getBeanProvider(WorkerPool.class),
                context.getBeanProvider(RelayControlSource.class), context.getBeanProvider(RelayConfig.class),
                DEFAULT_THRESHOLD);
    }

    @Test
    void GIVEN_a_process_that_runs_no_relay_WHEN_its_health_is_read_THEN_the_relay_is_unknown_rather_than_down() {
        RelayHealthIndicator indicator = indicatorIn(new DefaultListableBeanFactory());

        assertThat(indicator.health().getStatus()).isEqualTo(Status.UNKNOWN);
    }

    @Test
    void GIVEN_a_relay_polling_more_slowly_than_the_threshold_allows_WHEN_the_application_starts_THEN_it_is_refused() {
        DefaultListableBeanFactory context = new DefaultListableBeanFactory();
        context.registerSingleton("relayConfig",
                RelayConfig.builder().pollInterval(DEFAULT_THRESHOLD.stalledAfter()).build());

        assertThatThrownBy(() -> indicatorIn(context)).isInstanceOf(TandemConfigurationException.class);
    }
}
