package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.rabbitmq.client.ConnectionFactory;
import java.net.URI;
import java.time.Duration;
import org.junit.jupiter.api.Test;

class RabbitRelayConfigTest {

    @Test
    void GIVEN_a_deadline_that_can_never_elapse_WHEN_the_adapter_is_configured_THEN_it_is_rejected_at_once() {
        // A zero or negative confirm timeout would mean no deadline at all, which is the leak the
        // deadline exists to prevent (LLD-rabbitmq §3): it must fail at configuration, not at runtime.
        assertThatThrownBy(() -> new RabbitRelayConfig(URI.create("/tandem/orders"), "application/json", null,
                "tandem-events", "-topic", Duration.ZERO))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("confirmTimeout");

        assertThatThrownBy(() -> new RabbitRelayConfig(URI.create("/tandem/orders"), "application/json", null,
                "tandem-events", "-topic", Duration.ofSeconds(-1)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_broker_that_is_not_there_WHEN_the_relay_starts_THEN_it_says_where_it_tried_to_connect() {
        ConnectionFactory unreachable = new ConnectionFactory();
        unreachable.setHost("localhost");
        unreachable.setPort(1);            // reserved, nothing listens there
        unreachable.setConnectionTimeout(500);

        assertThatThrownBy(() -> new RabbitRelay(unreachable, RabbitRelayConfig.of("/tandem/orders", "events")))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("localhost:1");
    }

    @Test
    void GIVEN_the_shorthand_factory_WHEN_it_builds_a_config_THEN_the_defaults_are_the_documented_ones() {
        RabbitRelayConfig cfg = RabbitRelayConfig.of("/tandem/orders", "tandem-events");

        assertThat(cfg.confirmTimeout()).isEqualTo(RabbitRelayConfig.DEFAULT_CONFIRM_TIMEOUT);
        assertThat(cfg.defaultDataSchema()).isNull();
    }
}
