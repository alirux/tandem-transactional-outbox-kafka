package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AmqpRouteTest {

    private static final String EXCHANGE = "tandem-events";

    @Test
    void GIVEN_a_routing_key_of_exactly_255_bytes_WHEN_the_route_is_built_THEN_it_is_accepted() {
        String routingKey = "k".repeat(255);

        assertThat(new AmqpRoute(EXCHANGE, routingKey).routingKey()).isEqualTo(routingKey);
    }

    @Test
    void GIVEN_a_routing_key_of_256_bytes_WHEN_the_route_is_built_THEN_it_is_refused() {
        assertThatThrownBy(() -> new AmqpRoute(EXCHANGE, "k".repeat(256)))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("routingKey");
    }

    @Test
    void GIVEN_a_routing_key_short_in_characters_but_long_in_UTF_8_bytes_WHEN_the_route_is_built_THEN_it_is_refused() {
        // 128 characters, two bytes each: the limit is AMQP's, counted on the wire.
        assertThatThrownBy(() -> new AmqpRoute(EXCHANGE, "é".repeat(128)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_an_exchange_longer_than_AMQP_allows_WHEN_the_route_is_built_THEN_it_is_refused() {
        assertThatThrownBy(() -> new AmqpRoute("x".repeat(256), "order-topic"))
                .isInstanceOf(IllegalArgumentException.class)
                .hasMessageContaining("exchange");
    }
}
