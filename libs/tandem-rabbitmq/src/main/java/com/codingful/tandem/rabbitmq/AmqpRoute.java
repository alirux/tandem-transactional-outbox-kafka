package com.codingful.tandem.rabbitmq;

import java.util.Objects;

/**
 * Where one message goes on an AMQP broker (LLD-rabbitmq §6): the exchange that routes it, and the
 * routing key bindings are matched against.
 *
 * <p>Two values, which is why {@link com.codingful.tandem.core.port.TopicRouter} is not reused here:
 * a Kafka topic names the stream directly, while on AMQP the exchange is the routing fabric and the
 * routing key is what a consumer's binding selects on.
 *
 * @param exchange   the exchange to publish to; the empty string is AMQP's default exchange, where the
 *                   routing key is read as a queue name
 * @param routingKey the routing key bindings are matched against
 */
public record AmqpRoute(String exchange, String routingKey) {

    /**
     * @throws NullPointerException if either argument is {@code null}
     */
    public AmqpRoute {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(routingKey, "routingKey");
    }
}
