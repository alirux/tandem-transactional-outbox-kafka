package com.codingful.tandem.rabbitmq;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Where one message goes on an AMQP broker (LLD-rabbitmq §6): the exchange that routes it, and the
 * routing key bindings are matched against.
 *
 * <p>Two values, which is why {@link com.codingful.tandem.core.port.TopicRouter} is not reused here:
 * a Kafka topic names the stream directly, while on AMQP the exchange is the routing fabric and the
 * routing key is what a consumer's binding selects on.
 *
 * <p>AMQP carries both values as short strings, so each is at most {@value #MAX_BYTES} bytes in UTF-8. A
 * longer one is refused here rather than by the client at publish time: built inside an encoder, the
 * refusal is an encoding failure, which fails the row permanently instead of retrying a publish that can
 * never succeed (LLD-rabbitmq §4).
 *
 * @param exchange   the exchange to publish to; the empty string is AMQP's default exchange, where the
 *                   routing key is read as a queue name
 * @param routingKey the routing key bindings are matched against
 */
public record AmqpRoute(String exchange, String routingKey) {

    private static final int MAX_BYTES = 255;

    /**
     * @throws NullPointerException     if either argument is {@code null}
     * @throws IllegalArgumentException if either argument is longer than 255 bytes in UTF-8
     */
    public AmqpRoute {
        requireShortString(Objects.requireNonNull(exchange, "exchange"), "exchange");
        requireShortString(Objects.requireNonNull(routingKey, "routingKey"), "routingKey");
    }

    private static void requireShortString(String value, String name) {
        int bytes = value.getBytes(StandardCharsets.UTF_8).length;
        if (bytes > MAX_BYTES) {
            throw new IllegalArgumentException(
                    name + " is " + bytes + " bytes in UTF-8, longer than the " + MAX_BYTES + " AMQP allows");
        }
    }
}
