package com.codingful.tandem.rabbitmq;

import com.rabbitmq.client.AMQP;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * One wire-ready AMQP message, as a {@link RabbitMessageEncoder} produced it: where it goes, its
 * properties and its body.
 *
 * <p><b>This value hands over ownership and copies nothing</b>, like
 * {@link com.codingful.tandem.core.EncodedMessage}: it lives for exactly one dispatch on the relay's
 * hot path, so an encoder must not retain or mutate what it passes here.
 *
 * <p>Two properties are <b>reserved by the adapter</b> and overwritten before publishing, so an
 * encoder need not set them and cannot keep them: {@code messageId}, which correlates an unroutable
 * return back to its outbox row, and {@code deliveryMode}, which is the persistence the no-loss
 * guarantee rests on (LLD-rabbitmq §1, §2).
 *
 * @param route      the exchange and routing key
 * @param properties the AMQP properties, carrying the headers this message puts on the wire
 * @param body       the message body; not copied, see above
 */
public record AmqpMessage(AmqpRoute route, AMQP.BasicProperties properties, byte[] body) {

    /**
     * @throws NullPointerException if any argument is {@code null}
     */
    public AmqpMessage {
        Objects.requireNonNull(route, "route");
        Objects.requireNonNull(properties, "properties");
        Objects.requireNonNull(body, "body");
    }

    /** Header names only: the values are the application's business data and never reach a log (AGENTS Logging §5). */
    private Set<String> headerNames() {
        Map<String, Object> headers = properties.getHeaders();
        return headers == null ? Set.of() : headers.keySet();
    }

    @Override
    public String toString() {
        return "AmqpMessage{exchange=" + route.exchange()
                + ", routingKey=" + route.routingKey()
                + ", bodyBytes=" + body.length
                + ", headerNames=" + headerNames() + '}';
    }
}
