package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.MessageEncoder;
import com.rabbitmq.client.AMQP;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The message-format port as AMQP sees it (LLD-rabbitmq §5): a {@link MessageEncoder} that writes an
 * {@link AmqpMessage} directly. {@link RabbitRelay} takes one, so the published format is a choice a
 * consumer of this module makes rather than something the dispatcher decides.
 *
 * <p>It exists alongside the transport-neutral {@link MessageEncoder} for the same reason its Kafka
 * counterpart does. A format with an AMQP-specific binding (CloudEvents, whose attributes become
 * {@code cloudEvents_*} headers here and {@code ce_*} on Kafka) is built against this interface, and
 * {@link CloudEventAmqpEncoder} does exactly that. A format with nothing AMQP-specific about it
 * implements {@link MessageEncoder} once and reaches this adapter through {@link #from}, and any
 * other transport adapter unchanged.
 */
public interface RabbitMessageEncoder {

    /**
     * @param record the row about to be published
     * @return the message to publish
     * @throws RuntimeException if the row cannot be encoded; {@link RabbitRelay} fails it permanently (§4)
     */
    AmqpMessage encode(OutboxRecord record);

    /**
     * Lifts a transport-neutral encoder onto AMQP.
     *
     * <p>The neutral {@link EncodedMessage#destination()} becomes the <b>routing key</b>, and the
     * exchange comes from this adapter's configuration. A neutral encoder names the stream its
     * consumers bind to, and on AMQP that is the routing key; the exchange is the routing fabric an
     * operator configures once. Reading it the other way would leave the routing key undefined.
     *
     * <p>Header values are narrowed from bytes to UTF-8 strings, which is the direction
     * {@link EncodedMessage} documents as available to a typed property model.
     *
     * @param encoder  the neutral encoder to adapt
     * @param exchange the exchange every lifted message is published to
     * @return an AMQP encoder delegating to it
     * @throws NullPointerException if either argument is {@code null}
     */
    static RabbitMessageEncoder from(MessageEncoder encoder, String exchange) {
        Objects.requireNonNull(encoder, "encoder");
        Objects.requireNonNull(exchange, "exchange");
        return record -> {
            EncodedMessage message = encoder.encode(record);
            Map<String, Object> headers = new LinkedHashMap<>();
            message.headers().forEach((name, value) -> headers.put(name, new String(value, StandardCharsets.UTF_8)));
            AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                    .headers(headers)
                    .build();
            return new AmqpMessage(new AmqpRoute(exchange, message.destination()), properties, message.body());
        };
    }
}
