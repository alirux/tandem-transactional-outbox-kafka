package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.exception.OutboxDispatchException;

/**
 * Default AMQP error mapping (LLD-rabbitmq §4): every failure it is handed is <b>retriable</b>.
 *
 * <p>It judges a publish the client itself refused, and none of those says anything permanent about the
 * message. A publish is asynchronous, so a broker refusal of this message (a missing exchange, a refused
 * permission) arrives later as a channel close, which {@code RabbitRelay} retries whatever the reply code,
 * and never hands to a classifier: the close cannot say which message caused it. What remains is an I/O
 * failure or an error nobody anticipated, and failing a row permanently is the unrecoverable direction, so
 * the default leans the other way, exactly as the Kafka classifier does. An application with a failure it
 * knows to be permanent plugs its own {@link RabbitErrorClassifier}.
 */
public final class DefaultRabbitErrorClassifier implements RabbitErrorClassifier {

    @Override
    public OutboxDispatchException classify(Throwable cause) {
        String message = "AMQP publish failed (retriable): " + cause.getClass().getSimpleName()
                + (cause.getMessage() == null ? "" : " - " + cause.getMessage());
        return new OutboxDispatchException(message, true, cause);
    }
}
