package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.exception.OutboxDispatchException;

/**
 * Classifies an AMQP publish failure into an {@link OutboxDispatchException} carrying the
 * retriable/permanent verdict (LLD-rabbitmq §4), so {@code tandem-jdbc} routes it to
 * {@code markForRetry} or {@code markFailed} without knowing any AMQP type. Pluggable SPI; the default
 * is {@link DefaultRabbitErrorClassifier}.
 */
@FunctionalInterface
public interface RabbitErrorClassifier {

    /**
     * @param cause the failure the broker or the client reported
     * @return the verdict to hand back to {@code tandem-jdbc}
     */
    OutboxDispatchException classify(Throwable cause);
}
