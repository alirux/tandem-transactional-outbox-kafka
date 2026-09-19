package com.codingful.tandem.rabbitmq;

import com.rabbitmq.client.AMQP;
import java.io.IOException;

/**
 * The narrow seam {@link RabbitRelay} publishes through: the four things it needs from an AMQP
 * channel, and nothing else.
 *
 * <p>It exists so the relay's confirm, return and shutdown handling can be driven by a real in-memory
 * double in the unit tests rather than by a broker or a mock framework (AGENTS Testing §2). The
 * callbacks are declared here, in Tandem's own terms, for the same reason: a double implementing this
 * interface is a dozen lines, one implementing {@link com.rabbitmq.client.Channel} is not.
 */
interface PublishChannel extends AutoCloseable {

    /** The delivery tag the next {@link #publish} will use. Only meaningful under the caller's lock. */
    long nextPublishSeqNo();

    /** Publishes with the {@code mandatory} flag set, so an unroutable message is returned, not dropped (§1). */
    void publish(AmqpRoute route, AMQP.BasicProperties properties, byte[] body) throws IOException;

    void onConfirm(ConfirmHandler handler);

    void onReturn(ReturnHandler handler);

    void onShutdown(ShutdownHandler handler);

    @Override
    void close() throws IOException;

    /** An ack or a nack for one delivery tag, or for every tag up to it when {@code multiple}. */
    @FunctionalInterface
    interface ConfirmHandler {
        void handle(long deliveryTag, boolean multiple, boolean acknowledged);
    }

    /**
     * An unroutable message coming back. AMQP's return carries no delivery tag, so it is correlated by
     * the {@code messageId} the relay set to the outbox row id (§2).
     */
    @FunctionalInterface
    interface ReturnHandler {
        void handle(String messageId, int replyCode, String replyText);
    }

    /** The channel or its connection went away; every pending confirm is lost with it (§1). */
    @FunctionalInterface
    interface ShutdownHandler {
        void handle(Throwable cause);
    }
}
