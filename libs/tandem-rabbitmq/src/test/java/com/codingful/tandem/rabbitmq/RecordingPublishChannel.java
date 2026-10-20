package com.codingful.tandem.rabbitmq;

import com.rabbitmq.client.AMQP;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * An in-memory {@link PublishChannel} that records what was published and lets a test drive the
 * broker's half of the protocol explicitly: confirm, nack, return, shutdown.
 *
 * <p>A real test double, not a mock: it behaves like a channel, including the detail that matters for
 * the tag bookkeeping, namely that a failed publish does not consume a delivery tag.
 */
final class RecordingPublishChannel implements PublishChannel {

    record Published(long deliveryTag, AmqpRoute route, AMQP.BasicProperties properties, byte[] body) {
    }

    private final List<Published> published = new ArrayList<>();

    private ConfirmHandler confirmHandler;
    private ReturnHandler returnHandler;
    private ShutdownHandler shutdownHandler;

    private long nextTag = 1;
    private Exception publishFailure;
    private IOException closeFailure;
    private boolean closed;

    void failNextPublishWith(IOException failure) {
        this.publishFailure = failure;
    }

    /** The real client throws an unchecked {@code AlreadyClosedException} on a channel already closed. */
    void failNextPublishWith(RuntimeException failure) {
        this.publishFailure = failure;
    }

    void failCloseWith(IOException failure) {
        this.closeFailure = failure;
    }

    List<Published> published() {
        return List.copyOf(published);
    }

    Published last() {
        return published.get(published.size() - 1);
    }

    void confirm(long deliveryTag) {
        confirmHandler.handle(deliveryTag, false, true);
    }

    void confirmUpTo(long deliveryTag) {
        confirmHandler.handle(deliveryTag, true, true);
    }

    void nack(long deliveryTag) {
        confirmHandler.handle(deliveryTag, false, false);
    }

    void returnUnroutable(String messageId, int replyCode, String replyText) {
        returnHandler.handle(messageId, replyCode, replyText);
    }

    /** The channel goes away with every tag it issued. */
    void shutdown(Throwable cause) {
        shutdownHandler.handle(cause, nextTag - 1);
    }

    /** A channel that was replaced goes away late: only tags up to {@code lastTag} were its own. */
    void shutdown(Throwable cause, long lastTag) {
        shutdownHandler.handle(cause, lastTag);
    }

    boolean isClosed() {
        return closed;
    }

    @Override
    public long nextPublishSeqNo() {
        return nextTag;
    }

    @Override
    public void publish(AmqpRoute route, AMQP.BasicProperties properties, byte[] body) throws IOException {
        if (publishFailure != null) {
            Exception failure = publishFailure;
            publishFailure = null;
            // the tag stays unconsumed, exactly as the real client leaves it
            if (failure instanceof IOException io) {
                throw io;
            }
            throw (RuntimeException) failure;
        }
        published.add(new Published(nextTag++, route, properties, body));
    }

    @Override
    public void onConfirm(ConfirmHandler handler) {
        this.confirmHandler = handler;
    }

    @Override
    public void onReturn(ReturnHandler handler) {
        this.returnHandler = handler;
    }

    @Override
    public void onShutdown(ShutdownHandler handler) {
        this.shutdownHandler = handler;
    }

    @Override
    public void close() throws IOException {
        closed = true;
        if (closeFailure != null) {
            throw closeFailure;
        }
    }
}
