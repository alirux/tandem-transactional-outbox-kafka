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
    private IOException publishFailure;
    private IOException closeFailure;
    private boolean closed;

    void failNextPublishWith(IOException failure) {
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

    void shutdown(Throwable cause) {
        shutdownHandler.handle(cause);
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
            IOException failure = publishFailure;
            publishFailure = null;
            throw failure;   // the tag stays unconsumed, exactly as the real client leaves it
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
