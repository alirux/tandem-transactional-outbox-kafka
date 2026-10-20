package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** {@link PublishChannel} over a real AMQP connection: confirms enabled, mandatory publishing (§1). */
final class AmqpPublishChannel implements PublishChannel {

    private static final Logger LOG = LoggerFactory.getLogger(AmqpPublishChannel.class);

    private static final boolean MANDATORY = true;

    private final Connection connection;

    /** Replaced only under the relay's publish lock, which every publish holds (§2). */
    private volatile Channel channel;

    /** The delivery tags issued by the channels the current one replaced. */
    private long tagOffset;

    private volatile ConfirmHandler confirmHandler;
    private volatile ReturnHandler returnHandler;
    private volatile ShutdownHandler shutdownHandler;

    private AmqpPublishChannel(Connection connection, Channel channel) {
        this.connection = connection;
        this.channel = channel;
        attach(channel, 0);
    }

    /**
     * Opens the connection and the publish channel, verifies the exchange exists, and switches the
     * channel into confirm mode.
     *
     * @throws TandemConfigurationException if the broker is unreachable or the exchange does not exist
     */
    static AmqpPublishChannel open(ConnectionFactory factory, String exchange) {
        // The client reconnects on its own; pending confirms do not survive it, which onShutdown
        // handles by failing them retriably (§1).
        factory.setAutomaticRecoveryEnabled(true);
        Connection connection = null;
        try {
            connection = factory.newConnection();
            verifyExchange(connection, exchange);
            Channel channel = connection.createChannel();
            channel.confirmSelect();
            return new AmqpPublishChannel(connection, channel);
        } catch (TandemConfigurationException alreadyDiagnosed) {
            closeQuietly(connection);
            throw alreadyDiagnosed;
        } catch (Exception failure) {
            closeQuietly(connection);
            throw new TandemConfigurationException("Cannot open the AMQP publish channel to "
                    + factory.getHost() + ":" + factory.getPort() + " - " + failure.getMessage(), failure);
        }
    }

    /**
     * A passive declare, never an active one: which exchanges and queues exist is the operator's
     * design, and Tandem does not own the consumer's side of the system (§1). Run on a throwaway
     * channel because a failed passive declare closes the channel it runs on.
     */
    private static void verifyExchange(Connection connection, String exchange) throws IOException {
        if (exchange.isEmpty()) {
            return;   // AMQP's default exchange always exists and cannot be declared, passively or not
        }
        Channel probe = connection.createChannel();
        try {
            probe.exchangeDeclarePassive(exchange);
        } catch (IOException missing) {
            throw new TandemConfigurationException("The AMQP exchange `" + exchange + "` does not exist."
                    + " Tandem never declares topology: create the exchange (and the bindings its"
                    + " consumers need) before starting the relay (LLD-rabbitmq §1).", missing);
        } finally {
            closeQuietly(probe);
        }
    }

    @Override
    public long nextPublishSeqNo() {
        reopenIfClosedByBroker();
        return tagOffset + channel.getNextPublishSeqNo();
    }

    /**
     * The broker closes a channel over a channel-level error, such as a publish to an exchange that no
     * longer exists, and connection recovery does not reopen it: every later publish would fail on the
     * closed channel until the relay restarts. Tags keep counting across the replacement, so a late
     * confirm or shutdown of the old channel never settles a publish of the new one.
     */
    private void reopenIfClosedByBroker() {
        Channel closed = channel;
        if (closed.isOpen() || !connection.isOpen()) {
            return;   // open, or the whole connection is down and its recovery reopens the channel
        }
        Channel replacement = null;
        try {
            replacement = connection.createChannel();
            replacement.confirmSelect();
            long issued = tagOffset + closed.getNextPublishSeqNo() - 1;
            attach(replacement, issued);
            channel = replacement;
            tagOffset = issued;
        } catch (IOException | RuntimeException reopenFailure) {
            // The closed channel stays in place: the publish fails on it, retriably whatever reason the
            // broker closed it with (LLD-rabbitmq §4), and the next publish tries again.
            closeQuietly(replacement);
            LOG.warn("Reopening the AMQP publish channel failed host:{}, port:{}", connection.getAddress(),
                    connection.getPort(), reopenFailure);
        }
    }

    /** Relays the channel's callbacks to the registered handlers, its delivery tags shifted by {@code offset}. */
    private void attach(Channel target, long offset) {
        target.addConfirmListener(
                (deliveryTag, multiple) -> confirmHandler.handle(offset + deliveryTag, multiple, true),
                (deliveryTag, multiple) -> confirmHandler.handle(offset + deliveryTag, multiple, false));
        target.addReturnListener(returned -> returnHandler.handle(returned.getProperties().getMessageId(),
                returned.getReplyCode(), returned.getReplyText()));
        target.addShutdownListener(cause -> {
            ShutdownHandler handler = shutdownHandler;
            if (handler != null) {
                handler.handle(cause, offset + target.getNextPublishSeqNo() - 1);
            }
        });
    }

    @Override
    public void publish(AmqpRoute route, AMQP.BasicProperties properties, byte[] body) throws IOException {
        channel.basicPublish(route.exchange(), route.routingKey(), MANDATORY, properties, body);
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
        try {
            closeQuietly(channel);
        } finally {
            closeQuietly(connection);
        }
    }

    private static void closeQuietly(AutoCloseable closeable) {
        if (closeable == null) {
            return;
        }
        try {
            closeable.close();
        } catch (Exception ignored) {
            // Closing is best-effort: the broker may already have closed this channel or connection,
            // and a failure here would mask whatever is actually being handled.
        }
    }
}
