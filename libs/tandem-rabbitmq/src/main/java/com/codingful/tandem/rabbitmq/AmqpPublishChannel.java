package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.io.IOException;

/** {@link PublishChannel} over a real AMQP connection: confirms enabled, mandatory publishing (§1). */
final class AmqpPublishChannel implements PublishChannel {

    private static final boolean MANDATORY = true;

    private final Connection connection;
    private final Channel channel;

    private AmqpPublishChannel(Connection connection, Channel channel) {
        this.connection = connection;
        this.channel = channel;
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
        return channel.getNextPublishSeqNo();
    }

    @Override
    public void publish(AmqpRoute route, AMQP.BasicProperties properties, byte[] body) throws IOException {
        channel.basicPublish(route.exchange(), route.routingKey(), MANDATORY, properties, body);
    }

    @Override
    public void onConfirm(ConfirmHandler handler) {
        channel.addConfirmListener(
                (deliveryTag, multiple) -> handler.handle(deliveryTag, multiple, true),
                (deliveryTag, multiple) -> handler.handle(deliveryTag, multiple, false));
    }

    @Override
    public void onReturn(ReturnHandler handler) {
        channel.addReturnListener(returned -> handler.handle(returned.getProperties().getMessageId(),
                returned.getReplyCode(), returned.getReplyText()));
    }

    @Override
    public void onShutdown(ShutdownHandler handler) {
        channel.addShutdownListener(handler::handle);
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
