package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.rabbitmq.RabbitRelay;
import com.codingful.tandem.rabbitmq.RabbitRelayConfig;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.DeliverCallback;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * {@link BrokerHarness} over a real RabbitMQ broker (LLD-benchmark §3.1), for correctness runs rather
 * than for numbers.
 *
 * <p><b>The topology is one durable queue bound to everything, read by one consumer.</b> That is what
 * makes per-aggregate ordering observable at all: the relay already serialises an aggregate's rows,
 * so a single ordered queue read by a single consumer preserves what it published, while competing
 * consumers on one queue would interleave an aggregate's own events and report violations that the
 * relay never committed. It also caps delivered throughput at what one consumer thread drains, which
 * is why a run here gates correctness and says nothing about the broker's capacity.
 *
 * <p>Tandem never declares topology (LLD-rabbitmq §1), and publishes are mandatory, so the exchange,
 * the queue and the binding must all exist before the first row is dispatched or the broker returns
 * it and the relay fails the row permanently. {@link #start()} is where that happens.
 */
final class RabbitBrokerHarness implements BrokerHarness {

    static final String EXCHANGE = "tandem-benchmark";
    static final String QUEUE = "tandem-benchmark-events";

    /** Binds every routing key the default router can produce. */
    private static final String CATCH_ALL = "#";

    /**
     * Short enough that a frozen broker is noticed inside a scenario's patience rather than after the
     * client's default minute (S12). A heartbeat is a deployment setting, not a test affordance: this
     * is the value an operator who wants a partition detected promptly would pick.
     */
    private static final int HEARTBEAT_SECONDS = 5;

    private final BenchmarkConfig config;
    private final RabbitMQContainer rabbit =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.1-alpine"));
    private final List<RabbitRelay> producers = new ArrayList<>();

    RabbitBrokerHarness(BenchmarkConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        rabbit.start();
        declareTopology();
    }

    @Override
    public OutboxDispatcher newDispatcher(TandemSpanRecorder spanRecorder) {
        RabbitRelay relay = new RabbitRelay(connectionFactory(), relayConfig(), spanRecorder);
        producers.add(relay);
        return relay;
    }

    @Override
    public long deliveryTimeoutMillis() {
        return config.deliveryTimeoutMs();
    }

    @Override
    public EventReceiver newReceiver(String group) {
        return new RabbitEventReceiver(connectionFactory(), group);
    }

    /** Empties the queue, so a scenario that ends with a backlog does not deliver it to the next one. */
    @Override
    public void resetBetweenScenarios() {
        withAdminChannel(channel -> channel.queuePurge(QUEUE));
    }

    @Override
    public OptionalLong inFlightPublishes() {
        long total = 0;
        for (RabbitRelay producer : producers) {
            total += producer.inFlightConfirms();
        }
        return OptionalLong.of(total);
    }

    @Override
    public void interruptBroker() {
        DockerPause.pause(rabbit.getContainerId());
    }

    @Override
    public void restoreBroker() {
        DockerPause.unpause(rabbit.getContainerId());
    }

    @Override
    public void close() {
        for (RabbitRelay producer : producers) {
            producer.close();
        }
        rabbit.stop();
    }

    /**
     * The confirm timeout is the run's {@code deliveryTimeoutMs}, the same knob Kafka's
     * {@code delivery.timeout.ms} takes, so both brokers enforce the deadline the row lease was sized
     * against.
     */
    private RabbitRelayConfig relayConfig() {
        return new RabbitRelayConfig(URI.create("/tandem/benchmark"), "application/json", null,
                EXCHANGE, "-topic", Duration.ofMillis(config.deliveryTimeoutMs()));
    }

    private ConnectionFactory connectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(rabbit.getHost());
        factory.setPort(rabbit.getAmqpPort());
        factory.setUsername(rabbit.getAdminUsername());
        factory.setPassword(rabbit.getAdminPassword());
        factory.setRequestedHeartbeat(HEARTBEAT_SECONDS);
        return factory;
    }

    private void declareTopology() {
        withAdminChannel(channel -> {
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
            // Durable and explicitly not auto-delete: the queue outlives every consumer the run opens
            // and closes, so a scenario's messages are never dropped in the gap between two of them.
            channel.queueDeclare(QUEUE, true, false, false, Map.of());
            channel.queueBind(QUEUE, EXCHANGE, CATCH_ALL);
        });
    }

    private void withAdminChannel(ChannelWork work) {
        try (Connection connection = connectionFactory().newConnection();
             Channel channel = connection.createChannel()) {
            work.run(channel);
        } catch (Exception e) {
            throw new IllegalStateException("AMQP topology administration failed", e);
        }
    }

    @FunctionalInterface
    private interface ChannelWork {
        void run(Channel channel) throws Exception;
    }

    /**
     * Turns the client's pushed deliveries into the poll the harness reads. One connection and one
     * channel, because the client dispatches a channel's deliveries in order on a single thread: that
     * ordering is what the correctness verdict rests on.
     */
    private static final class RabbitEventReceiver implements EventReceiver {

        /** Identity-compared, never correlated: only there to end a wait early. */
        private static final ReceivedEvent WAKEUP = new ReceivedEvent("", -1, -1, -1);

        private final Connection connection;
        private final Channel channel;
        private final BlockingQueue<ReceivedEvent> delivered = new LinkedBlockingQueue<>();

        RabbitEventReceiver(ConnectionFactory factory, String group) {
            try {
                this.connection = factory.newConnection();
                this.channel = connection.createChannel();
                DeliverCallback onDelivery = (consumerTag, message) ->
                        delivered.add(toEvent(message.getProperties()));
                // autoAck: this consumer observes, it does not process. Acknowledging each delivery
                // would add a round trip per event to the one thread the ordering guarantee rests on.
                channel.basicConsume(QUEUE, true, group, onDelivery, consumerTag -> { });
            } catch (Exception e) {
                throw new IllegalStateException("subscribing the benchmark consumer failed", e);
            }
        }

        @Override
        public List<ReceivedEvent> poll(Duration timeout) {
            ReceivedEvent first;
            try {
                first = delivered.poll(timeout.toMillis(), TimeUnit.MILLISECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return List.of();
            }
            if (first == null || first == WAKEUP) {
                return List.of();
            }
            List<ReceivedEvent> batch = new ArrayList<>();
            batch.add(first);
            delivered.drainTo(batch);
            batch.removeIf(event -> event == WAKEUP);
            return batch;
        }

        @Override
        public void wakeup() {
            delivered.add(WAKEUP);
        }

        /**
         * Idempotent and quiet, because it is called twice: a scenario declares the receiver as a
         * resource and hands it to the {@link CorrelationConsumer}, which owns it and closes it first.
         * The AMQP client throws on the second close where Kafka's consumer does not, so tolerating it
         * here is what keeps one set of scenarios valid on both.
         */
        @Override
        public void close() {
            closeQuietly(channel::close);
            closeQuietly(connection::close);
        }

        private static void closeQuietly(AutoCloseable closeable) {
            try {
                closeable.close();
            } catch (Exception alreadyGone) {
                // Nothing to report: either the broker tore it down or this is the second close.
            }
        }

        /**
         * The receive stamp is taken in the delivery callback, which is where this transport actually
         * hands the event over, not when the harness gets round to draining it.
         */
        private static ReceivedEvent toEvent(AMQP.BasicProperties properties) {
            Map<String, Object> headers = properties.getHeaders();
            return new ReceivedEvent(headerText(headers, CloudEventsHeaders.AMQP_PARTITION_KEY),
                    headerLong(headers, CloudEventsHeaders.AMQP_SEQ),
                    headerLong(headers, BenchmarkHeaders.T0_NANOS),
                    System.nanoTime());
        }

        private static String headerText(Map<String, Object> headers, String name) {
            Object value = headers == null ? null : headers.get(name);
            return value == null ? null : value.toString();
        }

        private static long headerLong(Map<String, Object> headers, String name) {
            String text = headerText(headers, name);
            if (text == null) {
                return -1;
            }
            try {
                return Long.parseLong(text.trim());
            } catch (NumberFormatException notANumber) {
                return -1;
            }
        }
    }
}
