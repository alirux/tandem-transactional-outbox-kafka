package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

@Tag("integration")
@SuppressWarnings("resource")   // JUnit @BeforeAll/@AfterAll lifecycle, not try-with-resources
class RabbitRelayIT {

    private static final RabbitMQContainer RABBIT =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.1-alpine"));

    private static final String EXCHANGE = "tandem-events";
    private static final String ORDER_QUEUE = "order-events";
    private static final String ORDER_ROUTING_KEY = "order-topic";
    private static final String SOURCE = "/tandem/orders";
    private static final String PREFIX = CloudEventsHeaders.AMQP_BINARY_PREFIX;

    @BeforeAll
    static void startBroker() throws Exception {
        RABBIT.start();
        // Tandem declares no topology (LLD-rabbitmq §1), so the test plays the operator: one topic
        // exchange, one queue, one binding.
        try (Connection connection = connectionFactory().newConnection();
             Channel channel = connection.createChannel()) {
            channel.exchangeDeclare(EXCHANGE, BuiltinExchangeType.TOPIC, true);
            channel.queueDeclare(ORDER_QUEUE, true, false, false, null);
            channel.queueBind(ORDER_QUEUE, EXCHANGE, ORDER_ROUTING_KEY);
        }
    }

    @AfterAll
    static void stopBroker() {
        RABBIT.stop();
    }

    private static ConnectionFactory connectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT.getHost());
        factory.setPort(RABBIT.getAmqpPort());
        factory.setUsername(RABBIT.getAdminUsername());
        factory.setPassword(RABBIT.getAdminPassword());
        return factory;
    }

    private static OutboxRecord order(long id, String aggregateId, long seq) {
        return OutboxRecord.builder()
                .id(id)
                .message(OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType("Order").type("com.acme.order.placed").seq(seq)
                        .payload(("{\"amount\":" + seq + "}").getBytes(StandardCharsets.UTF_8))
                        .contentType("application/json").header(TandemHeaders.CORRELATION_ID, "corr-1").build())
                .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
                .build();
    }

    /** Polls the queue until a message shows up or the deadline passes. */
    private static GetResponse consumeOne(Channel channel) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (System.nanoTime() < deadline) {
            GetResponse response = channel.basicGet(ORDER_QUEUE, true);
            if (response != null) {
                return response;
            }
            Thread.sleep(50);
        }
        throw new AssertionError("No message arrived on " + ORDER_QUEUE + " within the deadline");
    }

    @Test
    void GIVEN_a_stored_row_WHEN_it_is_published_to_a_real_broker_THEN_the_cloudevent_arrives_in_binary_form()
            throws Exception {
        OutboxRecord record = order(99, "order-7", 3);

        try (Connection connection = connectionFactory().newConnection();
             Channel consumer = connection.createChannel();
             RabbitRelay relay = new RabbitRelay(connectionFactory(), RabbitRelayConfig.of(SOURCE, EXCHANGE))) {

            relay.dispatch(record).get(10, TimeUnit.SECONDS);

            GetResponse delivered = consumeOne(consumer);
            assertThat(delivered.getEnvelope().getRoutingKey()).isEqualTo(ORDER_ROUTING_KEY);
            assertThat(delivered.getBody()).isEqualTo(record.payload());
            assertThat(delivered.getProps().getContentType()).isEqualTo(record.contentType());
            assertThat(delivered.getProps().getDeliveryMode()).isEqualTo(2);
            assertThat(headerText(delivered, PREFIX + "id")).isEqualTo(String.valueOf(record.id()));
            assertThat(headerText(delivered, PREFIX + "type")).isEqualTo(record.type());
            assertThat(headerText(delivered, PREFIX + "source")).isEqualTo(SOURCE);
            assertThat(headerText(delivered, PREFIX + "subject")).isEqualTo(record.aggregateId().value());
            assertThat(headerText(delivered, CloudEventsHeaders.AMQP_SEQ)).isEqualTo(String.valueOf(record.seq()));
            assertThat(headerText(delivered, CloudEventsHeaders.AMQP_PARTITION_KEY))
                    .isEqualTo(record.aggregateId().value());
            assertThat(headerText(delivered, TandemHeaders.CORRELATION_ID)).isEqualTo("corr-1");
        }
    }

    @Test
    void GIVEN_an_aggregates_events_WHEN_the_relay_publishes_them_one_after_its_ack_THEN_they_arrive_in_that_order()
            throws Exception {
        try (Connection connection = connectionFactory().newConnection();
             Channel consumer = connection.createChannel();
             RabbitRelay relay = new RabbitRelay(connectionFactory(), RabbitRelayConfig.of(SOURCE, EXCHANGE))) {

            // The relay never has two rows of one aggregate in flight: it dispatches the head and waits
            // for its ack before the next (LLD-jdbc §3.3). Published order is what this asserts.
            for (long seq = 1; seq <= 5; seq++) {
                relay.dispatch(order(200 + seq, "order-ordered", seq)).get(10, TimeUnit.SECONDS);
            }

            List<String> arrived = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                arrived.add(headerText(consumeOne(consumer), CloudEventsHeaders.AMQP_SEQ));
            }
            assertThat(arrived).containsExactly("1", "2", "3", "4", "5");
        }
    }

    @Test
    void GIVEN_a_row_whose_events_nobody_subscribed_to_WHEN_it_is_published_THEN_it_fails_permanently_instead_of_vanishing()
            throws Exception {
        // The exchange exists, but no binding matches this aggregate type: without `mandatory` the
        // broker would drop the message and confirm it anyway (LLD-rabbitmq §1).
        OutboxRecord unbound = OutboxRecord.builder()
                .id(500)
                .message(OutboxMessage.builder()
                        .aggregateId("ghost-1").aggregateType("Ghost").seq(1).payload("{}".getBytes()).build())
                .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
                .build();

        try (RabbitRelay relay = new RabbitRelay(connectionFactory(), RabbitRelayConfig.of(SOURCE, EXCHANGE))) {
            assertThatThrownBy(() -> relay.dispatch(unbound).get(10, TimeUnit.SECONDS))
                    .cause()
                    .isInstanceOf(OutboxDispatchException.class)
                    .satisfies(failure -> assertThat(((OutboxDispatchException) failure).isRetriable()).isFalse());
        }
    }

    @Test
    void GIVEN_an_exchange_nobody_created_WHEN_the_relay_starts_THEN_it_refuses_to_start_instead_of_failing_row_by_row() {
        assertThatThrownBy(() -> new RabbitRelay(connectionFactory(), RabbitRelayConfig.of(SOURCE, "no-such-exchange")))
                .isInstanceOf(TandemConfigurationException.class)
                .hasMessageContaining("no-such-exchange");
    }

    @Test
    void GIVEN_the_brokers_default_exchange_WHEN_the_relay_starts_THEN_it_does_not_try_to_verify_what_cannot_be_declared() {
        // AMQP's default exchange always exists and cannot be declared, passively or otherwise, so the
        // startup check must skip it rather than fail every relay configured that way (LLD-rabbitmq §1).
        try (RabbitRelay relay = new RabbitRelay(connectionFactory(), RabbitRelayConfig.of(SOURCE, ""))) {
            assertThat(relay.deliveryTimeoutMillis()).isPresent();
        }
    }

    private static String headerText(GetResponse delivered, String name) {
        Object value = delivered.getProps().getHeaders().get(name);
        return value == null ? null : value.toString();
    }
}
