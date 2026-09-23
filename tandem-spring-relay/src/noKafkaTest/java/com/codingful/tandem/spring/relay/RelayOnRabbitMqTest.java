package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.rabbitmq.RabbitRelay;
import com.codingful.tandem.rabbitmq.RabbitRelayConfig;
import com.codingful.tandem.spring.relay.RelayWithoutKafkaTest.UnconnectedDataSource;
import com.codingful.tandem.spring.relay.RelayWithoutKafkaTest.UnstartedLifecycle;
import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.GetResponse;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.concurrent.TimeUnit;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.testcontainers.containers.RabbitMQContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The Spring wiring the user guide gives for RabbitMQ (guide/rabbitmq.md §4.1), run as written against
 * a real broker on a classpath with no Kafka adapter: a {@link RabbitRelay} contributed as a bean is the
 * dispatcher the relay publishes through, and Spring releases its broker connection on shutdown. There
 * is no RabbitMQ autoconfiguration, so this bean is the whole integration; if the guide's snippet
 * stopped wiring, nothing else would notice.
 */
@Tag("integration")
@SuppressWarnings("resource")   // JUnit @BeforeAll/@AfterAll lifecycle, not try-with-resources
class RelayOnRabbitMqTest {

    private static final RabbitMQContainer RABBIT =
            new RabbitMQContainer(DockerImageName.parse("rabbitmq:4.1-alpine"));

    private static final String EXCHANGE = "tandem-events";
    private static final String ORDER_QUEUE = "order-events";
    private static final String ORDER_ROUTING_KEY = "order-topic";
    private static final String SOURCE = "/orders/service";

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemRelayAutoConfiguration.class))
            .withBean(DataSource.class, UnconnectedDataSource::new)
            .withUserConfiguration(GuideRelayConfiguration.class, UnstartedLifecycle.class);

    @BeforeAll
    static void startBroker() throws Exception {
        RABBIT.start();
        // Tandem declares no topology: the test plays the operator, as the guide's §3 asks.
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

    static ConnectionFactory connectionFactory() {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost(RABBIT.getHost());
        factory.setPort(RABBIT.getAmqpPort());
        factory.setUsername(RABBIT.getAdminUsername());
        factory.setPassword(RABBIT.getAdminPassword());
        return factory;
    }

    @Test
    void GIVEN_the_guide_rabbitmq_bean_WHEN_the_application_starts_THEN_the_relay_publishes_through_it_to_the_broker()
            throws Exception {
        OutboxRecord order = order();

        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(context).hasSingleBean(WorkerPool.class);
            OutboxDispatcher dispatcher = context.getBean(OutboxDispatcher.class);
            assertThat(dispatcher).isInstanceOf(RabbitRelay.class);

            dispatcher.dispatch(order).get(10, TimeUnit.SECONDS);
        });

        try (Connection connection = connectionFactory().newConnection();
             Channel channel = connection.createChannel()) {
            GetResponse delivered = channel.basicGet(ORDER_QUEUE, true);
            assertThat(delivered).isNotNull();
            assertThat(delivered.getBody()).isEqualTo(order.payload());
        }
    }

    @Test
    void GIVEN_the_guide_rabbitmq_bean_WHEN_the_application_shuts_down_THEN_its_broker_connection_is_released()
            throws Exception {
        runner.run(context -> {
            assertThat(context).hasNotFailed();
            assertThat(openConnections()).isPositive();
        });

        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (openConnections() > 0 && System.nanoTime() < deadline) {
            Thread.sleep(100);
        }
        assertThat(openConnections()).isZero();
    }

    /** Counted by the broker itself, so what is asserted is the connection actually released. */
    private static int openConnections() throws Exception {
        String names = RABBIT.execInContainer("rabbitmqctl", "list_connections", "--silent", "name").getStdout();
        return (int) names.lines().filter(line -> !line.isBlank()).count();
    }

    private static OutboxRecord order() {
        return OutboxRecord.builder()
                .id(1)
                .message(OutboxMessage.builder()
                        .aggregateId("order-1").aggregateType("Order").type("com.acme.order.placed").unsequenced()
                        .payload("{\"amount\":1}".getBytes(StandardCharsets.UTF_8))
                        .contentType("application/json").build())
                .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
                .build();
    }

    /** The guide's §4.1 bean, verbatim apart from where the connection settings come from. */
    @Configuration(proxyBeanMethods = false)
    static class GuideRelayConfiguration {

        @Bean
        RabbitRelay outboxDispatcher(ObjectProvider<TandemSpanRecorder> spans) {
            return new RabbitRelay(connectionFactory(),
                    RabbitRelayConfig.of(SOURCE, EXCHANGE),
                    spans.getIfAvailable(() -> TandemSpanRecorder.NOOP));
        }
    }
}
