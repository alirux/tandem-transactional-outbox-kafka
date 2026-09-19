package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.kafka.KafkaRelay;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.ExecutionException;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * {@link BrokerHarness} over a real Kafka broker: the default, and what the archived numbers were
 * measured on (LLD-benchmark §3.1).
 *
 * <p>The producer already defaults to the mandated {@code acks=all} + {@code enable.idempotence=true}:
 * {@link KafkaRelay} hardens any producer config to those values (LLD-kafka §1), so nothing here has
 * to override them to get the production configuration.
 */
@SuppressWarnings({"deprecation", "resource"})  // deprecation: legacy KafkaContainer+withKraft; resource: close() manages the container
final class KafkaBrokerHarness implements BrokerHarness {

    static final String TOPIC = "tandem-benchmark-events";

    /** Enough partitions that the relay's workers are not serialised behind one leader. */
    private static final int TOPIC_PARTITIONS = 16;

    private final BenchmarkConfig config;
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1")).withKraft();
    private final List<KafkaRelay> producers = new ArrayList<>();

    KafkaBrokerHarness(BenchmarkConfig config) {
        this.config = config;
    }

    @Override
    public void start() {
        kafka.start();
        createTopic();
    }

    @Override
    public OutboxDispatcher newDispatcher(TandemSpanRecorder spanRecorder) {
        KafkaRelay relay = new KafkaRelay(producerConfig(), record -> TOPIC,
                KafkaRelayConfig.of("/tandem/benchmark"), spanRecorder);
        producers.add(relay);
        return relay;
    }

    @Override
    public long deliveryTimeoutMillis() {
        // Read back off a producer rather than returned from the config: the value the relay validates
        // the row lease against is the hardened one, and producerConfig() is the only reason the two
        // agree today.
        return producers.isEmpty() ? config.deliveryTimeoutMs() : producers.get(0).deliveryTimeoutMs();
    }

    @Override
    public EventReceiver newReceiver(String group) {
        return new KafkaEventReceiver(newKafkaConsumer(group));
    }

    /**
     * The raw consumer, for the one caller that needs Kafka's own record (the tracing demo's
     * consumer-side span bridge reads {@code traceparent} off it). Closed by its caller, as
     * {@link #newReceiver} results are.
     */
    KafkaConsumer<String, byte[]> newKafkaConsumer(String group) {
        Map<String, Object> consumerConfig = new HashMap<>();
        consumerConfig.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        consumerConfig.put(ConsumerConfig.GROUP_ID_CONFIG, group);
        consumerConfig.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(consumerConfig, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(TOPIC));
        return consumer;
    }

    /**
     * Nothing: a Kafka topic is a log, and each scenario reads it under a group of its own from the
     * beginning. What the previous scenario published stays there and is re-read, which is harmless
     * because every scenario addresses aggregates seeded from its own id.
     */
    @Override
    public void resetBetweenScenarios() {
        // intentionally empty, see the javadoc
    }

    /** Empty: the producer keeps its in-flight batches to itself. */
    @Override
    public OptionalLong inFlightPublishes() {
        return OptionalLong.empty();
    }

    @Override
    public void interruptBroker() {
        DockerPause.pause(kafka.getContainerId());
    }

    @Override
    public void restoreBroker() {
        DockerPause.unpause(kafka.getContainerId());
    }

    @Override
    public void close() {
        for (KafkaRelay producer : producers) {
            producer.close();
        }
        kafka.stop();
    }

    private Map<String, Object> producerConfig() {
        Map<String, Object> producerConfig = new HashMap<>();
        producerConfig.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        producerConfig.put(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, (int) config.deliveryTimeoutMs());
        // Kafka requires delivery.timeout.ms >= linger.ms + request.timeout.ms; request.timeout.ms
        // defaults to 30s, so a smaller deliveryTimeoutMs (the smoke config) must shrink it too.
        producerConfig.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, (int) Math.min(30_000, config.deliveryTimeoutMs()));
        return producerConfig;
    }

    private void createTopic() {
        Map<String, Object> adminConfig =
                Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafka.getBootstrapServers());
        try (Admin admin = Admin.create(adminConfig)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, TOPIC_PARTITIONS, (short) 1))).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted creating the benchmark topic", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("creating the benchmark topic failed", e);
        }
    }

    /** Drains {@code poll} into the harness's neutral shape; the whole batch shares one receive stamp. */
    private static final class KafkaEventReceiver implements EventReceiver {

        private final KafkaConsumer<String, byte[]> consumer;

        KafkaEventReceiver(KafkaConsumer<String, byte[]> consumer) {
            this.consumer = consumer;
        }

        @Override
        public List<ReceivedEvent> poll(Duration timeout) {
            ConsumerRecords<String, byte[]> records;
            try {
                records = consumer.poll(timeout);
            } catch (WakeupException stopping) {
                return List.of();
            }
            long receivedNanos = System.nanoTime();
            List<ReceivedEvent> events = new ArrayList<>(records.count());
            for (ConsumerRecord<String, byte[]> record : records) {
                events.add(new ReceivedEvent(record.key(), headerLong(record, CloudEventsHeaders.CE_SEQ),
                        headerLong(record, BenchmarkHeaders.T0_NANOS), receivedNanos));
            }
            return events;
        }

        @Override
        public void wakeup() {
            consumer.wakeup();
        }

        @Override
        public void close() {
            consumer.close();
        }

        private static long headerLong(ConsumerRecord<String, byte[]> record, String headerName) {
            Header header = record.headers().lastHeader(headerName);
            if (header == null) {
                return -1;
            }
            try {
                return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8).trim());
            } catch (NumberFormatException notANumber) {
                return -1;
            }
        }
    }
}
