package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaMessageEncoder;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import com.codingful.tandem.test.encoder.DebeziumStyleEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("integration")
class EndToEndIT {

    private static final String TOPIC = "order-topic";
    private static final int AGGREGATES = 10;
    private static final int PER_AGGREGATE = 20;
    private static final int TOTAL = AGGREGATES * PER_AGGREGATE;
    private static final int BUCKETS = 256;
    /** An aggregate with no version field — Tandem numbers its events (HLD-managed-seq §4.1). */
    private static final String UNNUMBERED_AGGREGATE = "order-managed";
    /** The topic the worked example routes {@code Order} to: Debezium's default route. */
    private static final String EXAMPLE_TOPIC = "outbox.event.Order";
    private static final String CORRELATION_ID = "corr-e2e";
    /**
     * Read the fixture payload {@code {"agg":"…","seq":n}}, which the test writes itself; whitespace is
     * allowed because the store may hand the JSON back normalised.
     */
    private static final Pattern PAYLOAD_SEQ = Pattern.compile("\"seq\"\\s*:\\s*(\\d+)");
    private static final Pattern PAYLOAD_AGGREGATE = Pattern.compile("\"agg\"\\s*:\\s*\"([^\"]+)\"");

    @Test
    void GIVEN_events_inserted_across_aggregates_WHEN_the_relay_runs_THEN_each_aggregate_lands_on_the_topic_in_order_with_no_loss() {
        try (TandemTestContainer tandem = new TandemTestContainer()) {
            tandem.start();
            // Insert in one transaction (a single batched insert), seq-ascending per aggregate.
            tandem.newRepository(BUCKETS).insertAll(buildMessages());
            tandem.createTopic(TOPIC, 4);   // multiple partitions so aggregate keys distribute

            try (KafkaConsumer<String, byte[]> consumer = tandem.newConsumer("e2e", TOPIC)) {
                consumer.poll(Duration.ofMillis(200));   // join the group before the relay publishes

                WorkerPool relay = tandem.newRelay(
                        RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(4)
                                .pollInterval(Duration.ofMillis(50)).build(),
                        TopicRouter.kebabWithSuffix("-topic"),
                        KafkaRelayConfig.of("/tandem/orders"));
                relay.start();
                try {
                    Map<String, List<Long>> deliveredSeqsByAggregate =
                            ordersByAggregate(pollAll(consumer, TOTAL), EndToEndIT::payloadSeq);

                    // Zero lost events: every (aggregate, seq) arrived exactly once.
                    long delivered = deliveredSeqsByAggregate.values().stream().mapToLong(List::size).sum();
                    assertThat(delivered).isEqualTo(TOTAL);
                    assertThat(deliveredSeqsByAggregate).hasSize(AGGREGATES);

                    // Zero ordering violations: each aggregate's events arrived in 1..N seq order.
                    List<Long> expected = expectedSeqs();
                    deliveredSeqsByAggregate.forEach((aggregate, seqs) ->
                            assertThat(seqs).as("delivery order for %s", aggregate).isEqualTo(expected));
                } finally {
                    relay.stop();
                }
            }
        }
    }

    @Test
    void GIVEN_an_aggregate_with_no_version_WHEN_its_events_are_relayed_THEN_they_arrive_in_write_order_carrying_the_number_tandem_assigned() {
        try (TandemTestContainer tandem = new TandemTestContainer()) {
            tandem.start();
            tandem.newRepository(BUCKETS).insertAll(buildUnnumberedMessages());
            tandem.createTopic(TOPIC, 4);

            try (KafkaConsumer<String, byte[]> consumer = tandem.newConsumer("e2e-managed", TOPIC)) {
                consumer.poll(Duration.ofMillis(200));

                WorkerPool relay = tandem.newRelay(
                        RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(4)
                                .pollInterval(Duration.ofMillis(50)).build(),
                        TopicRouter.kebabWithSuffix("-topic"),
                        KafkaRelayConfig.of("/tandem/orders"));
                relay.start();
                try {
                    Map<String, List<Long>> delivered =
                            ordersByAggregate(pollAll(consumer, PER_AGGREGATE), EndToEndIT::seqHeader);

                    assertThat(delivered).containsOnlyKeys(UNNUMBERED_AGGREGATE);
                    // The numbers are Tandem's, so nothing here knows what they are — only that every
                    // event carries one and that they reach the consumer in the order they were written.
                    assertThat(delivered.get(UNNUMBERED_AGGREGATE))
                            .hasSize(PER_AGGREGATE).isSorted().doesNotHaveDuplicates();
                } finally {
                    relay.stop();
                }
            }
        }
    }

    @Test
    void GIVEN_an_application_encoder_WHEN_the_relay_publishes_through_it_THEN_each_aggregate_arrives_in_order_with_its_row_headers_and_its_own_id() {
        try (TandemTestContainer tandem = new TandemTestContainer()) {
            tandem.start();
            tandem.newRepository(BUCKETS).insertAll(buildMessages(CORRELATION_ID));
            tandem.createTopic(EXAMPLE_TOPIC, 4);

            try (KafkaConsumer<String, byte[]> consumer = tandem.newConsumer("e2e-custom", EXAMPLE_TOPIC)) {
                consumer.poll(Duration.ofMillis(200));

                WorkerPool relay = tandem.newRelay(
                        RelayConfig.builder().bucketCount(BUCKETS).workersPerInstance(4)
                                .pollInterval(Duration.ofMillis(50)).build(),
                        KafkaMessageEncoder.from(new DebeziumStyleEncoder()));
                relay.start();
                try {
                    List<ConsumerRecord<String, byte[]>> records = pollAll(consumer, TOTAL);

                    assertThat(records).hasSize(TOTAL);
                    Map<String, List<Long>> ordersByAggregate = ordersByAggregate(records, EndToEndIT::payloadSeq);
                    assertThat(ordersByAggregate).hasSize(AGGREGATES);
                    List<Long> expected = expectedSeqs();
                    ordersByAggregate.forEach((aggregate, seqs) ->
                            assertThat(seqs).as("delivery order for %s", aggregate).isEqualTo(expected));
                    assertThat(records).allSatisfy(record -> {
                        assertThat(record.key()).isEqualTo(payloadAggregate(record));
                        assertThat(headerText(record, TandemHeaders.CORRELATION_ID)).isEqualTo(CORRELATION_ID);
                    });
                    assertThat(records).extracting(record -> headerText(record, DebeziumStyleEncoder.ID_HEADER))
                            .doesNotContainNull()
                            .doesNotHaveDuplicates();
                } finally {
                    relay.stop();
                }
            }
        }
    }

    private static List<OutboxMessage> buildUnnumberedMessages() {
        List<OutboxMessage> messages = new ArrayList<>(PER_AGGREGATE);
        for (int event = 0; event < PER_AGGREGATE; event++) {
            messages.add(OutboxMessage.builder()
                    .aggregateId(UNNUMBERED_AGGREGATE).aggregateType("Order").type("com.acme.order.changed")
                    .managedSeq()
                    .payload(("{\"event\":" + event + '}').getBytes(StandardCharsets.UTF_8))
                    .contentType("application/json").build());
        }
        return messages;
    }

    private static List<OutboxMessage> buildMessages() {
        return buildMessages(null);
    }

    /** The numbered fixture, each row carrying {@code correlationId} as a row header when it is not {@code null}. */
    private static List<OutboxMessage> buildMessages(String correlationId) {
        List<OutboxMessage> messages = new ArrayList<>(TOTAL);
        for (int a = 0; a < AGGREGATES; a++) {
            String aggregateId = "order-" + a;
            for (long seq = 1; seq <= PER_AGGREGATE; seq++) {
                OutboxMessage.Builder message = OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType("Order").type("com.acme.order.changed").seq(seq)
                        .payload(("{\"agg\":\"" + aggregateId + "\",\"seq\":" + seq + "}").getBytes(StandardCharsets.UTF_8))
                        .contentType("application/json");
                if (correlationId != null) {
                    message.header(TandemHeaders.CORRELATION_ID, correlationId);
                }
                messages.add(message.build());
            }
        }
        return messages;
    }

    private static List<Long> expectedSeqs() {
        List<Long> expected = new ArrayList<>(PER_AGGREGATE);
        for (long seq = 1; seq <= PER_AGGREGATE; seq++) {
            expected.add(seq);
        }
        return expected;
    }

    /** Poll until {@code expected} events arrive (or time out), in arrival order. */
    private static List<ConsumerRecord<String, byte[]>> pollAll(KafkaConsumer<String, byte[]> consumer, int expected) {
        List<ConsumerRecord<String, byte[]>> arrived = new ArrayList<>();
        long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
        while (arrived.size() < expected && System.nanoTime() < deadline) {
            ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(500));
            records.forEach(arrived::add);
        }
        return arrived;
    }

    /** Each aggregate's order numbers in arrival order, the aggregate being the record key. */
    private static Map<String, List<Long>> ordersByAggregate(
            List<ConsumerRecord<String, byte[]>> records, Function<ConsumerRecord<String, byte[]>, Long> order) {
        Map<String, List<Long>> ordersByAggregate = new LinkedHashMap<>();
        for (ConsumerRecord<String, byte[]> record : records) {
            ordersByAggregate.computeIfAbsent(record.key(), k -> new ArrayList<>()).add(order.apply(record));
        }
        return ordersByAggregate;
    }

    /** The number Tandem published in the CloudEvents envelope. */
    private static long seqHeader(ConsumerRecord<String, byte[]> record) {
        return Long.parseLong(headerText(record, CloudEventsHeaders.CE_SEQ));
    }

    /** The number the test wrote into the payload, readable whatever envelope carried it. */
    private static long payloadSeq(ConsumerRecord<String, byte[]> record) {
        return Long.parseLong(payloadField(record, PAYLOAD_SEQ));
    }

    private static String payloadAggregate(ConsumerRecord<String, byte[]> record) {
        return payloadField(record, PAYLOAD_AGGREGATE);
    }

    private static String payloadField(ConsumerRecord<String, byte[]> record, Pattern field) {
        Matcher matcher = field.matcher(new String(record.value(), StandardCharsets.UTF_8));
        assertThat(matcher.find()).as("payload field %s", field).isTrue();
        return matcher.group(1);
    }

    private static String headerText(ConsumerRecord<String, byte[]> record, String name) {
        Header header = record.headers().lastHeader(name);
        return header == null ? null : new String(header.value(), StandardCharsets.UTF_8);
    }
}
