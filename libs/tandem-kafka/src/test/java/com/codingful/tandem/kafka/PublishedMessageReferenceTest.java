package com.codingful.tandem.kafka;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.RawMessageEncoder;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.test.reference.ReferenceFile;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.stream.StreamSupport;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;

/**
 * Pins the published message (HLD-api-stability §2): the record key, every header with its value, and
 * the body each built-in envelope puts on Kafka, for a record with every optional field set and for one
 * with none (LLD-api-stability §8.2). The topic comes from the default router, so its naming is pinned
 * too. The files write the wire names as literals: built from the constants, they could not notice a
 * constant whose value changed.
 */
class PublishedMessageReferenceTest {

    private static final String SOURCE = "/tandem/reference";
    private static final TopicRouter DEFAULT_ROUTER = TopicRouter.kebabWithSuffix("-topic");

    /** Every optional field set: an event type, an application seq, stored content type and data schema, two more headers. */
    private static final OutboxRecord COMPLETE = OutboxRecord.builder()
            .id(1001)
            .message(OutboxMessage.builder()
                    .aggregateType("OrderLine")
                    .aggregateId("order-line-7")
                    .type("OrderLineAdded")
                    .seq(3)
                    .payload("{\"sku\":\"A-1\",\"quantity\":2}".getBytes(StandardCharsets.UTF_8))
                    .header(TandemHeaders.CONTENT_TYPE, "application/vnd.tandem.order-line+json")
                    .header(TandemHeaders.DATA_SCHEMA, "https://schemas.example.com/order-line/1")
                    .header("x-tenant", "acme")
                    .header(TandemHeaders.CORRELATION_ID, "checkout-42")
                    .build())
            .createdAt(Instant.parse("2025-03-01T10:15:30.123Z"))
            .build();

    /** No optional field: unsequenced, no event type, no header, no content type. */
    private static final OutboxRecord MINIMAL = OutboxRecord.builder()
            .id(1002)
            .message(OutboxMessage.builder()
                    .aggregateType("Order")
                    .aggregateId("order-9")
                    .unsequenced()
                    .payload("{\"total\":10}".getBytes(StandardCharsets.UTF_8))
                    .build())
            .createdAt(Instant.parse("2025-03-01T10:15:31Z"))
            .build();

    @Test
    void GIVEN_a_complete_and_a_minimal_record_WHEN_published_as_cloudevents_THEN_the_messages_match_their_reference_file() {
        KafkaMessageEncoder withDefaultSchema = KafkaMessageEncoder.cloudEvents(DEFAULT_ROUTER,
                new KafkaRelayConfig(URI.create(SOURCE), "application/json",
                        URI.create("https://schemas.example.com/default/1")));
        KafkaMessageEncoder withDefaults = KafkaMessageEncoder.cloudEvents(DEFAULT_ROUTER, KafkaRelayConfig.of(SOURCE));

        List<String> lines = new ArrayList<>();
        // Binary mode gives the order of its headers no meaning, so they are sorted.
        render(lines, "complete", withDefaultSchema.encode(COMPLETE), true);
        lines.add("");
        render(lines, "minimal", withDefaults.encode(MINIMAL), true);

        ReferenceFile.of("message-cloudevents.txt", PublishedMessageReferenceTest.class).assertMatches(lines);
    }

    @Test
    void GIVEN_a_complete_and_a_minimal_record_WHEN_published_raw_THEN_the_messages_match_their_reference_file() {
        KafkaMessageEncoder raw = KafkaMessageEncoder.from(new RawMessageEncoder(DEFAULT_ROUTER));

        List<String> lines = new ArrayList<>();
        // The raw envelope promises the stored order (LLD-alternative-envelope §2.3), so it is kept.
        render(lines, "complete", raw.encode(COMPLETE), false);
        lines.add("");
        render(lines, "minimal", raw.encode(MINIMAL), false);

        ReferenceFile.of("message-raw.txt", PublishedMessageReferenceTest.class).assertMatches(lines);
    }

    private static void render(List<String> lines, String name, ProducerRecord<String, byte[]> message,
            boolean sortHeaders) {
        lines.add("[" + name + "]");
        lines.add("topic " + message.topic());
        lines.add("key " + message.key());
        List<Header> headers = StreamSupport.stream(message.headers().spliterator(), false).toList();
        if (sortHeaders) {
            headers = headers.stream().sorted(Comparator.comparing(Header::key)).toList();
        }
        headers.forEach(header ->
                lines.add("header " + header.key() + " " + new String(header.value(), StandardCharsets.UTF_8)));
        lines.add("body " + new String(message.value(), StandardCharsets.UTF_8));
    }
}
