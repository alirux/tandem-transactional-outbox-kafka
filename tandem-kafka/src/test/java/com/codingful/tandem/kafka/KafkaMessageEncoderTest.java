package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.MessageEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;

class KafkaMessageEncoderTest {

    private static final String SCHEMA_HEADER = "x-schema-id";
    private static final byte[] BODY = "avro-bytes".getBytes(StandardCharsets.UTF_8);

    private static final OutboxRecord RECORD = OutboxRecord.builder()
            .id(7)
            .message(OutboxMessage.builder()
                    .aggregateId("order-3").aggregateType("Order").seq(1).payload("{}".getBytes()).build())
            .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

    /** A format with nothing Kafka-specific about it: the case {@code from} exists to serve. */
    private static MessageEncoder neutralEncoder() {
        return record -> {
            Map<String, byte[]> headers = new LinkedHashMap<>();
            headers.put(SCHEMA_HEADER, String.valueOf(record.id()).getBytes(StandardCharsets.UTF_8));
            return new EncodedMessage("events." + record.aggregateType(), record.aggregateId().value(),
                    BODY, headers);
        };
    }

    @Test
    void GIVEN_a_transport_neutral_format_WHEN_it_is_published_over_kafka_THEN_its_destination_key_body_and_headers_carry_over() {
        ProducerRecord<String, byte[]> encoded = KafkaMessageEncoder.from(neutralEncoder()).encode(RECORD);

        assertThat(encoded.topic()).isEqualTo("events.Order");
        assertThat(encoded.key()).isEqualTo(RECORD.aggregateId().value());
        assertThat(encoded.value()).isEqualTo(BODY);
        assertThat(new String(encoded.headers().lastHeader(SCHEMA_HEADER).value(), StandardCharsets.UTF_8))
                .isEqualTo(String.valueOf(RECORD.id()));
    }

    @Test
    void GIVEN_a_transport_neutral_format_that_emits_no_headers_WHEN_it_is_published_over_kafka_THEN_none_are_invented() {
        MessageEncoder bare = record -> new EncodedMessage("events", record.aggregateId().value(), BODY, Map.of());

        assertThat(KafkaMessageEncoder.from(bare).encode(RECORD).headers()).isEmpty();
    }
}
