package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.MessageEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class RabbitMessageEncoderTest {

    private static final String EXCHANGE = "tandem-events";
    private static final String SCHEMA_HEADER = "x-schema-id";
    private static final byte[] BODY = "avro-bytes".getBytes(StandardCharsets.UTF_8);

    private static final OutboxRecord RECORD = OutboxRecord.builder()
            .id(7)
            .message(OutboxMessage.builder()
                    .aggregateId("order-3").aggregateType("Order").seq(1).payload("{}".getBytes()).build())
            .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

    /** A format with nothing AMQP-specific about it: the case {@code from} exists to serve. */
    private static MessageEncoder neutralEncoder() {
        return record -> {
            Map<String, byte[]> headers = new LinkedHashMap<>();
            headers.put(SCHEMA_HEADER, String.valueOf(record.id()).getBytes(StandardCharsets.UTF_8));
            return new EncodedMessage("events." + record.aggregateType(), record.aggregateId().value(),
                    BODY, headers);
        };
    }

    @Test
    void GIVEN_a_transport_neutral_format_WHEN_it_is_published_over_amqp_THEN_its_destination_becomes_the_routing_key() {
        AmqpMessage message = RabbitMessageEncoder.from(neutralEncoder(), EXCHANGE).encode(RECORD);

        assertThat(message.route()).isEqualTo(new AmqpRoute(EXCHANGE, "events.Order"));
        assertThat(message.body()).isEqualTo(BODY);
    }

    @Test
    void GIVEN_a_transport_neutral_format_WHEN_it_is_published_over_amqp_THEN_its_header_bytes_narrow_to_text() {
        AmqpMessage message = RabbitMessageEncoder.from(neutralEncoder(), EXCHANGE).encode(RECORD);

        assertThat(message.properties().getHeaders())
                .containsEntry(SCHEMA_HEADER, String.valueOf(RECORD.id()));
    }

    @Test
    void GIVEN_a_transport_neutral_format_that_emits_no_headers_WHEN_it_is_published_over_amqp_THEN_none_are_invented() {
        MessageEncoder bare = record -> new EncodedMessage("events", record.aggregateId().value(), BODY, Map.of());

        assertThat(RabbitMessageEncoder.from(bare, EXCHANGE).encode(RECORD).properties().getHeaders()).isEmpty();
    }
}
