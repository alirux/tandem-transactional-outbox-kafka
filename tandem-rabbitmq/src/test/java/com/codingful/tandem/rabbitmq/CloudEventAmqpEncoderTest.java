package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CloudEventAmqpEncoderTest {

    private static final String PREFIX = CloudEventsHeaders.AMQP_BINARY_PREFIX;
    private static final String EXCHANGE = "tandem-events";
    private static final String SOURCE = "/tandem/orders";
    private static final String AGGREGATE_ID = "order-1";
    private static final String CORRELATION_ID = "corr-9";
    private static final byte[] PAYLOAD = "{\"total\":10}".getBytes(StandardCharsets.UTF_8);

    private static final RabbitRelayConfig CONFIG = RabbitRelayConfig.of(SOURCE, EXCHANGE);

    private static CloudEventAmqpEncoder encoder() {
        return new CloudEventAmqpEncoder(RabbitRouter.kebabWithSuffix(EXCHANGE, "-topic"), CONFIG);
    }

    private static OutboxRecord.Builder recordWith(OutboxMessage message) {
        return OutboxRecord.builder().id(7).message(message).createdAt(Instant.parse("2024-01-01T00:00:00Z"));
    }

    private static OutboxMessage.Builder orderMessage() {
        return OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID).aggregateType("Order").seq(3).payload(PAYLOAD);
    }

    @Test
    void GIVEN_a_stored_row_WHEN_it_is_bound_to_an_amqp_message_THEN_the_envelope_travels_as_prefixed_headers() {
        OutboxRecord record = recordWith(orderMessage().build()).build();

        AmqpMessage message = encoder().encode(record);
        Map<String, Object> headers = message.properties().getHeaders();

        assertThat(headers).containsEntry(PREFIX + "specversion", "1.0")
                .containsEntry(PREFIX + "id", String.valueOf(record.id()))
                .containsEntry(PREFIX + "source", SOURCE)
                .containsEntry(PREFIX + "type", record.aggregateType())
                .containsEntry(PREFIX + "subject", AGGREGATE_ID)
                .containsEntry(CloudEventsHeaders.AMQP_PARTITION_KEY, AGGREGATE_ID)
                .containsEntry(CloudEventsHeaders.AMQP_SEQ, String.valueOf(record.seq()))
                .containsKey(PREFIX + "time");
        assertThat(message.body()).isEqualTo(PAYLOAD);
    }

    @Test
    void GIVEN_a_payload_media_type_WHEN_the_row_is_bound_THEN_it_rides_the_amqp_content_type_rather_than_a_header() {
        OutboxRecord record = recordWith(orderMessage().contentType("application/avro").build()).build();

        AmqpMessage message = encoder().encode(record);

        assertThat(message.properties().getContentType()).isEqualTo("application/avro");
        assertThat(message.properties().getHeaders()).doesNotContainKey(PREFIX + "datacontenttype")
                .doesNotContainKey(TandemHeaders.CONTENT_TYPE);
    }

    @Test
    void GIVEN_a_row_written_without_an_order_number_WHEN_it_is_bound_THEN_no_sequence_is_invented_for_it() {
        OutboxRecord record = recordWith(OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID).aggregateType("Order").unsequenced().payload(PAYLOAD).build()).build();

        assertThat(encoder().encode(record).properties().getHeaders())
                .doesNotContainKey(CloudEventsHeaders.AMQP_SEQ)
                .containsEntry(CloudEventsHeaders.AMQP_PARTITION_KEY, AGGREGATE_ID);
    }

    @Test
    void GIVEN_a_schema_reference_on_the_row_WHEN_it_is_bound_THEN_it_travels_as_the_envelope_attribute_not_a_raw_header() {
        String schema = "https://schemas.acme.com/order/v2.json";
        OutboxRecord record = recordWith(orderMessage().header(TandemHeaders.DATA_SCHEMA, schema).build()).build();

        assertThat(encoder().encode(record).properties().getHeaders())
                .containsEntry(PREFIX + "dataschema", schema)
                .doesNotContainKey(TandemHeaders.DATA_SCHEMA);
    }

    @Test
    void GIVEN_application_headers_on_the_row_WHEN_it_is_bound_THEN_they_reach_the_consumer_untouched() {
        OutboxRecord record = recordWith(orderMessage()
                .header(TandemHeaders.CORRELATION_ID, CORRELATION_ID).build()).build();

        assertThat(encoder().encode(record).properties().getHeaders())
                .containsEntry(TandemHeaders.CORRELATION_ID, CORRELATION_ID);
    }

    @Test
    void GIVEN_an_aggregate_type_WHEN_the_default_router_places_it_THEN_the_routing_key_is_its_kebab_case_name() {
        OutboxRecord record = recordWith(OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID).aggregateType("OrderLine").seq(1).payload(PAYLOAD).build()).build();

        assertThat(encoder().encode(record).route()).isEqualTo(new AmqpRoute(EXCHANGE, "order-line-topic"));
    }
}
