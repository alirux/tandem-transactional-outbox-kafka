package com.codingful.tandem.cloudevents;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import io.cloudevents.CloudEvent;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Map;
import org.junit.jupiter.api.Test;

class CloudEventFactoryTest {

    private static final URI SOURCE = URI.create("/tandem/orders");
    private static final String DEFAULT_CONTENT_TYPE = "application/json";
    private static final String STORED_SCHEMA = "https://schemas.example/order";

    private final CloudEventFactory factory = new CloudEventFactory(SOURCE, DEFAULT_CONTENT_TYPE, null);

    private static OutboxMessage.Builder orderMessage() {
        return OutboxMessage.builder().aggregateId("order-7").aggregateType("Order").seq(5)
                .payload("{\"amount\":1}".getBytes(StandardCharsets.UTF_8));
    }

    private static OutboxRecord recordOf(OutboxMessage message) {
        return OutboxRecord.builder().id(42).message(message)
                .createdAt(Instant.parse("2024-01-01T00:00:00Z")).build();
    }

    @Test
    void GIVEN_a_stored_row_WHEN_the_envelope_is_built_THEN_its_attributes_come_from_the_row_and_the_configured_source() {
        OutboxRecord record = recordOf(orderMessage().type("com.acme.order.placed").build());

        CloudEvent event = factory.build(record);

        assertThat(event.getId()).isEqualTo(String.valueOf(record.id()));
        assertThat(event.getSource()).isEqualTo(SOURCE);
        assertThat(event.getType()).isEqualTo("com.acme.order.placed");
        assertThat(event.getSubject()).isEqualTo(record.aggregateId().value());
        assertThat(event.getData().toBytes()).isEqualTo(record.payload());
        assertThat(event.getExtension(CloudEventsHeaders.EXT_SEQ)).isEqualTo(record.seq());
        assertThat(event.getExtension(CloudEventsHeaders.EXT_PARTITION_KEY))
                .isEqualTo(record.aggregateId().value());
    }

    @Test
    void GIVEN_a_row_with_no_event_type_of_its_own_WHEN_the_envelope_is_built_THEN_the_aggregate_type_stands_in() {
        CloudEvent event = factory.build(recordOf(orderMessage().build()));

        assertThat(event.getType()).isEqualTo("Order");
    }

    @Test
    void GIVEN_an_event_written_without_a_sequence_number_WHEN_the_envelope_is_built_THEN_seq_is_absent_rather_than_zero() {
        OutboxRecord record = recordOf(OutboxMessage.builder()
                .aggregateId("order-8").aggregateType("Order").unsequenced().payload("{}".getBytes()).build());

        CloudEvent event = factory.build(record);

        assertThat(event.getExtension(CloudEventsHeaders.EXT_SEQ)).isNull();
        // What a consumer deduplicates on is still there, which is why omitting seq costs nothing.
        assertThat(event.getId()).isEqualTo("42");
    }

    @Test
    void GIVEN_a_row_that_stored_no_content_type_WHEN_the_envelope_is_built_THEN_the_configured_default_applies() {
        assertThat(factory.build(recordOf(orderMessage().build())).getDataContentType())
                .isEqualTo(DEFAULT_CONTENT_TYPE);
    }

    @Test
    void GIVEN_a_row_that_stored_its_own_content_type_WHEN_the_envelope_is_built_THEN_it_wins_over_the_default() {
        OutboxRecord record = recordOf(orderMessage().contentType("application/avro").build());

        assertThat(factory.build(record).getDataContentType()).isEqualTo("application/avro");
    }

    @Test
    void GIVEN_a_content_type_persisted_as_a_header_WHEN_the_envelope_is_built_THEN_it_is_read_from_there() {
        // The normal shape of a stored row: the write side persists contentType into
        // headers["content-type"] rather than a column of its own (LLD-jdbc §2).
        OutboxRecord record = recordOf(orderMessage()
                .header(TandemHeaders.CONTENT_TYPE, "application/avro").build());

        assertThat(factory.build(record).getDataContentType()).isEqualTo("application/avro");
    }

    @Test
    void GIVEN_a_row_that_stored_a_schema_WHEN_the_envelope_is_built_THEN_it_becomes_the_data_schema_attribute() {
        OutboxRecord record = recordOf(orderMessage().header(TandemHeaders.DATA_SCHEMA, STORED_SCHEMA).build());

        assertThat(factory.build(record).getDataSchema()).isEqualTo(URI.create(STORED_SCHEMA));
    }

    @Test
    void GIVEN_a_schema_registry_setup_WHEN_a_row_stored_no_schema_THEN_the_configured_default_applies() {
        URI configured = URI.create("https://registry.example/order-v1");
        CloudEventFactory withDefault = new CloudEventFactory(SOURCE, DEFAULT_CONTENT_TYPE, configured);

        assertThat(withDefault.build(recordOf(orderMessage().build())).getDataSchema()).isEqualTo(configured);
    }

    @Test
    void GIVEN_no_schema_anywhere_WHEN_the_envelope_is_built_THEN_the_attribute_is_omitted_rather_than_empty() {
        assertThat(factory.build(recordOf(orderMessage().build())).getDataSchema()).isNull();
    }

    @Test
    void GIVEN_headers_the_envelope_already_carries_WHEN_the_passthrough_set_is_taken_THEN_only_the_others_remain() {
        OutboxRecord record = recordOf(orderMessage()
                .header(TandemHeaders.CONTENT_TYPE, DEFAULT_CONTENT_TYPE)
                .header(TandemHeaders.DATA_SCHEMA, STORED_SCHEMA)
                .header(TandemHeaders.CORRELATION_ID, "corr-9")
                .build());

        assertThat(factory.passthroughHeaders(record))
                .containsExactly(Map.entry(TandemHeaders.CORRELATION_ID, "corr-9"));
    }

    @Test
    void GIVEN_a_row_with_no_headers_of_its_own_WHEN_the_passthrough_set_is_taken_THEN_it_is_empty() {
        assertThat(factory.passthroughHeaders(recordOf(orderMessage().build()))).isEmpty();
    }
}
