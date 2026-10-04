package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.TopicRouter;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class RawMessageEncoderTest {

    private static final long ROW_ID = 42;
    private static final String AGGREGATE_ID = "order-7";
    private static final String AGGREGATE_TYPE = "Order";
    private static final String EVENT_TYPE = "com.acme.order.placed";
    private static final String JSON = "application/json";
    private static final String CORRELATION = "corr-9";
    private static final String APP_HEADER = "x-tenant";
    private static final String APP_VALUE = "acme";
    private static final byte[] PAYLOAD = "{\"amount\":1}".getBytes(StandardCharsets.UTF_8);

    private final RawMessageEncoder encoder = new RawMessageEncoder(TopicRouter.kebabWithSuffix("-topic"));

    private static OutboxMessage.Builder order() {
        return OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID).aggregateType(AGGREGATE_TYPE).type(EVENT_TYPE).seq(3).payload(PAYLOAD);
    }

    private static OutboxRecord stored(OutboxMessage message) {
        return OutboxRecord.builder().id(ROW_ID).message(message).createdAt(Instant.EPOCH).build();
    }

    private static String text(EncodedMessage message, String name) {
        byte[] value = message.headers().get(name);
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    @Test
    void GIVEN_a_stored_row_WHEN_it_is_published_raw_THEN_the_body_is_the_payload_and_the_key_is_the_aggregate() {
        EncodedMessage encoded = encoder.encode(stored(order().build()));

        assertThat(encoded.body()).isEqualTo(PAYLOAD);
        assertThat(encoded.key()).isEqualTo(AGGREGATE_ID);
    }

    @Test
    void GIVEN_a_stored_row_WHEN_it_is_published_raw_THEN_it_carries_the_row_id_as_its_event_id() {
        EncodedMessage encoded = encoder.encode(stored(order().build()));

        assertThat(text(encoded, RawHeaders.ID)).isEqualTo(String.valueOf(ROW_ID));
    }

    @Test
    void GIVEN_a_typed_row_WHEN_it_is_published_raw_THEN_the_type_header_is_the_event_type() {
        EncodedMessage encoded = encoder.encode(stored(order().build()));

        assertThat(text(encoded, RawHeaders.TYPE)).isEqualTo(EVENT_TYPE);
    }

    @Test
    void GIVEN_a_row_stored_without_an_event_type_WHEN_it_is_published_raw_THEN_the_type_header_is_the_aggregate_type() {
        EncodedMessage encoded = encoder.encode(stored(order().type(null).build()));

        assertThat(text(encoded, RawHeaders.TYPE)).isEqualTo(AGGREGATE_TYPE);
    }

    @Test
    void GIVEN_row_headers_WHEN_the_row_is_published_raw_THEN_every_one_reaches_the_wire_in_stored_order_after_raws_own() {
        OutboxMessage message = order()
                .header(TandemHeaders.CONTENT_TYPE, JSON)
                .header(TandemHeaders.DATA_SCHEMA, "https://schemas.example/order")
                .header(TandemHeaders.CORRELATION_ID, CORRELATION)
                .header(APP_HEADER, APP_VALUE)
                .build();

        EncodedMessage encoded = encoder.encode(stored(message));

        assertThat(encoded.headers().keySet()).containsExactly(
                RawHeaders.ID, RawHeaders.TYPE,
                TandemHeaders.CONTENT_TYPE, TandemHeaders.DATA_SCHEMA, TandemHeaders.CORRELATION_ID, APP_HEADER);
        message.headers().forEach((name, value) -> assertThat(text(encoded, name)).as(name).isEqualTo(value));
    }

    @Test
    void GIVEN_row_headers_named_id_and_type_WHEN_the_row_is_published_raw_THEN_they_reach_the_wire_like_any_other() {
        OutboxMessage message = order().header("id", "app-id").header("type", "app-type").build();

        EncodedMessage encoded = encoder.encode(stored(message));

        assertThat(text(encoded, "id")).isEqualTo(message.headers().get("id"));
        assertThat(text(encoded, "type")).isEqualTo(message.headers().get("type"));
    }

    @Test
    void GIVEN_row_headers_named_like_raws_own_WHEN_the_row_is_published_raw_THEN_the_encoders_values_win() {
        OutboxMessage message = order().header(RawHeaders.ID, "forged-id").header(RawHeaders.TYPE, "forged-type").build();

        EncodedMessage encoded = encoder.encode(stored(message));

        assertThat(text(encoded, RawHeaders.ID)).isEqualTo(String.valueOf(ROW_ID));
        assertThat(text(encoded, RawHeaders.TYPE)).isEqualTo(EVENT_TYPE);
        assertThat(encoded.headers()).hasSize(2);
    }

    @Test
    void GIVEN_a_record_carrying_its_media_type_only_as_a_field_WHEN_published_raw_THEN_it_travels_as_the_content_type_header() {
        EncodedMessage encoded = encoder.encode(stored(order().contentType(JSON).build()));

        assertThat(text(encoded, TandemHeaders.CONTENT_TYPE)).isEqualTo(JSON);
    }

    @Test
    void GIVEN_a_stored_content_type_header_and_a_different_field_WHEN_published_raw_THEN_the_stored_header_wins() {
        String stored = "application/avro";
        OutboxMessage message = order().contentType(JSON).header(TandemHeaders.CONTENT_TYPE, stored).build();

        EncodedMessage encoded = encoder.encode(stored(message));

        assertThat(text(encoded, TandemHeaders.CONTENT_TYPE)).isEqualTo(stored);
    }

    @Test
    void GIVEN_a_record_with_no_media_type_at_all_WHEN_published_raw_THEN_no_content_type_is_invented() {
        EncodedMessage encoded = encoder.encode(stored(order().build()));

        assertThat(encoded.headers()).doesNotContainKey(TandemHeaders.CONTENT_TYPE);
    }

    @Test
    void GIVEN_a_row_written_unsequenced_WHEN_published_raw_THEN_it_encodes_and_publishes_no_sequence() {
        OutboxRecord record = stored(OutboxMessage.builder()
                .aggregateId(AGGREGATE_ID).aggregateType(AGGREGATE_TYPE).unsequenced().payload(PAYLOAD).build());

        EncodedMessage encoded = encoder.encode(record);

        assertThat(encoded.headers().keySet()).containsExactly(RawHeaders.ID, RawHeaders.TYPE);
    }

    @Test
    void GIVEN_an_application_router_WHEN_a_row_is_published_raw_THEN_the_destination_is_what_the_router_names() {
        RawMessageEncoder routed = new RawMessageEncoder(record -> "orders." + record.aggregateType());

        assertThat(routed.encode(stored(order().build())).destination()).isEqualTo("orders.Order");
    }
}
