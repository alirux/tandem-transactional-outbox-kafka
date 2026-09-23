package com.codingful.tandem.test.encoder;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.test.MessageEncoderContract;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class DebeziumStyleEncoderTest {

    private static final long ROW_ID = 42;

    private final DebeziumStyleEncoder encoder = new DebeziumStyleEncoder();

    private static OutboxRecord row(OutboxMessage.Builder message) {
        return OutboxRecord.builder().id(ROW_ID).createdAt(Instant.EPOCH).message(message
                .aggregateId("order-7").aggregateType("OrderLine").unsequenced()
                .payload("{}".getBytes(StandardCharsets.UTF_8)).build()).build();
    }

    @Test
    void GIVEN_the_worked_example_WHEN_it_is_held_to_the_encoder_contract_THEN_it_keeps_every_invariant() {
        MessageEncoderContract contract = MessageEncoderContract
                .of(encoder, MessageEncoderContract.header(DebeziumStyleEncoder.ID_HEADER))
                .consumesHeaders(DebeziumStyleEncoder.ID_HEADER);

        assertThatCode(contract::verify).doesNotThrowAnyException();
    }

    @Test
    void GIVEN_an_aggregate_type_WHEN_the_example_routes_it_THEN_the_topic_is_the_debezium_default_route_with_the_type_verbatim() {
        assertThat(encoder.encode(row(OutboxMessage.builder())).destination()).isEqualTo("outbox.event.OrderLine");
    }

    @Test
    void GIVEN_a_row_header_named_like_the_examples_id_WHEN_encoded_THEN_the_row_id_wins() {
        EncodedMessage encoded = encoder.encode(row(OutboxMessage.builder().header(DebeziumStyleEncoder.ID_HEADER, "forged")));

        assertThat(new String(encoded.headers().get(DebeziumStyleEncoder.ID_HEADER), StandardCharsets.UTF_8))
                .isEqualTo(String.valueOf(ROW_ID));
    }
}
