package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.MessageEncoder;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;

class MessageEncoderContractTest {

    private static final String ID_HEADER = "event-id";
    private static final Function<EncodedMessage, String> EVENT_ID = MessageEncoderContract.header(ID_HEADER);

    /** A compliant envelope: the row id as event id, the aggregate as key, every row header passed through. */
    private static EncodedMessage compliant(OutboxRecord record) {
        Map<String, byte[]> headers = rowHeaders(record);
        headers.put(ID_HEADER, utf8(String.valueOf(record.id())));
        return new EncodedMessage("orders", record.aggregateId().value(), record.payload(), headers);
    }

    private static Map<String, byte[]> rowHeaders(OutboxRecord record) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        record.headers().forEach((name, value) -> headers.put(name, utf8(value)));
        return headers;
    }

    private static EncodedMessage withHeader(EncodedMessage message, String name, String value) {
        Map<String, byte[]> headers = new LinkedHashMap<>(message.headers());
        headers.put(name, utf8(value));
        return new EncodedMessage(message.destination(), message.key(), message.body(), headers);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static OutboxRecord row(long id) {
        return OutboxRecord.builder().id(id).createdAt(Instant.EPOCH).message(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").unsequenced().payload(new byte[] {1}).build()).build();
    }

    /** Asserts the run fails, reporting exactly the given checks and no other. */
    private static void assertFailsOnly(MessageEncoderContract contract, Integer... checks) {
        Set<Integer> expected = Set.of(checks);
        assertThatThrownBy(contract::verify)
                .isInstanceOf(AssertionError.class)
                .satisfies(failure -> {
                    for (int check = 1; check <= 6; check++) {
                        assertThat(failure.getMessage().contains("Check " + check + " "))
                                .as("check %d reported", check)
                                .isEqualTo(expected.contains(check));
                    }
                });
    }

    @Test
    void GIVEN_an_encoder_keeping_every_invariant_WHEN_it_is_held_to_the_contract_THEN_it_passes() {
        assertThatCode(() -> MessageEncoderContract.of(MessageEncoderContractTest::compliant, EVENT_ID).verify())
                .doesNotThrowAnyException();
    }

    @Test
    void GIVEN_an_encoder_keying_by_aggregate_type_WHEN_it_is_held_to_the_contract_THEN_the_ordering_key_check_fails() {
        MessageEncoder byType = record -> {
            EncodedMessage message = compliant(record);
            return new EncodedMessage(message.destination(), record.aggregateType(), message.body(), message.headers());
        };

        assertFailsOnly(MessageEncoderContract.of(byType, EVENT_ID), 5);
    }

    @Test
    void GIVEN_an_encoder_keying_by_aggregate_type_WHEN_it_declares_its_own_key_THEN_it_passes() {
        MessageEncoder byType = record -> {
            EncodedMessage message = compliant(record);
            return new EncodedMessage(message.destination(), record.aggregateType(), message.body(), message.headers());
        };

        assertThatCode(() -> MessageEncoderContract.of(byType, EVENT_ID)
                .declaresOwnOrderingKey("events of one type are ordered together on purpose")
                .verify())
                .doesNotThrowAnyException();
    }

    @Test
    void GIVEN_an_encoder_dropping_row_headers_WHEN_it_is_held_to_the_contract_THEN_the_passthrough_check_fails() {
        MessageEncoder dropping = record -> new EncodedMessage("orders", record.aggregateId().value(), record.payload(),
                new LinkedHashMap<>(Map.of(ID_HEADER, utf8(String.valueOf(record.id())))));

        assertFailsOnly(MessageEncoderContract.of(dropping, EVENT_ID), 6);
    }

    @Test
    void GIVEN_an_encoder_dropping_row_headers_WHEN_it_declares_them_consumed_THEN_it_passes() {
        Set<String> dropped = new LinkedHashSet<>();
        MessageEncoder dropping = record -> {
            dropped.addAll(record.headers().keySet());
            return new EncodedMessage("orders", record.aggregateId().value(), record.payload(),
                    new LinkedHashMap<>(Map.of(ID_HEADER, utf8(String.valueOf(record.id())))));
        };
        assertFailsOnly(MessageEncoderContract.of(dropping, EVENT_ID), 6);

        assertThatCode(() -> MessageEncoderContract.of(dropping, EVENT_ID)
                .consumesHeaders(dropped.toArray(String[]::new))
                .verify())
                .doesNotThrowAnyException();
    }

    @Test
    void GIVEN_an_encoder_rewriting_a_row_header_WHEN_it_is_held_to_the_contract_THEN_the_passthrough_check_fails() {
        MessageEncoder rewriting = record -> {
            EncodedMessage message = compliant(record);
            String first = record.headers().keySet().iterator().next();
            return withHeader(message, first, "rewritten");
        };

        assertFailsOnly(MessageEncoderContract.of(rewriting, EVENT_ID), 6);
    }

    @Test
    void GIVEN_an_encoder_minting_a_random_id_per_encode_WHEN_it_is_held_to_the_contract_THEN_determinism_and_id_stability_fail() {
        MessageEncoder random = record -> withHeader(compliant(record), ID_HEADER, UUID.randomUUID().toString());

        assertFailsOnly(MessageEncoderContract.of(random, EVENT_ID), 2, 3);
    }

    @Test
    void GIVEN_an_encoder_whose_destination_key_and_body_change_per_call_WHEN_it_is_held_to_the_contract_THEN_the_determinism_check_names_all_three() {
        AtomicInteger calls = new AtomicInteger();
        MessageEncoder drifting = record -> {
            EncodedMessage message = compliant(record);
            int call = calls.incrementAndGet();
            return new EncodedMessage("orders-" + call, message.key() + call, new byte[] {(byte) call}, message.headers());
        };

        assertThatThrownBy(MessageEncoderContract.of(drifting, EVENT_ID)::verify)
                .hasMessageContaining("Check 2 ")
                .hasMessageContaining("destination")
                .hasMessageContaining("key")
                .hasMessageContaining("body");
    }

    @Test
    void GIVEN_an_event_id_reader_that_throws_WHEN_the_contract_runs_THEN_the_id_check_fails_with_the_cause_attached() {
        Function<EncodedMessage, String> broken = message -> {
            throw new IllegalStateException("no id here");
        };

        assertThatThrownBy(MessageEncoderContract.of(MessageEncoderContractTest::compliant, broken)::verify)
                .hasMessageContaining("Check 3 ")
                .satisfies(failure -> assertThat(failure.getSuppressed())
                        .isNotEmpty()
                        .allMatch(IllegalStateException.class::isInstance));
    }

    @Test
    void GIVEN_rows_of_the_callers_own_that_repeat_one_row_WHEN_the_contract_runs_THEN_the_repeat_is_not_taken_for_a_shared_id() {
        assertThatCode(() -> MessageEncoderContract.of(MessageEncoderContractTest::compliant, EVENT_ID)
                .records(List.of(row(1), row(1), row(2)))
                .verify())
                .doesNotThrowAnyException();
    }

    @Test
    void GIVEN_an_encoder_emitting_one_id_for_every_row_WHEN_it_is_held_to_the_contract_THEN_the_distinct_id_check_fails() {
        MessageEncoder constant = record -> withHeader(compliant(record), ID_HEADER, "the-event");

        assertFailsOnly(MessageEncoderContract.of(constant, EVENT_ID), 4);
    }

    @Test
    void GIVEN_an_encoder_emitting_no_id_WHEN_it_is_held_to_the_contract_THEN_the_id_presence_check_fails() {
        MessageEncoder anonymous = record -> new EncodedMessage("orders", record.aggregateId().value(), record.payload(),
                rowHeaders(record));

        assertFailsOnly(MessageEncoderContract.of(anonymous, EVENT_ID), 3);
    }

    @Test
    void GIVEN_an_encoder_emitting_a_blank_id_WHEN_it_is_held_to_the_contract_THEN_the_id_presence_check_fails() {
        MessageEncoder blank = record -> withHeader(compliant(record), ID_HEADER, " ");

        assertFailsOnly(MessageEncoderContract.of(blank, EVENT_ID), 3);
    }

    @Test
    void GIVEN_an_encoder_stamping_the_current_time_WHEN_it_is_held_to_the_contract_THEN_the_determinism_check_fails() {
        MessageEncoder clocked = record -> withHeader(compliant(record), "sent-at", String.valueOf(System.nanoTime()));

        assertFailsOnly(MessageEncoderContract.of(clocked, EVENT_ID), 2);
    }

    @Test
    void GIVEN_an_encoder_reading_the_sequence_unguarded_WHEN_it_is_held_to_the_contract_THEN_it_fails_on_the_unsequenced_row() {
        MessageEncoder sequenced = record -> withHeader(compliant(record), "seq", String.valueOf(record.seq()));

        assertThatThrownBy(MessageEncoderContract.of(sequenced, EVENT_ID)::verify)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Check 1 ")
                .hasMessageContaining("rowId:2,")
                .satisfies(failure -> assertThat(failure.getSuppressed()).singleElement()
                        .isInstanceOf(IllegalStateException.class));
    }

    @Test
    void GIVEN_an_encoder_reading_the_event_type_without_fallback_WHEN_it_is_held_to_the_contract_THEN_it_fails_on_the_untyped_row() {
        MessageEncoder typed = record -> {
            EncodedMessage message = compliant(record);
            return new EncodedMessage(message.destination(), message.key(), record.type().getBytes(StandardCharsets.UTF_8),
                    message.headers());
        };

        assertThatThrownBy(MessageEncoderContract.of(typed, EVENT_ID)::verify)
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("Check 1 ")
                .hasMessageContaining("rowId:3,")
                .hasMessageNotContaining("rowId:1,")
                .hasMessageNotContaining("rowId:2,");
    }

    @Test
    void GIVEN_an_encoder_returning_nothing_WHEN_it_is_held_to_the_contract_THEN_the_acceptance_check_fails() {
        assertFailsOnly(MessageEncoderContract.of(record -> null, EVENT_ID), 1);
    }

    @Test
    void GIVEN_an_encoder_breaking_several_invariants_WHEN_it_is_held_to_the_contract_THEN_one_run_reports_all_of_them() {
        MessageEncoder careless = record -> new EncodedMessage("orders", record.aggregateType(), record.payload(),
                new LinkedHashMap<>(Map.of(ID_HEADER, utf8("the-event"))));

        assertFailsOnly(MessageEncoderContract.of(careless, EVENT_ID), 4, 5, 6);
    }

    @Test
    void GIVEN_a_violation_WHEN_it_is_reported_THEN_it_names_the_header_but_never_a_value() {
        String rewrittenValue = "rewritten-value";
        Map<String, String> original = new LinkedHashMap<>();
        MessageEncoder rewriting = record -> {
            String name = record.headers().keySet().iterator().next();
            original.put(name, record.headers().get(name));
            return withHeader(compliant(record), name, rewrittenValue);
        };

        assertThatThrownBy(MessageEncoderContract.of(rewriting, EVENT_ID)::verify)
                .satisfies(failure -> {
                    original.forEach((name, value) -> assertThat(failure.getMessage())
                            .contains(name)
                            .doesNotContain(value));
                    assertThat(failure.getMessage()).doesNotContain(rewrittenValue);
                });
    }

    @Test
    void GIVEN_rows_of_the_callers_own_WHEN_the_contract_runs_THEN_it_checks_those_rows_instead_of_the_built_in_ones() {
        MessageEncoder onlyTheseRows = record -> {
            if (record.id() > 100) {
                return compliant(record);
            }
            throw new IllegalArgumentException("not a row of this test");
        };

        assertThatCode(() -> MessageEncoderContract.of(onlyTheseRows, EVENT_ID).records(List.of(row(101), row(102))).verify())
                .doesNotThrowAnyException();
    }

    @Test
    void GIVEN_rows_with_fewer_than_two_distinct_ids_WHEN_they_replace_the_fixtures_THEN_they_are_refused() {
        MessageEncoderContract contract = MessageEncoderContract.of(MessageEncoderContractTest::compliant, EVENT_ID);

        assertThatThrownBy(() -> contract.records(List.of(row(7), row(7))))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_blank_reason_WHEN_an_encoder_declares_its_own_ordering_key_THEN_the_declaration_is_refused() {
        MessageEncoderContract contract = MessageEncoderContract.of(MessageEncoderContractTest::compliant, EVENT_ID);

        assertThatThrownBy(() -> contract.declaresOwnOrderingKey(" "))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
