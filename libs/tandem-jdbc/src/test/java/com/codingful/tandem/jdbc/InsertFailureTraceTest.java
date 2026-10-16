package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * The {@code TRACE} lines a failed insert writes (LLD-jdbc §2) for the cases PostgreSQL does not produce
 * in {@link JdbcInsertFailureDisclosureIT}: a driver worded differently, a failure outside any batch, and
 * an event with no number.
 */
class InsertFailureTraceTest {

    private static final String SENSITIVE_PAYLOAD = "SENSITIVE-PAYLOAD";
    private static final String SENSITIVE_HEADER = "SENSITIVE-HEADER";

    private static OutboxMessage unnumberedEvent() {
        return OutboxMessage.builder().aggregateId("order-unnumbered").aggregateType("Order").unsequenced()
                .header("x-customer-email", SENSITIVE_HEADER)
                .payload(SENSITIVE_PAYLOAD.getBytes(StandardCharsets.UTF_8)).build();
    }

    @Test
    void GIVEN_a_driver_that_words_its_failure_differently_WHEN_traced_THEN_what_it_cannot_tell_is_unknown_and_nothing_it_quotes_leaks() {
        SQLException failure = new SQLException("Insert rejected: values (" + SENSITIVE_PAYLOAD + ")", "22P02", 7);
        OutboxMessage event = unnumberedEvent();

        List<String> trace = JdbcOutboxRepository.failureTrace(failure, List.of(event));

        assertThat(trace.get(0)).contains("sqlState:22P02").contains("vendorCode:7")
                .contains("batchEntry:?").contains("constraint:?");
        assertThat(trace.get(1)).contains("seq:" + event.seqSource()).contains("headerNames:[x-customer-email]");
        assertThat(String.join("\n", trace)).doesNotContain(SENSITIVE_PAYLOAD).doesNotContain(SENSITIVE_HEADER);
    }

    @Test
    void GIVEN_a_failure_outside_any_batch_WHEN_traced_THEN_a_single_line_says_the_events_are_unknown() {
        List<String> trace = JdbcOutboxRepository.failureTrace(new SQLException(null, "53000"), null);

        assertThat(trace).singleElement().asString().contains("sqlState:53000").contains("events:?");
    }
}
