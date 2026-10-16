package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.catchThrowable;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.exception.OutboxInsertException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.charset.StandardCharsets;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

/**
 * What a failed insert lets out: the exception an application logs, and the repository's own TRACE
 * lines. Neither may carry an event's payload or header values (HLD-logging), whatever the driver's own
 * message quotes (LLD-jdbc §2).
 */
class JdbcInsertFailureDisclosureIT extends AbstractPostgresIT {

    private static final String SENSITIVE_PAYLOAD = "SENSITIVE-PAYLOAD";
    private static final String SENSITIVE_HEADER = "SENSITIVE-HEADER";
    private static final String SQLSTATE_UNIQUE_VIOLATION = "23505";

    private final JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);

    private static OutboxMessage event(String aggregateId, String payload) {
        return OutboxMessage.builder().aggregateId(aggregateId).aggregateType("Order").seq(1)
                .header("x-customer-email", SENSITIVE_HEADER)
                .payload(payload.getBytes(StandardCharsets.UTF_8)).build();
    }

    private static OutboxMessage validEvent(String aggregateId) {
        return event(aggregateId, "{\"secret\":\"" + SENSITIVE_PAYLOAD + "\"}");
    }

    private static OutboxMessage invalidJsonEvent(String aggregateId) {
        return event(aggregateId, SENSITIVE_PAYLOAD + " is not json");
    }

    /** Everything a logger prints for the exception: every message in the cause chain, and the traces. */
    private static String asLogged(Throwable failure) {
        StringWriter out = new StringWriter();
        failure.printStackTrace(new PrintWriter(out));
        return out.toString();
    }

    @Test
    void GIVEN_an_event_whose_payload_the_database_rejects_WHEN_inserted_THEN_the_failure_quotes_none_of_it() {
        Throwable failure = catchThrowable(() -> repository.insert(invalidJsonEvent("order-invalid")));

        assertThat(failure).isExactlyInstanceOf(OutboxInsertException.class);
        assertThat(asLogged(failure)).doesNotContain(SENSITIVE_PAYLOAD).doesNotContain(SENSITIVE_HEADER);
    }

    @Test
    void GIVEN_a_batch_with_a_repeated_number_WHEN_inserted_THEN_the_failure_quotes_no_event_but_keeps_the_sql_state() {
        OutboxMessage stored = validEvent("order-stored");
        repository.insert(stored);

        Throwable failure = catchThrowable(() -> repository.insertAll(List.of(validEvent("order-new"), stored)));

        assertThat(failure).isInstanceOf(DuplicateSeqException.class)
                .hasMessageContaining(SQLSTATE_UNIQUE_VIOLATION);
        assertThat(failure.getCause()).isInstanceOf(SQLException.class);
        assertThat(((SQLException) failure.getCause()).getSQLState()).isEqualTo(SQLSTATE_UNIQUE_VIOLATION);
        assertThat(asLogged(failure)).doesNotContain(SENSITIVE_PAYLOAD).doesNotContain(SENSITIVE_HEADER);
    }

    @Test
    void GIVEN_a_batch_with_a_payload_the_database_rejects_WHEN_inserted_THEN_the_failure_quotes_none_of_it() {
        Throwable failure = catchThrowable(() -> repository.insertAll(
                List.of(validEvent("order-valid"), invalidJsonEvent("order-invalid"))));

        assertThat(failure).isExactlyInstanceOf(OutboxInsertException.class);
        assertThat(asLogged(failure)).doesNotContain(SENSITIVE_PAYLOAD).doesNotContain(SENSITIVE_HEADER);
    }

    @Test
    void GIVEN_trace_logging_WHEN_a_batch_fails_THEN_the_failed_entry_maps_to_its_event_without_its_content() {
        OutboxMessage stored = validEvent("order-stored");
        repository.insert(stored);
        OutboxMessage fresh = validEvent("order-new");

        List<String> trace = traceWhile(() -> catchThrowable(() -> repository.insertAll(List.of(fresh, stored))));

        assertThat(trace).anySatisfy(line -> assertThat(line).contains("batchEntry:1")
                .contains("constraint:tandem_outbox_aggregate_id_seq_key"));
        assertThat(trace).anySatisfy(line -> assertThat(line).contains("batchEntry:1")
                .contains("aggregateId:" + stored.aggregateId().value())
                .contains("payloadBytes:" + stored.payload().length));
        assertThat(String.join("\n", trace)).doesNotContain(SENSITIVE_PAYLOAD).doesNotContain(SENSITIVE_HEADER);
    }

    /** What the repository logged at TRACE (JUL's FINER) through the JDK's own logging while {@code body} ran. */
    private static List<String> traceWhile(Runnable body) {
        List<String> lines = new CopyOnWriteArrayList<>();
        Handler collector = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getLevel() == Level.FINER) {
                    lines.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        collector.setLevel(Level.ALL);
        Logger logger = Logger.getLogger(JdbcOutboxRepository.class.getName());
        Level previous = logger.getLevel();
        logger.setLevel(Level.ALL);
        logger.addHandler(collector);
        try {
            body.run();
        } finally {
            logger.removeHandler(collector);
            logger.setLevel(previous);
        }
        return lines;
    }
}
