package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Test;

class JdbcForwardCompatibleHeadersIT extends AbstractPostgresIT {

    @Test
    void GIVEN_a_header_another_writer_stored_as_an_array_WHEN_the_relay_claims_a_batch_THEN_every_aggregate_in_it_is_returned() {
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);
        for (String aggregate : new String[] {"array-header", "healthy"}) {
            repository.insert(OutboxMessage.builder().aggregateId(aggregate).aggregateType("Order")
                    .unsequenced().payload("{}".getBytes(StandardCharsets.UTF_8)).build());
        }
        execute("UPDATE tandem_outbox SET headers = '{\"tags\":[\"a\",\"b\"]}'::jsonb WHERE id = 1");
        Set<Integer> buckets = IntStream.range(0, 256).boxed().collect(Collectors.toSet());
        JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

        assertThat(store.claimBatch(buckets, "worker", Duration.ofSeconds(60), 10)).hasSize(2);
    }

    @Test
    void GIVEN_an_unknown_nullable_header_WHEN_the_relay_claims_a_batch_THEN_both_its_event_and_other_aggregates_can_be_delivered() {
        JdbcOutboxRepository repository = new JdbcOutboxRepository(DATA_SOURCE, 256);
        for (String aggregate : new String[] {"nullable-header", "healthy"}) {
            repository.insert(OutboxMessage.builder().aggregateId(aggregate).aggregateType("Order")
                    .unsequenced().payload("{}".getBytes(StandardCharsets.UTF_8)).build());
        }
        // A newer writer can add an optional header without changing the existing event contract.
        execute("UPDATE tandem_outbox SET headers = '{\"future-optional-header\":null}'::jsonb WHERE id = 1");
        Set<Integer> buckets = IntStream.range(0, 256).boxed().collect(Collectors.toSet());
        JdbcOutboxStore store = new JdbcOutboxStore(DATA_SOURCE, 10);

        var claimed = store.claimBatch(buckets, "worker", Duration.ofSeconds(60), 10);

        assertThat(claimed).hasSize(2);
        store.markDoneBatch(claimed);
        assertThat(store.lag().orElseThrow().pending()).isZero();
    }
}
