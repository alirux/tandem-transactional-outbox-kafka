package com.codingful.tandem.core.port;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxStatus;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;

class OutboxStoreDefaultTest {

    private static OutboxRecord claimed(long id) {
        return OutboxRecord.builder().id(id)
                .message(OutboxMessage.builder().aggregateId("order-" + id).aggregateType("Order").seq(1)
                        .payload(new byte[0]).build())
                .status(OutboxStatus.IN_FLIGHT).createdAt(Instant.EPOCH).build();
    }

    @Test
    void GIVEN_multiple_acked_rows_WHEN_marked_done_in_batch_THEN_each_is_marked_once_in_order() {
        List<Long> markedDone = new ArrayList<>();

        OutboxStore store = new OutboxStore() {
            @Override
            public List<OutboxRecord> claimBatch(Set<Integer> buckets, String workerId, Duration lease, int batchSize) {
                return List.of();
            }

            @Override
            public void markDone(OutboxRecord claimed) {
                markedDone.add(claimed.id());
            }

            @Override
            public void markForRetry(OutboxRecord claimed, String error, Duration retryDelay) {
            }

            @Override
            public void markFailed(OutboxRecord claimed, String error) {
            }

            @Override
            public int reclaimExpiredLeases() {
                return 0;
            }

            @Override
            public int cleanup(Instant doneBefore, int batchSize) {
                return 0;
            }
        };

        store.markDoneBatch(List.of(claimed(10), claimed(20), claimed(30)));

        assertThat(markedDone).containsExactly(10L, 20L, 30L);
    }

    @Test
    void GIVEN_an_empty_batch_WHEN_marked_done_THEN_no_row_is_marked() {
        List<Long> markedDone = new ArrayList<>();

        OutboxStore store = new OutboxStore() {
            @Override
            public List<OutboxRecord> claimBatch(Set<Integer> b, String w, Duration l, int s) { return List.of(); }
            @Override public void markDone(OutboxRecord claimed) { markedDone.add(claimed.id()); }
            @Override public void markForRetry(OutboxRecord claimed, String error, Duration retryDelay) {}
            @Override public void markFailed(OutboxRecord claimed, String error) {}
            @Override public int reclaimExpiredLeases() { return 0; }
            @Override public int cleanup(Instant doneBefore, int batchSize) { return 0; }
        };

        store.markDoneBatch(List.of());

        assertThat(markedDone).isEmpty();
    }
}
