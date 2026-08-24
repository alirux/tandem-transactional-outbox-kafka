package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.SeqSource;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class PublishOrderWatermarksTest {

    private static final AggregateId ORDER_1 = AggregateId.of("order-1");
    private static final AggregateId ORDER_2 = AggregateId.of("order-2");
    private static final Instant NOW = Instant.parse("2026-01-01T00:00:00Z");

    private final PublishOrderWatermarks watermarks = new PublishOrderWatermarks();

    /** A row whose number the application chose — judged on that number. */
    private static OutboxRecord numbered(AggregateId aggregateId, long seq) {
        return numbered(aggregateId, seq, 1);
    }

    private static OutboxRecord numbered(AggregateId aggregateId, long seq, long id) {
        return OutboxRecord.builder()
                .id(id)
                .message(OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType("Order").seq(seq)
                        .payload(new byte[] {1}).build())
                .createdAt(NOW)
                .build();
    }

    /** A row that declared no ordering of its own — judged on its id. */
    private static OutboxRecord unsequenced(AggregateId aggregateId, long id) {
        return OutboxRecord.builder()
                .id(id)
                .message(OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType("Order").unsequenced()
                        .payload(new byte[] {1}).build())
                .createdAt(NOW)
                .build();
    }

    /** A row Tandem numbered: it has a seq, but insert order is all that number says. */
    private static OutboxRecord managed(AggregateId aggregateId, long seq, long id) {
        return OutboxRecord.builder()
                .id(id)
                .message(OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType("Order").seq(seq)
                        .payload(new byte[] {1}).build())
                .seqSource(SeqSource.MANAGED)
                .createdAt(NOW)
                .build();
    }

    // --- which ordering a row is judged on -------------------------------------------------------

    @Test
    void GIVEN_a_row_numbered_by_the_application_WHEN_it_is_judged_THEN_its_own_number_is_the_ordering() {
        OutboxRecord record = numbered(ORDER_1, 7, 99);

        assertThat(PublishOrderWatermarks.keyFor(record)).isEqualTo(PublishOrderWatermarks.Key.SEQ);
        assertThat(PublishOrderWatermarks.valueFor(record)).isEqualTo(7);
    }

    @Test
    void GIVEN_a_row_tandem_numbered_WHEN_it_is_judged_THEN_its_id_is_the_ordering_not_the_number() {
        // Its seq is insert order restated, so reading it would only import the sequence's own
        // hazards - a cached range, a botched restart - as violations that never happened.
        OutboxRecord record = managed(ORDER_1, 8347283, 99);

        assertThat(PublishOrderWatermarks.keyFor(record)).isEqualTo(PublishOrderWatermarks.Key.ID);
        assertThat(PublishOrderWatermarks.valueFor(record)).isEqualTo(99);
    }

    @Test
    void GIVEN_a_row_with_no_number_WHEN_it_is_judged_THEN_its_id_is_the_ordering() {
        OutboxRecord record = unsequenced(ORDER_1, 99);

        assertThat(PublishOrderWatermarks.keyFor(record)).isEqualTo(PublishOrderWatermarks.Key.ID);
        assertThat(PublishOrderWatermarks.valueFor(record)).isEqualTo(99);
    }

    @Test
    void GIVEN_a_row_whose_provenance_this_build_predates_WHEN_it_is_judged_THEN_it_falls_back_to_the_id() {
        OutboxRecord record = OutboxRecord.builder()
                .id(99)
                .message(OutboxMessage.builder()
                        .aggregateId(ORDER_1).aggregateType("Order").seq(7)
                        .payload(new byte[] {1}).build())
                .seqSource(SeqSource.UNKNOWN)
                .createdAt(NOW)
                .build();

        // The safe reading: under-reports, never invents. Trusting an unknown provenance's number as a
        // declared ordering is the one thing that could produce a violation that never happened.
        assertThat(PublishOrderWatermarks.keyFor(record)).isEqualTo(PublishOrderWatermarks.Key.ID);
    }

    // --- verdicts on the application's own numbering ----------------------------------------------

    @Test
    void GIVEN_an_aggregate_never_published_before_WHEN_its_first_event_goes_out_THEN_nothing_is_suspected() {
        assertThat(watermarks.record(numbered(ORDER_1, 7))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
    }

    @Test
    void GIVEN_events_published_in_order_WHEN_each_one_goes_out_THEN_nothing_is_suspected() {
        assertThat(watermarks.record(numbered(ORDER_1, 1))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.record(numbered(ORDER_1, 2))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.record(numbered(ORDER_1, 3))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
    }

    /** A gap is legitimate: not every domain event need pass through the outbox. */
    @Test
    void GIVEN_a_gap_in_the_sequence_WHEN_the_later_event_goes_out_THEN_nothing_is_suspected() {
        watermarks.record(numbered(ORDER_1, 1));

        assertThat(watermarks.record(numbered(ORDER_1, 50))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
    }

    /** At-least-once delivery: the same row re-published after a lease reclaim is not an ordering fault. */
    @Test
    void GIVEN_an_event_already_published_WHEN_the_very_same_one_goes_out_again_THEN_it_is_a_duplicate_not_a_regression() {
        watermarks.record(numbered(ORDER_1, 4));

        assertThat(watermarks.record(numbered(ORDER_1, 4))).isEqualTo(PublishOrderWatermarks.Verdict.DUPLICATE);
    }

    @Test
    void GIVEN_an_event_already_published_WHEN_an_earlier_one_goes_out_after_it_THEN_it_is_suspected() {
        watermarks.record(numbered(ORDER_1, 2));

        assertThat(watermarks.record(numbered(ORDER_1, 1))).isEqualTo(PublishOrderWatermarks.Verdict.REGRESSED);
    }

    @Test
    void GIVEN_two_aggregates_WHEN_one_goes_backwards_THEN_the_other_is_judged_on_its_own_history() {
        watermarks.record(numbered(ORDER_1, 9));
        watermarks.record(numbered(ORDER_2, 1));

        assertThat(watermarks.record(numbered(ORDER_2, 2))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.record(numbered(ORDER_1, 8))).isEqualTo(PublishOrderWatermarks.Verdict.REGRESSED);
    }

    /**
     * The trap HLD §8 names explicitly: had the suspected event lowered the watermark, everything after
     * it would read as an advance and the next genuine regression would go unseen.
     */
    @Test
    void GIVEN_an_event_went_backwards_WHEN_a_later_one_goes_backwards_too_THEN_it_is_still_suspected() {
        watermarks.record(numbered(ORDER_1, 10));
        watermarks.record(numbered(ORDER_1, 3));

        assertThat(watermarks.record(numbered(ORDER_1, 4))).isEqualTo(PublishOrderWatermarks.Verdict.REGRESSED);
        assertThat(watermarks.record(numbered(ORDER_1, 11))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
    }

    @Test
    void GIVEN_a_suspected_event_WHEN_the_report_asks_what_it_was_judged_against_THEN_the_watermark_still_holds_it() {
        watermarks.record(numbered(ORDER_1, 41));
        watermarks.record(numbered(ORDER_1, 7));

        // A REGRESSED verdict leaves the watermark untouched, which is what lets the ERROR name the
        // value the row lost to without a second lookup.
        assertThat(watermarks.lastPublished(ORDER_1))
                .isEqualTo(new PublishOrderWatermarks.Watermark(PublishOrderWatermarks.Key.SEQ, 41));
    }

    // --- verdicts on rows that declared no ordering -----------------------------------------------

    @Test
    void GIVEN_events_with_no_numbers_WHEN_a_later_written_one_is_published_first_THEN_it_is_suspected() {
        // The commit-order race, on an aggregate that publishes no sequence number at all: the row
        // written second commits first, so the earlier id goes out afterwards.
        watermarks.record(unsequenced(ORDER_1, 101));

        assertThat(watermarks.record(unsequenced(ORDER_1, 100)))
                .isEqualTo(PublishOrderWatermarks.Verdict.REGRESSED);
    }

    @Test
    void GIVEN_events_with_no_numbers_WHEN_published_in_the_order_they_were_written_THEN_nothing_is_suspected() {
        watermarks.record(unsequenced(ORDER_1, 100));

        assertThat(watermarks.record(unsequenced(ORDER_1, 101)))
                .isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
    }

    @Test
    void GIVEN_an_aggregate_mixing_its_own_numbering_with_tandems_WHEN_published_in_order_THEN_nothing_is_suspected() {
        // A large managed number against a small application one would look like a wild jump, and the
        // reverse like a regression, if the two were ever compared with each other.
        watermarks.record(numbered(ORDER_1, 3, 100));

        assertThat(watermarks.record(managed(ORDER_1, 8347283, 101)))
                .isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.record(numbered(ORDER_1, 4, 102)))
                .isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
    }

    @Test
    void GIVEN_an_aggregate_that_changed_how_it_orders_WHEN_the_first_row_of_the_new_kind_goes_out_THEN_it_is_not_judged() {
        watermarks.record(numbered(ORDER_1, 500, 100));

        // The transition row is the documented blind spot: there is nothing meaningful to compare it
        // against, so it is stored rather than suspected.
        assertThat(watermarks.record(unsequenced(ORDER_1, 101)))
                .isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
        assertThat(watermarks.lastPublished(ORDER_1))
                .isEqualTo(new PublishOrderWatermarks.Watermark(PublishOrderWatermarks.Key.ID, 101));
    }

    // --- bounding -------------------------------------------------------------------------------

    @Test
    void GIVEN_more_aggregates_than_the_cap_WHEN_they_all_publish_THEN_the_tracked_set_stays_bounded() {
        PublishOrderWatermarks bounded = new PublishOrderWatermarks(3);

        for (int i = 0; i < 100; i++) {
            bounded.record(numbered(AggregateId.of("order-" + i), 1));
        }

        assertThat(bounded.size()).isEqualTo(3);
    }

    /**
     * Eviction degrades detection exactly as a relay restart does, and the design accepts that — pinned
     * so the trade-off is a decision on record rather than a surprise.
     */
    @Test
    void GIVEN_an_aggregate_evicted_by_newer_traffic_WHEN_it_goes_backwards_THEN_the_regression_is_missed() {
        PublishOrderWatermarks bounded = new PublishOrderWatermarks(2);
        bounded.record(numbered(ORDER_1, 10));
        bounded.record(numbered(AggregateId.of("other-1"), 1));
        bounded.record(numbered(AggregateId.of("other-2"), 1));

        assertThat(bounded.record(numbered(ORDER_1, 2))).isEqualTo(PublishOrderWatermarks.Verdict.ADVANCED);
    }

    /** Recency is by publish, not by insertion: a busy aggregate must not be evicted by newcomers. */
    @Test
    void GIVEN_an_aggregate_kept_busy_WHEN_newer_aggregates_arrive_THEN_the_busy_one_survives_eviction() {
        PublishOrderWatermarks bounded = new PublishOrderWatermarks(2);
        bounded.record(numbered(ORDER_1, 10));
        bounded.record(numbered(AggregateId.of("other-1"), 1));
        bounded.record(numbered(ORDER_1, 11));
        bounded.record(numbered(AggregateId.of("other-2"), 1));

        assertThat(bounded.record(numbered(ORDER_1, 5))).isEqualTo(PublishOrderWatermarks.Verdict.REGRESSED);
    }
}
