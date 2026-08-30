package com.codingful.tandem.benchmark;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.Map;
import org.junit.jupiter.api.Test;

/**
 * Pins the bounded correctness bookkeeping an endurance run depends on (S9). Every assertion here
 * exists because the alternative — the per-event set-diff the shorter scenarios use — is exact by
 * construction, whereas this re-derives the same answers from one watermark per aggregate: if the
 * derivation is wrong, a run reports zero loss because it cannot see loss, which is the one way a
 * correctness check fails silently.
 */
class SequenceLedgerTest {

    private static final String ORDER = "S9-order-1";
    private static final String OTHER = "S9-order-2";

    @Test
    void GIVEN_every_event_arrives_in_order_WHEN_reconciled_against_the_writes_THEN_nothing_is_lost() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 1, 2, 3);
        deliver(ledger, OTHER, 1, 2);

        assertThat(ledger.lostEvents(Map.of(ORDER, 3L, OTHER, 2L))).isZero();
        assertThat(ledger.orderingViolations()).isZero();
        assertThat(ledger.duplicates()).isZero();
        assertThat(ledger.aggregatesSeen()).isEqualTo(2);
    }

    @Test
    void GIVEN_an_event_never_arrives_WHEN_a_later_one_does_THEN_the_hole_counts_as_lost() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 1, 4);   // 2 and 3 never arrive

        assertThat(ledger.gaps()).isEqualTo(2);
        assertThat(ledger.lostEvents(Map.of(ORDER, 4L))).isEqualTo(2);
    }

    @Test
    void GIVEN_the_first_events_of_an_aggregate_never_arrive_THEN_they_count_as_lost() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 3, 4);   // 1 and 2 never arrive, and nothing earlier was ever seen

        assertThat(ledger.lostEvents(Map.of(ORDER, 4L))).isEqualTo(2);
    }

    @Test
    void GIVEN_delivery_stops_before_the_last_writes_THEN_the_missing_tail_counts_as_lost() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 1, 2);

        assertThat(ledger.gaps()).isZero();
        assertThat(ledger.lostEvents(Map.of(ORDER, 5L))).isEqualTo(3);
    }

    @Test
    void GIVEN_an_aggregate_whose_events_never_arrive_at_all_THEN_its_whole_sequence_counts_as_lost() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 1, 2, 3);

        assertThat(ledger.lostEvents(Map.of(ORDER, 3L, OTHER, 4L))).isEqualTo(4);
    }

    @Test
    void GIVEN_an_event_is_redelivered_THEN_it_is_a_duplicate_and_not_an_ordering_violation() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 1, 2, 2, 3);

        assertThat(ledger.duplicates()).isEqualTo(1);
        assertThat(ledger.orderingViolations()).isZero();
        assertThat(ledger.lostEvents(Map.of(ORDER, 3L))).isZero();
    }

    @Test
    void GIVEN_an_event_arrives_after_a_later_one_THEN_it_is_an_ordering_violation_and_the_watermark_holds() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 1, 3, 2);

        assertThat(ledger.orderingViolations()).isEqualTo(1);
        assertThat(ledger.violationSamples()).hasSize(1).first(org.assertj.core.api.InstanceOfAssertFactories.STRING)
                .contains(ORDER);
        // The out-of-order arrival must not drag the watermark back to 2 — doing so would then read
        // event 3, already delivered, as a missing tail.
        assertThat(ledger.lostEvents(Map.of(ORDER, 3L))).isEqualTo(1);   // only the skipped 2, counted as a gap
    }

    @Test
    void GIVEN_far_more_violations_than_the_sample_cap_THEN_only_a_bounded_sample_is_kept() {
        SequenceLedger ledger = new SequenceLedger();
        ledger.record(ORDER, 10_000);
        for (int i = 0; i < 500; i++) {
            ledger.record(ORDER, 1);
        }

        assertThat(ledger.orderingViolations()).isEqualTo(500);
        assertThat(ledger.violationSamples()).hasSize(20);
    }

    @Test
    void GIVEN_delivery_is_still_behind_the_writes_THEN_it_is_not_yet_caught_up() {
        SequenceLedger ledger = new SequenceLedger();
        deliver(ledger, ORDER, 1, 2);

        assertThat(ledger.caughtUpWith(Map.of(ORDER, 3L))).isFalse();

        deliver(ledger, ORDER, 3);
        assertThat(ledger.caughtUpWith(Map.of(ORDER, 3L))).isTrue();
    }

    private static void deliver(SequenceLedger ledger, String aggregateId, long... seqs) {
        for (long seq : seqs) {
            ledger.record(aggregateId, seq);
        }
    }
}
