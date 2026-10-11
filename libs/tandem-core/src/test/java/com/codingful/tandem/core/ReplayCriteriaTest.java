package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.HashSet;
import java.util.Set;
import org.junit.jupiter.api.Test;

class ReplayCriteriaTest {

    @Test
    void GIVEN_no_selector_WHEN_constructed_THEN_it_is_rejected() {
        assertThatThrownBy(() -> new ReplayCriteria(null, null, null, null, Set.of(), false))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_null_status_set_WHEN_constructed_with_another_selector_THEN_it_defaults_to_empty() {
        ReplayCriteria criteria = new ReplayCriteria(null, "Order", null, null, null, true);

        assertThat(criteria.statuses()).isEmpty();
        assertThat(criteria.dryRun()).isTrue();
    }

    @Test
    void GIVEN_only_a_status_selector_WHEN_constructed_THEN_it_is_accepted() {
        ReplayCriteria criteria =
                new ReplayCriteria(null, null, null, null, Set.of(OutboxStatus.FAILED), false);

        assertThat(criteria.statuses()).containsExactly(OutboxStatus.FAILED);
    }

    @Test
    void GIVEN_only_an_upper_id_bound_WHEN_constructed_THEN_it_is_accepted_as_the_selector() {
        ReplayCriteria criteria = new ReplayCriteria(null, null, null, 20L, Set.of(), false);

        assertThat(criteria.toId()).isEqualTo(20L);
    }

    @Test
    void GIVEN_a_status_set_WHEN_changed_after_construction_THEN_the_criteria_keep_the_original_statuses() {
        Set<OutboxStatus> statuses = new HashSet<>(Set.of(OutboxStatus.FAILED));
        ReplayCriteria criteria = new ReplayCriteria(null, null, null, null, statuses, false);

        statuses.add(OutboxStatus.DONE);

        assertThat(criteria.statuses()).containsExactly(OutboxStatus.FAILED);
    }

    @Test
    void GIVEN_every_selector_WHEN_built_THEN_each_reaches_the_criteria() {
        AggregateId aggregateId = AggregateId.of("order-1");
        String aggregateType = "Order";
        Set<OutboxStatus> statuses = Set.of(OutboxStatus.FAILED, OutboxStatus.DONE);

        ReplayCriteria criteria = ReplayCriteria.builder()
                .aggregateId(aggregateId)
                .aggregateType(aggregateType)
                .fromId(10L)
                .toId(20L)
                .statuses(statuses)
                .dryRun(true)
                .build();

        assertThat(criteria).isEqualTo(new ReplayCriteria(aggregateId, aggregateType, 10L, 20L, statuses, true));
    }

    @Test
    void GIVEN_only_an_id_range_WHEN_built_THEN_nothing_else_is_selected_and_it_is_not_a_dry_run() {
        ReplayCriteria criteria = ReplayCriteria.builder().fromId(7L).toId(7L).build();

        assertThat(criteria).isEqualTo(new ReplayCriteria(null, null, 7L, 7L, Set.of(), false));
    }

    @Test
    void GIVEN_no_selector_WHEN_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> ReplayCriteria.builder().dryRun(true).build())
                .isInstanceOf(IllegalArgumentException.class);
    }
}
