package com.codingful.tandem.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/**
 * Pins the write-side population shape a backlog measurement depends on (S11). The distinction these
 * assertions defend is not cosmetic: under a fixed cardinality every aggregate's chain grows without
 * bound, so the share of rows the head-of-chain gate leaves claimable falls as a backlog grows, and a
 * recovery measurement taken that way describes the generator rather than the relay. The lifecycle
 * population bounds chain length instead, and every property below is one a plausible-looking
 * implementation can lose while still producing ids that look fine in a log.
 */
class AggregateSelectorTest {

    private static final String NAMESPACE = "S11";
    private static final int QUOTA = 100;
    private static final double JITTER = 0.2;
    private static final int MIN_QUOTA = (int) Math.round(QUOTA * (1 - JITTER));
    private static final int MAX_QUOTA = (int) Math.round(QUOTA * (1 + JITTER));

    @Test
    void GIVEN_a_lifecycle_population_WHEN_an_aggregate_has_had_its_events_THEN_it_is_never_written_to_again() {
        int window = 16;
        List<String> emitted = emit(
                AggregateSelector.lifecycle(NAMESPACE, QUOTA, JITTER, window, 5_000), 100_000);

        Map<String, Integer> counts = countById(emitted);
        // An id used past its quota would mean a retired aggregate came back, which is exactly the
        // unbounded chain this population exists to prevent.
        assertThat(counts.values()).allSatisfy(count -> assertThat(count).isLessThanOrEqualTo(MAX_QUOTA));

        // Only the aggregates still active at the end may be short of their quota, and there are at
        // most `window` of those.
        long unfinished = counts.values().stream().filter(count -> count < MIN_QUOTA).count();
        assertThat(unfinished).isLessThanOrEqualTo(window);
    }

    @Test
    void GIVEN_a_jittered_quota_WHEN_many_aggregates_retire_THEN_their_lengths_vary_inside_the_band() {
        int window = 16;
        List<String> emitted = emit(
                AggregateSelector.lifecycle(NAMESPACE, QUOTA, JITTER, window, 5_000), 100_000);

        Set<Integer> retiredLengths = Set.copyOf(retiredCounts(emitted, window).values());

        assertThat(retiredLengths).hasSizeGreaterThan(1);
        assertThat(retiredLengths).allSatisfy(length -> assertThat(length).isBetween(MIN_QUOTA, MAX_QUOTA));
    }

    @Test
    void GIVEN_a_fixed_quota_WHEN_many_aggregates_retire_THEN_every_length_is_exactly_the_quota() {
        int window = 4;
        List<String> emitted = emit(
                AggregateSelector.lifecycle(NAMESPACE, 10, 0, window, 5_000), 10_000);

        assertThat(Set.copyOf(retiredCounts(emitted, window).values())).containsExactly(10);
    }

    @Test
    void GIVEN_a_lifecycle_population_WHEN_load_begins_THEN_only_the_active_set_receives_events() {
        int window = 16;
        List<String> emitted = emit(
                AggregateSelector.lifecycle(NAMESPACE, QUOTA, JITTER, window, 5_000), 500);

        // 500 events cannot retire an aggregate whose quota is at least 80, so the whole prefix must
        // land inside the initial active set.
        assertThat(Set.copyOf(emitted)).hasSizeLessThanOrEqualTo(window);
    }

    @Test
    void GIVEN_a_lifecycle_population_WHEN_events_are_generated_THEN_one_aggregate_s_events_are_spread_out() {
        int window = 16;
        List<String> emitted = emit(
                AggregateSelector.lifecycle(NAMESPACE, QUOTA, JITTER, window, 5_000), 20_000);

        long consecutive = 0;
        for (int i = 1; i < emitted.size(); i++) {
            if (emitted.get(i).equals(emitted.get(i - 1))) {
                consecutive++;
            }
        }
        // A generator that filled one aggregate before moving to the next would sit near 1.0 here,
        // and would produce dense chains rather than the interleaved ones a real write side makes.
        assertThat((double) consecutive / emitted.size()).isLessThan(0.25);
    }

    @Test
    void GIVEN_an_id_space_that_runs_out_WHEN_another_aggregate_must_start_THEN_it_fails_loudly() {
        AggregateSelector selector = AggregateSelector.lifecycle(NAMESPACE, 1, 0, 2, 3);

        assertThatThrownBy(() -> emit(selector, 100))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("id space exhausted")
                .hasMessageContaining("universeSizeFor");
    }

    @Test
    void GIVEN_an_id_space_sized_for_a_run_WHEN_the_whole_run_is_generated_THEN_it_does_not_run_out() {
        int window = 64;
        int events = 100_000;
        int universe = AggregateSelector.universeSizeFor(events, QUOTA, JITTER, window);

        List<String> emitted = emit(
                AggregateSelector.lifecycle(NAMESPACE, QUOTA, JITTER, window, universe), events);

        assertThat(emitted).hasSize(events);
    }

    @Test
    void GIVEN_a_lifecycle_population_WHEN_its_universe_is_seeded_THEN_every_id_it_can_produce_is_included() {
        AggregateSelector selector = AggregateSelector.lifecycle(NAMESPACE, 2, 0, 2, 40);

        List<String> emitted = emit(selector, 60);

        assertThat(selector.universe()).containsAll(emitted);
    }

    @Test
    void GIVEN_arguments_outside_their_range_WHEN_a_population_is_built_THEN_it_is_rejected() {
        assertThatThrownBy(() -> AggregateSelector.lifecycle(NAMESPACE, 0, JITTER, 4, 100))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("eventsPerAggregate");
        assertThatThrownBy(() -> AggregateSelector.lifecycle(NAMESPACE, QUOTA, 1.0, 4, 100))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("quotaJitter");
        assertThatThrownBy(() -> AggregateSelector.lifecycle(NAMESPACE, QUOTA, -0.1, 4, 100))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("quotaJitter");
        assertThatThrownBy(() -> AggregateSelector.lifecycle(NAMESPACE, QUOTA, JITTER, 0, 100))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("concurrentAggregates");
        assertThatThrownBy(() -> AggregateSelector.lifecycle(NAMESPACE, QUOTA, JITTER, 10, 9))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("universeSize");
    }

    private static List<String> emit(AggregateSelector selector, int count) {
        List<String> emitted = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            emitted.add(selector.nextAggregateId());
        }
        return emitted;
    }

    private static Map<String, Integer> countById(List<String> emitted) {
        Map<String, Integer> counts = new HashMap<>();
        emitted.forEach(id -> counts.merge(id, 1, Integer::sum));
        return counts;
    }

    /**
     * Counts for aggregates that certainly retired, so a partially filled active one can never be
     * mistaken for a short quota. Ids are handed out in increasing order and only the last
     * {@code window} of them can still be active, so everything below that is finished. Filtering by
     * "count looks big enough" instead is what let a build with the jitter removed pass: the active
     * aggregates supplied the variation the assertion was looking for.
     */
    private static Map<String, Integer> retiredCounts(List<String> emitted, int window) {
        Map<String, Integer> counts = countById(emitted);
        int highest = counts.keySet().stream().mapToInt(AggregateSelectorTest::indexOf).max().orElseThrow();
        Map<String, Integer> retired = new HashMap<>();
        counts.forEach((id, count) -> {
            if (indexOf(id) <= highest - window) {
                retired.put(id, count);
            }
        });
        assertThat(retired).as("the run must retire enough aggregates to assert on").isNotEmpty();
        return retired;
    }

    private static int indexOf(String aggregateId) {
        return Integer.parseInt(aggregateId.substring(aggregateId.lastIndexOf('-') + 1));
    }
}
