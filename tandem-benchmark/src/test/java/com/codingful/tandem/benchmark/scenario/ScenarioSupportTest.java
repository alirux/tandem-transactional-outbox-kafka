package com.codingful.tandem.benchmark.scenario;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.benchmark.RampController;
import org.junit.jupiter.api.Test;

/**
 * Pins how a scenario describes the load it held. The two cases below produce the same number and
 * mean opposite things: a scenario holding half of a measured ceiling is doing what it says, and one
 * holding half of a figure the search budget happened to reach is not — a difference that is invisible
 * in an archived result line unless the line names it.
 */
class ScenarioSupportTest {

    @Test
    void GIVEN_a_search_that_found_a_rate_the_host_could_not_hold_WHEN_the_load_is_described_THEN_it_reads_as_a_ceiling() {
        RampController.RampResult bracketed = new RampController.RampResult(1450, 1500);

        String described = ScenarioSupport.RateBasis.of(bracketed).describe(0.5);

        assertThat(described).isEqualTo("50% of a bracketed ceiling");
    }

    @Test
    void GIVEN_a_search_that_never_failed_WHEN_the_load_is_described_THEN_it_warns_that_it_is_not_half_of_capacity() {
        RampController.RampResult unbracketed =
                new RampController.RampResult(3200, Double.POSITIVE_INFINITY);

        String described = ScenarioSupport.RateBasis.of(unbracketed).describe(0.5);

        assertThat(described).contains("LOWER BOUND").contains("not 50% of capacity");
    }

    @Test
    void GIVEN_a_rate_given_on_the_command_line_WHEN_it_is_described_THEN_no_ceiling_is_claimed_for_it() {
        String described = ScenarioSupport.RateBasis.EXPLICIT.describe(0.5);

        assertThat(described).isEqualTo("rate given explicitly");
        assertThat(described).doesNotContain("ceiling");
    }

    @Test
    void GIVEN_a_load_that_is_not_half_of_the_search_result_WHEN_it_is_described_THEN_the_stated_fraction_follows_it() {
        RampController.RampResult bracketed = new RampController.RampResult(1000, 1100);

        assertThat(ScenarioSupport.RateBasis.of(bracketed).describe(0.8))
                .isEqualTo("80% of a bracketed ceiling");
    }
}
