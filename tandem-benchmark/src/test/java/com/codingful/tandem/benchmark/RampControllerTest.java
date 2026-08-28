package com.codingful.tandem.benchmark;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.assertj.core.api.Assertions.within;

import com.codingful.tandem.benchmark.RampController.Search;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the rate search (LLD-benchmark §7): exponential bracketing followed by bisection over the
 * monotone predicate "this rate keeps the backlog flat". Drives {@link RampController#afterHold}
 * directly — the search is a pure function of each hold's outcome, so its behaviour and its
 * convergence are checked without a database, a relay, or the wall-clock waits a real search spends.
 */
class RampControllerTest {

    private static final double TOLERANCE = 0.05;

    /** Runs the search against a host whose true ceiling is {@code ceiling}, for at most {@code holds}. */
    private static Search searchAgainstCeiling(double seed, double ceiling, int holds) {
        Search search = Search.from(seed);
        for (int i = 0; i < holds && !search.withinTolerance(TOLERANCE); i++) {
            search = RampController.afterHold(search, search.rate() <= ceiling);
        }
        return search;
    }

    @Test
    void GIVEN_a_rate_the_backlog_absorbs_WHEN_the_ceiling_is_still_unknown_THEN_the_next_candidate_doubles() {
        Search next = RampController.afterHold(Search.from(100), true);

        assertThat(next.lo()).isEqualTo(100);
        assertThat(next.rate()).isEqualTo(200);
        assertThat(next.bracketed()).isFalse();
    }

    @Test
    void GIVEN_the_backlog_grows_for_the_first_time_WHEN_that_rate_fails_THEN_it_becomes_the_upper_end_of_the_bracket() {
        Search next = RampController.afterHold(new Search(800, Double.POSITIVE_INFINITY, 1600), false);

        assertThat(next.lo()).isEqualTo(800);
        assertThat(next.hi()).isEqualTo(1600);
        assertThat(next.bracketed()).isTrue();
        assertThat(next.rate()).as("bisects the bracket it just closed").isEqualTo(1200);
    }

    @Test
    void GIVEN_a_bracketed_search_WHEN_the_midpoint_holds_THEN_it_raises_the_floor_and_bisects_again() {
        Search next = RampController.afterHold(new Search(800, 1600, 1200), true);

        assertThat(next.lo()).isEqualTo(1200);
        assertThat(next.hi()).isEqualTo(1600);
        assertThat(next.rate()).isEqualTo(1400);
    }

    @Test
    void GIVEN_a_bracketed_search_WHEN_the_midpoint_fails_THEN_it_lowers_the_ceiling_and_bisects_again() {
        Search next = RampController.afterHold(new Search(800, 1600, 1200), false);

        assertThat(next.lo()).isEqualTo(800);
        assertThat(next.hi()).isEqualTo(1200);
        assertThat(next.rate()).isEqualTo(1000);
    }

    /**
     * The convergence property the algorithm was chosen for: the answer's precision is a function of
     * how many holds were run, stated in advance — not of where the budget happened to run out.
     */
    @Test
    void GIVEN_a_ceiling_far_above_the_seed_WHEN_the_search_is_given_enough_holds_THEN_it_converges_to_the_stated_tolerance() {
        Search search = searchAgainstCeiling(100, 3700, 20);

        assertThat(search.withinTolerance(TOLERANCE)).isTrue();
        assertThat(search.lo()).isCloseTo(3700, within(3700 * TOLERANCE));
    }

    @Test
    void GIVEN_a_ceiling_below_the_seed_WHEN_the_search_runs_THEN_it_still_converges_from_above() {
        Search search = searchAgainstCeiling(100, 12, 20);

        assertThat(search.withinTolerance(TOLERANCE)).isTrue();
        assertThat(search.lo()).isCloseTo(12, within(12 * TOLERANCE));
    }

    /**
     * The sanity check whose absence let an arithmetic artefact pass as a measurement: with too few
     * holds to ever observe a failing rate, the search has located nothing, and the figure it carries
     * is the seed doubled a few times — the same on every host.
     */
    @Test
    void GIVEN_a_ceiling_the_budget_cannot_reach_WHEN_the_search_runs_out_of_holds_THEN_it_reports_no_bracket() {
        Search search = searchAgainstCeiling(100, 1_000_000, 4);

        assertThat(search.bracketed()).as("no rate ever failed, so no ceiling was found").isFalse();
        assertThat(search.lo()).isEqualTo(800);
    }

    @Test
    void GIVEN_a_host_that_cannot_hold_even_the_seed_WHEN_the_search_runs_THEN_the_floor_stays_at_zero_until_a_rate_holds() {
        Search next = RampController.afterHold(Search.from(100), false);

        assertThat(next.lo()).isEqualTo(0);
        assertThat(next.hi()).isEqualTo(100);
        assertThat(next.rate()).isEqualTo(50);
    }

    @Test
    void GIVEN_a_bracket_that_has_closed_to_the_floor_WHEN_bisection_would_go_below_one_THEN_the_offered_rate_never_does() {
        Search next = RampController.afterHold(new Search(0, 1, 1), false);

        assertThat(next.rate()).isGreaterThanOrEqualTo(1);
    }

    @Test
    void GIVEN_a_bracket_still_wider_than_the_tolerance_WHEN_it_is_checked_THEN_the_search_is_not_done() {
        assertThat(new Search(800, 1600, 1200).withinTolerance(TOLERANCE)).isFalse();
        assertThat(new Search(1550, 1600, 1575).withinTolerance(TOLERANCE)).isTrue();
    }

    @Test
    void GIVEN_no_rate_has_failed_yet_WHEN_the_bracket_is_checked_THEN_it_is_never_within_tolerance() {
        assertThat(Search.from(100).withinTolerance(TOLERANCE))
                .as("an unbounded bracket cannot be within any tolerance")
                .isFalse();
    }

    /**
     * A search cut short by its budget still yields a real measurement — just a looser one. Reporting
     * the target tolerance rather than the bracket actually achieved would overstate it.
     */
    @Test
    void GIVEN_a_search_stopped_before_it_converged_WHEN_it_reports_its_precision_THEN_it_states_the_bracket_it_actually_reached() {
        RampController.RampResult result = new RampController.RampResult(800, 1000);

        assertThat(result.bracketed()).isTrue();
        assertThat(result.relativeUncertainty()).isCloseTo(0.2, within(0.001));
    }

    @Test
    void GIVEN_a_search_that_never_saw_a_rate_fail_WHEN_it_reports_THEN_it_claims_neither_a_bracket_nor_a_precision() {
        RampController.RampResult result = new RampController.RampResult(800, Double.POSITIVE_INFINITY);

        assertThat(result.bracketed()).isFalse();
        assertThat(result.relativeUncertainty()).isNaN();
    }

    private static List<RampController.Observation> trace(Object... rateThenHeld) {
        List<RampController.Observation> out = new ArrayList<>();
        for (int i = 0; i < rateThenHeld.length; i += 2) {
            out.add(new RampController.Observation(
                    ((Number) rateThenHeld[i]).doubleValue(), (Boolean) rateThenHeld[i + 1]));
        }
        return out;
    }

    @Test
    void GIVEN_a_result_backed_by_its_own_trace_WHEN_it_is_audited_THEN_it_is_accepted() {
        RampController.auditAgainstTrace(new RampController.RampResult(1300, 1350),
                trace(800, true, 1600, false, 1200, true, 1400, false, 1300, true, 1350, false));
    }

    /**
     * The defect this audit exists for: an earlier version reported the latest confirmed hold rather
     * than the highest, so the run beat its own published figure and nothing objected.
     */
    @Test
    void GIVEN_a_rate_that_held_above_the_reported_maximum_WHEN_the_result_is_audited_THEN_it_is_rejected() {
        assertThatThrownBy(() -> RampController.auditAgainstTrace(
                new RampController.RampResult(677.6, 968),
                trace(800, true, 880, true, 968, false, 677.6, true)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("880")
                .hasMessageContaining("held during the same run");
    }

    @Test
    void GIVEN_a_rate_that_failed_at_or_below_the_reported_maximum_WHEN_the_result_is_audited_THEN_it_is_rejected() {
        assertThatThrownBy(() -> RampController.auditAgainstTrace(
                new RampController.RampResult(1300, 1350),
                trace(1200, false, 1300, true)))
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("failed to hold");
    }

    @Test
    void GIVEN_a_search_that_completed_no_holds_WHEN_it_is_audited_THEN_there_is_nothing_to_contradict() {
        RampController.auditAgainstTrace(
                new RampController.RampResult(0, Double.POSITIVE_INFINITY), List.of());
    }
}
