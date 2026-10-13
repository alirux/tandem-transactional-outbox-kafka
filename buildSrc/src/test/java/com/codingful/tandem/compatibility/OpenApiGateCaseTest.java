package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;

class OpenApiGateCaseTest {

    private static final OpenApiGate.Line ACCEPTED = new OpenApiGate.Line(OpenApiGate.Kind.ACCEPTED,
            "finding 897e924ade7d: ERR request-parameter-became-required at GET /outbox/messages: aggregateId");
    private static final OpenApiGate.Line UNDECLARED = new OpenApiGate.Line(OpenApiGate.Kind.UNDECLARED,
            "finding b80cc19d72d8: ERR request-parameter-became-required at GET /outbox/messages: aggregateType");
    private static final OpenApiGate.Line STALE = new OpenApiGate.Line(OpenApiGate.Kind.STALE, "finding e01fe03fe989");

    private static OpenApiGate.Outcome outcome(OpenApiGate.Line... lines) {
        return new OpenApiGate.Outcome(List.of(lines), List.of(), List.of(), List.of());
    }

    @Test
    void GIVEN_an_expectation_WHEN_parsed_THEN_it_holds_the_verdict_the_baseline_and_the_lines_sorted() {
        OpenApiGateCase.Expectation expected = OpenApiGateCase.parse("# What the case changes.\n\nverdict fail\n"
                + UNDECLARED + "\n" + ACCEPTED + "\n");
        OpenApiGateCase.Expectation alone = OpenApiGateCase.parse("# Validity only.\nbaseline none\nverdict pass\n");

        assertThat(expected.passes()).isFalse();
        assertThat(expected.compared()).isTrue();
        assertThat(expected.lines()).containsExactly(ACCEPTED.toString(), UNDECLARED.toString());
        assertThat(alone.passes()).isTrue();
        assertThat(alone.compared()).isFalse();
        assertThat(alone.lines()).isEmpty();
    }

    @Test
    void GIVEN_an_expectation_that_breaks_the_format_or_contradicts_itself_WHEN_parsed_THEN_it_is_rejected() {
        List<String> invalid = List.of(
                "# No verdict.\n" + ACCEPTED,
                "verdict pass\nverdict fail\n",
                "verdict maybe\n",
                "verdict fail\nfinding 897e924ade7d\n",
                "verdict pass\n" + UNDECLARED,
                "verdict fail\n" + ACCEPTED,
                "verdict fail\n");

        for (String text : invalid) {
            assertThatThrownBy(() -> OpenApiGateCase.parse(text)).as(text).isInstanceOf(GradleException.class)
                    .hasMessageContaining(OpenApiGateCase.EXPECTED);
        }
    }

    @Test
    void GIVEN_an_outcome_concluding_the_expected_lines_in_any_order_WHEN_compared_THEN_the_case_holds() {
        OpenApiGateCase.Expectation expected = OpenApiGateCase.parse("verdict fail\n" + STALE + "\n" + ACCEPTED + "\n"
                + UNDECLARED + "\n");

        assertThat(OpenApiGateCase.mismatches(expected, outcome(UNDECLARED, ACCEPTED, STALE))).isEmpty();
    }

    @Test
    void GIVEN_an_outcome_with_another_verdict_or_other_lines_WHEN_compared_THEN_each_difference_is_named() {
        OpenApiGateCase.Expectation expected = OpenApiGateCase.parse("verdict fail\n" + ACCEPTED + "\n" + UNDECLARED
                + "\n");

        assertThat(OpenApiGateCase.mismatches(expected, outcome(ACCEPTED))).containsExactly(
                "the gate passes, expected to fail",
                "expected, not concluded: " + UNDECLARED);
        assertThat(OpenApiGateCase.mismatches(expected, outcome(ACCEPTED, UNDECLARED, STALE))).containsExactly(
                "concluded, not expected: " + STALE);
        assertThat(OpenApiGateCase.mismatches(OpenApiGateCase.parse("verdict pass\n"), outcome(STALE))).containsExactly(
                "the gate fails, expected to pass",
                "concluded, not expected: " + STALE);
    }

    @Test
    void GIVEN_an_outcome_WHEN_rendered_THEN_it_reads_back_as_the_expectation_it_meets() {
        OpenApiGate.Outcome outcome = outcome(UNDECLARED, ACCEPTED);

        assertThat(OpenApiGateCase.mismatches(OpenApiGateCase.parse(OpenApiGateCase.render(outcome)), outcome))
                .isEmpty();
        assertThat(OpenApiGateCase.render(outcome(ACCEPTED))).startsWith("verdict pass\n");
    }
}
