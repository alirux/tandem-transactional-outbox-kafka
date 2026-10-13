package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;

class OpenApiGateCaseTest {

    private static final OpenApiGate.Line ACCEPTED = new OpenApiGate.Line(OpenApiGate.Kind.ACCEPTED,
            "finding 897e924ade7d", "ERR request-parameter-became-required at GET /outbox/messages: aggregateId");
    private static final OpenApiGate.Line UNDECLARED = new OpenApiGate.Line(OpenApiGate.Kind.UNDECLARED,
            "finding b80cc19d72d8", "ERR request-parameter-became-required at GET /outbox/messages: aggregateType");
    private static final OpenApiGate.Line STALE = new OpenApiGate.Line(OpenApiGate.Kind.STALE, "finding e01fe03fe989");

    private static OpenApiGate.Outcome outcome(OpenApiGate.Line... lines) {
        return new OpenApiGate.Outcome(List.of(lines), List.of(), List.of(), List.of());
    }

    @Test
    void GIVEN_an_expectation_WHEN_parsed_THEN_it_holds_the_parts_the_verdict_and_the_identities_sorted() {
        OpenApiGateCase.Expectation expected = OpenApiGateCase.parse("# What the case changes.\n\n"
                + "checks compatibility frozen-names\nverdict fail\n" + UNDECLARED.identity() + "\n"
                + ACCEPTED.identity() + "\n");

        assertThat(expected.parts()).containsExactly(OpenApiGateCase.Part.COMPATIBILITY,
                OpenApiGateCase.Part.FROZEN_NAMES);
        assertThat(expected.passes()).isFalse();
        assertThat(expected.identities()).containsExactly(ACCEPTED.identity(), UNDECLARED.identity());
    }

    @Test
    void GIVEN_an_expectation_that_breaks_the_format_WHEN_parsed_THEN_it_is_rejected() {
        List<String> invalid = List.of(
                "verdict pass\n",
                "checks validity\n",
                "checks everything\nverdict pass\n",
                "checks validity\nverdict maybe\n",
                "checks validity\nverdict fail\nfinding 897e924ade7d\n");

        for (String text : invalid) {
            assertThatThrownBy(() -> OpenApiGateCase.parse(text)).as(text).isInstanceOf(GradleException.class)
                    .hasMessageContaining(OpenApiGateCase.EXPECTED);
        }
    }

    @Test
    void GIVEN_an_outcome_concluding_the_expected_identities_in_any_order_WHEN_compared_THEN_the_case_holds() {
        OpenApiGateCase.Expectation expected = OpenApiGateCase.parse("checks compatibility\nverdict fail\n"
                + STALE.identity() + "\n" + ACCEPTED.identity() + "\n" + UNDECLARED.identity() + "\n");

        assertThat(OpenApiGateCase.mismatches(expected, outcome(UNDECLARED, ACCEPTED, STALE))).isEmpty();
    }

    @Test
    void GIVEN_an_outcome_with_another_verdict_or_other_lines_WHEN_compared_THEN_each_difference_is_named() {
        OpenApiGateCase.Expectation expected = OpenApiGateCase.parse("checks compatibility\nverdict fail\n"
                + ACCEPTED.identity() + "\n" + UNDECLARED.identity() + "\n");

        assertThat(OpenApiGateCase.mismatches(expected, outcome(ACCEPTED))).containsExactly(
                "the gate passes, expected to fail",
                "expected, not concluded: " + UNDECLARED.identity());
        assertThat(OpenApiGateCase.mismatches(expected, outcome(ACCEPTED, UNDECLARED, STALE))).containsExactly(
                "concluded, not expected: " + STALE);
        assertThat(OpenApiGateCase.mismatches(OpenApiGateCase.parse("checks validity\nverdict pass\n"),
                outcome(STALE))).containsExactly("the gate fails, expected to pass", "concluded, not expected: " + STALE);
    }
}
