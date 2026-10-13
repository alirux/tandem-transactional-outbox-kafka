package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class CheckOpenApiTest {

    private static final DeclaredBreaks.Entry STALE = new DeclaredBreaks.Entry(OpenApiGate.GATE, OpenApiGate.MODULE,
            OpenApiGate.FINDING + "e01fe03fe989", "limit defaults to 100.", 7);
    private static final String LOAD_ERROR = "Error: failed to load revision spec";

    private static OpenApiGate.Outcome outcome(List<String> undeclared, List<DeclaredBreaks.Entry> stale,
            OpenApiGate.Line... lines) {
        return new OpenApiGate.Outcome(List.of(lines), undeclared, List.of(), stale);
    }

    @Test
    void GIVEN_an_undeclared_break_and_a_stale_declaration_WHEN_the_gate_fails_THEN_it_says_how_to_declare_and_what_to_remove() {
        String server = FrozenNames.SERVER + "https://admin.example.com:8443/v1";
        String advice = CheckOpenApi.advice(outcome(List.of("finding 4635980a877f", server), List.of(STALE),
                new OpenApiGate.Line(OpenApiGate.Kind.UNDECLARED, "finding 4635980a877f", "ERR ..."),
                new OpenApiGate.Line(OpenApiGate.Kind.UNDECLARED, server, "no longer offered"),
                new OpenApiGate.Line(OpenApiGate.Kind.STALE, STALE.item())), List.of());

        assertThat(advice).contains("\n  openapi | tandem-admin | finding 4635980a877f | <release-notes line>"
                        + "\n  openapi | tandem-admin | " + server + " | <release-notes line>")
                .contains("Declared breaks that match nothing, to remove:\n  " + STALE);
    }

    @Test
    void GIVEN_runs_that_failed_WHEN_the_gate_fails_THEN_it_shows_what_each_printed_but_not_a_validation_with_findings() {
        List<OasdiffRun> runs = List.of(
                new OasdiffRun("validate", 1, "[]", "validate findings"),
                new OasdiffRun("breaking", 102, "", LOAD_ERROR + "\n"),
                new OasdiffRun("document", 0, "{}", "Unable to find image locally"));

        String advice = CheckOpenApi.advice(outcome(List.of(), List.of(),
                new OpenApiGate.Line(OpenApiGate.Kind.ERROR, "breaking exit 102")), runs);

        assertThat(advice).isEqualTo("\noasdiff breaking printed: " + LOAD_ERROR);
        assertThat(CheckOpenApi.advice(outcome(List.of(), List.of()),
                List.of(new OasdiffRun("validate", 102, "", LOAD_ERROR))))
                .isEqualTo("\noasdiff validate printed: " + LOAD_ERROR);
    }
}
