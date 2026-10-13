package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class SchemaCompatOutcomeTest {

    private static final String JDBC = "tandem-jdbc";
    private static final String SCENARIO = "com.codingful.tandem.jdbc.schemacompat.BaselineOnUpgradedSchemaTest";
    private static final String RELEASE_NOTE = "Apply the v6 migration with every relay stopped.";

    private static final SchemaCompatOutcome.FailedTest UPGRADE = failed("GIVEN_a_populated_database_WHEN_upgraded_THEN_it_applies()");
    private static final SchemaCompatOutcome.FailedTest CLAIM = failed("GIVEN_the_upgraded_schema_WHEN_claiming_THEN_rows_return()");
    /** How Gradle reports a failure of the scenario's setup, run before its first test method. */
    private static final SchemaCompatOutcome.FailedTest SETUP = failed("initializationError");
    /** How Gradle reports a failure of the scenario's teardown, run after its last test method. */
    private static final SchemaCompatOutcome.FailedTest TEARDOWN = failed("executionError");

    private static SchemaCompatOutcome.FailedTest failed(String name) {
        return new SchemaCompatOutcome.FailedTest(SCENARIO, name);
    }

    private static final DeclaredBreaks.Entry DECLARED =
            new DeclaredBreaks.Entry(SchemaCompatOutcome.GATE, JDBC, SchemaCompatOutcome.ANY_STEP, RELEASE_NOTE, 3);

    @Test
    void GIVEN_every_step_passed_and_nothing_declared_WHEN_judged_THEN_the_gate_passes() {
        SchemaCompatOutcome.Verdict verdict = SchemaCompatOutcome.judge(JDBC, List.of(), List.of());

        assertThat(verdict.problems()).isEmpty();
        assertThat(verdict.accepted()).isEmpty();
    }

    @Test
    void GIVEN_failed_steps_and_nothing_declared_WHEN_judged_THEN_each_step_fails_the_gate_with_the_declaration_to_write() {
        SchemaCompatOutcome.Verdict verdict = SchemaCompatOutcome.judge(JDBC, List.of(UPGRADE, CLAIM), List.of());

        assertThat(verdict.accepted()).isEmpty();
        assertThat(verdict.problems()).hasSize(3);
        assertThat(verdict.problems().get(0)).startsWith(UPGRADE.toString()).contains("failed");
        assertThat(verdict.problems().get(1)).startsWith(CLAIM.toString()).contains("failed");
        assertThat(verdict.problems().get(2))
                .contains(SchemaCompatOutcome.GATE + " | " + JDBC + " | " + SchemaCompatOutcome.ANY_STEP + " | ");
    }

    @Test
    void GIVEN_failed_steps_and_a_declaration_WHEN_judged_THEN_the_gate_passes_reporting_the_steps_it_accepted() {
        SchemaCompatOutcome.Verdict verdict = SchemaCompatOutcome.judge(JDBC, List.of(UPGRADE, CLAIM), List.of(DECLARED));

        assertThat(verdict.problems()).isEmpty();
        assertThat(verdict.accepted()).singleElement().asString()
                .contains(DECLARED.toString())
                .contains(RELEASE_NOTE)
                .contains(UPGRADE.name())
                .contains(CLAIM.name());
    }

    @Test
    void GIVEN_a_declaration_and_every_step_passed_WHEN_judged_THEN_the_declaration_fails_the_gate_as_stale() {
        SchemaCompatOutcome.Verdict verdict = SchemaCompatOutcome.judge(JDBC, List.of(), List.of(DECLARED));

        assertThat(verdict.accepted()).isEmpty();
        assertThat(verdict.problems()).singleElement().asString()
                .contains("matches no failed step")
                .contains(DECLARED.toString());
    }

    @Test
    void GIVEN_the_setup_failed_WHEN_judged_THEN_the_gate_fails_whatever_is_declared() {
        for (List<DeclaredBreaks.Entry> declarations : List.of(List.<DeclaredBreaks.Entry>of(), List.of(DECLARED))) {
            SchemaCompatOutcome.Verdict verdict = SchemaCompatOutcome.judge(JDBC, List.of(SETUP), declarations);

            assertThat(verdict.accepted()).isEmpty();
            assertThat(verdict.problems()).singleElement().asString()
                    .startsWith(SETUP.toString())
                    .contains("no declaration accepts it");
        }
    }

    @Test
    void GIVEN_declared_steps_failed_and_the_teardown_failed_too_WHEN_judged_THEN_the_teardown_still_fails_the_gate() {
        SchemaCompatOutcome.Verdict verdict = SchemaCompatOutcome.judge(JDBC, List.of(CLAIM, TEARDOWN), List.of(DECLARED));

        assertThat(verdict.problems()).singleElement().asString().startsWith(TEARDOWN.toString());
        assertThat(verdict.accepted()).singleElement().asString().contains(CLAIM.name());
    }
}
