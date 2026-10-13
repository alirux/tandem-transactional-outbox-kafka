package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import groovy.json.JsonOutput;
import java.util.List;
import java.util.Map;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;

class SeverityPolicyTest {

    private static final String DEFAULT_CHANGED = "request-parameter-default-value-changed";
    private static final String OPTIONAL_REMOVED = "response-optional-property-removed";
    private static final String ENDPOINT_ADDED = "endpoint-added";
    private static final String ENUM_VALUE_ADDED = "response-property-enum-value-added";
    private static final String BECAME_REQUIRED = "request-parameter-became-required";

    /** A catalog of four rules: three at info upstream, one error. */
    private static final Map<String, String> CATALOG = Map.of(DEFAULT_CHANGED, "info", OPTIONAL_REMOVED, "info",
            ENDPOINT_ADDED, "info", ENUM_VALUE_ADDED, "error", BECAME_REQUIRED, "error");

    private static final String POLICY = """
            # Tandem's policy.

            # A default changes.
            request-parameter-default-value-changed err
              # A published element is removed.
            response-optional-property-removed   err

            # Compatible.
            endpoint-added info
            response-property-enum-value-added info
            """;

    private static OasdiffRun checks(int exitCode, String stdout) {
        return new OasdiffRun("catalog", exitCode, stdout, "Error: something");
    }

    @Test
    void GIVEN_a_policy_with_comments_and_blank_lines_WHEN_handed_to_oasdiff_THEN_only_its_rules_remain_in_order() {
        assertThat(SeverityPolicy.parse(POLICY).oasdiffFile()).isEqualTo("""
                request-parameter-default-value-changed err
                response-optional-property-removed err
                endpoint-added info
                response-property-enum-value-added info
                """);
        assertThat(SeverityPolicy.parse(POLICY.replace("\n", "\r\n")).oasdiffFile())
                .isEqualTo(SeverityPolicy.parse(POLICY).oasdiffFile());
    }

    @Test
    void GIVEN_lines_that_are_not_a_rule_and_a_level_WHEN_parsed_THEN_the_file_is_rejected_naming_the_line() {
        Map<String, String> invalid = Map.of(
                "endpoint-added", "is not '<rule-id> <level>'",
                "endpoint-added info # compatible", "is not '<rule-id> <level>'",
                "endpoint-added error", "sets the level 'error'",
                "endpoint-added none", "sets the level 'none'",
                "endpoint-added info\nendpoint-added err", "repeats the rule endpoint-added");

        invalid.forEach((text, problem) -> assertThatThrownBy(() -> SeverityPolicy.parse("# Policy.\n" + text))
                .as(text).isInstanceOf(GradleException.class)
                .hasMessageContaining(SeverityPolicy.PATH + " line ").hasMessageContaining(problem));
    }

    @Test
    void GIVEN_a_policy_classifying_every_info_rule_WHEN_checked_against_the_catalog_THEN_nothing_is_missing() {
        assertThat(SeverityPolicy.parse(POLICY).problems(CATALOG)).isEmpty();
    }

    @Test
    void GIVEN_an_info_rule_without_a_line_and_a_line_for_no_rule_WHEN_checked_THEN_both_are_named() {
        SeverityPolicy policy = SeverityPolicy.parse(POLICY.replace("endpoint-added info\n", "endpoint-removed err\n"));

        assertThat(policy.problems(CATALOG)).containsExactlyInAnyOrder(
                ENDPOINT_ADDED + " is info in the pinned oasdiff image and has no line: add '" + ENDPOINT_ADDED + " err' or '"
                        + ENDPOINT_ADDED + " info' under the group of the contract rule it falls in",
                "endpoint-removed is not a rule of the pinned oasdiff image: remove its line");
    }

    @Test
    void GIVEN_the_catalog_oasdiff_prints_WHEN_read_THEN_each_rule_has_its_upstream_level() {
        String json = JsonOutput.toJson(List.of(
                Map.of("id", ENDPOINT_ADDED, "level", "info", "description", "endpoint added"),
                Map.of("id", BECAME_REQUIRED, "level", "error", "description", "parameter became required")));

        assertThat(SeverityPolicy.catalog(checks(0, json)))
                .containsExactly(Map.entry(ENDPOINT_ADDED, "info"), Map.entry(BECAME_REQUIRED, "error"));
    }

    @Test
    void GIVEN_a_catalog_run_that_failed_or_printed_no_rule_WHEN_read_THEN_the_build_fails() {
        for (OasdiffRun run : List.of(checks(2, ""), checks(0, "not json"), checks(0, "[]"), checks(0, "{}"),
                checks(0, "[{\"id\": \"endpoint-added\"}]"), checks(0, "[\"endpoint-added\"]"), checks(0, "[{\"level\": \"info\"}]"))) {
            assertThatThrownBy(() -> SeverityPolicy.catalog(run)).as(run.toString())
                    .isInstanceOf(GradleException.class).hasMessageContaining("oasdiff catalog");
        }
    }

    @Test
    void GIVEN_a_catalog_WHEN_the_initial_policy_is_written_THEN_it_lists_every_info_rule_at_info_and_nothing_else() {
        String initial = SeverityPolicy.initial(CATALOG);
        SeverityPolicy policy = SeverityPolicy.parse(initial);

        assertThat(policy.oasdiffFile()).isEqualTo(ENDPOINT_ADDED + " info\n" + DEFAULT_CHANGED + " info\n"
                + OPTIONAL_REMOVED + " info\n");
        assertThat(policy.problems(CATALOG)).isEmpty();
        assertThat(initial).startsWith("#");
    }
}
