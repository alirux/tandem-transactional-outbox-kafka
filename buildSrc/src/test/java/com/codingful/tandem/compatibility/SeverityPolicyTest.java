package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.Map;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;

class SeverityPolicyTest {

    private static final String IMAGE = "tufin/oasdiff:v1.33.0";

    private static final String POLICY = """
            # Tandem's policy.
            reviewed-with tufin/oasdiff:v1.33.0

            # A default changes.
            request-parameter-default-value-changed err
              # A published element is removed.
            response-optional-property-removed   err

            # Lowered.
            response-property-enum-value-added info
            """;

    @Test
    void GIVEN_a_policy_with_comments_and_blank_lines_WHEN_handed_to_oasdiff_THEN_only_its_rules_remain_in_order() {
        assertThat(SeverityPolicy.parse(POLICY).oasdiffFile()).isEqualTo("""
                request-parameter-default-value-changed err
                response-optional-property-removed err
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
                "endpoint-added info\nendpoint-added err", "repeats the rule endpoint-added",
                "reviewed-with tufin/oasdiff:v1.34.0", "repeats the reviewed-with line");

        invalid.forEach((text, problem) -> assertThatThrownBy(() -> SeverityPolicy.parse(POLICY + text))
                .as(text).isInstanceOf(GradleException.class)
                .hasMessageContaining(SeverityPolicy.PATH + " line ").hasMessageContaining(problem));
        assertThatThrownBy(() -> SeverityPolicy.parse("endpoint-added info\n")).isInstanceOf(GradleException.class)
                .hasMessageContaining("reviewed-with");
    }

    @Test
    void GIVEN_the_image_the_file_was_reviewed_with_WHEN_checked_THEN_it_applies_whatever_the_digest() {
        assertThat(SeverityPolicy.parse(POLICY).problems(IMAGE + "@sha256:6263a96d")).isEmpty();
        assertThat(SeverityPolicy.parse(POLICY).problems(IMAGE)).isEmpty();
    }

    @Test
    void GIVEN_another_pinned_image_WHEN_checked_THEN_the_file_asks_for_a_review_naming_both() {
        assertThat(SeverityPolicy.parse(POLICY).problems("tufin/oasdiff:v1.34.0@sha256:0000")).singleElement()
                .asString().contains("reviewed with " + IMAGE).contains("pins tufin/oasdiff:v1.34.0");
    }
}
