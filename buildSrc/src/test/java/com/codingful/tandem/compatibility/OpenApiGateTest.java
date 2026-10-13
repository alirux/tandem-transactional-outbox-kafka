package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import groovy.json.JsonOutput;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class OpenApiGateTest {

    private static final String REQUIRED_ID = "897e924ade7d";
    private static final String REQUIRED_TYPE = "b80cc19d72d8";
    private static final String BECAME_REQUIRED = "request-parameter-became-required";
    private static final String MESSAGES = "/outbox/messages";
    private static final String NOT_FOUND = "not-found";
    private static final String TEMPLATE = "{basePath}/v1";

    private static final OasdiffRun VALID = new OasdiffRun("validate", 0, "[]", "");
    private static final OasdiffRun NO_BREAK = breaking("[]");

    private static OasdiffRun breaking(String json) {
        return new OasdiffRun("breaking", 0, json, "");
    }

    private static String findings(Map<?, ?>... findings) {
        return JsonOutput.toJson(List.of(findings));
    }

    private static Map<String, Object> finding(String fingerprint, int level, String text) {
        Map<String, Object> finding = new LinkedHashMap<>();
        finding.put("id", BECAME_REQUIRED);
        finding.put("text", text);
        finding.put("level", level);
        finding.put("operation", "GET");
        finding.put("operationId", "searchOutboxMessages");
        finding.put("path", MESSAGES);
        finding.put("section", "paths");
        finding.put("fingerprint", fingerprint);
        return finding;
    }

    /** A document as oasdiff renders it in JSON: one server, and a problem type in an example. */
    private static OasdiffRun document(String command, String basePath, String slug) {
        return new OasdiffRun(command, 0, JsonOutput.toJson(Map.of(
                "servers", List.of(Map.of("url", TEMPLATE, "variables", Map.of("basePath", Map.of("default", basePath)))),
                "components", Map.of("responses", Map.of("NotFound", Map.of("content", Map.of("application/problem+json",
                        Map.of("example", Map.of("type", "https://tandem.codingful.com/problems/" + slug)))))))), "");
    }

    private static final OasdiffRun BASELINE_DOCUMENT = document("baseline-document", "/tandem/admin", NOT_FOUND);
    private static final OasdiffRun SAME_DOCUMENT = document("document", "/tandem/admin", NOT_FOUND);

    private static OpenApiGate.Comparison comparing(OasdiffRun breaking) {
        return new OpenApiGate.Comparison(breaking, BASELINE_DOCUMENT, SAME_DOCUMENT);
    }

    private static DeclaredBreaks.Entry declared(String item) {
        return new DeclaredBreaks.Entry(OpenApiGate.GATE, OpenApiGate.MODULE, item, "Release note.", 3);
    }

    private static List<String> lines(OpenApiGate.Outcome outcome) {
        return outcome.lines().stream().map(OpenApiGate.Line::toString).toList();
    }

    @Test
    void GIVEN_a_valid_document_and_no_baseline_WHEN_judged_THEN_the_gate_passes_with_nothing_to_report() {
        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, null, List.of());

        assertThat(outcome.passed()).isTrue();
        assertThat(outcome.lines()).isEmpty();
    }

    @Test
    void GIVEN_validation_findings_WHEN_judged_THEN_each_fails_on_one_line_and_no_declaration_accepts_it() {
        OasdiffRun validate = new OasdiffRun("validate", 1, findings(
                Map.of("id", "info-required", "text", "must be an object", "level", 3, "fingerprint", "ccc581e500e3"),
                Map.of("id", "example-violates-schema", "text", "invalid example:\n  Value:\n    \"x\"", "level", 1,
                        "fingerprint", "0a1b2c3d4e5f")), "");

        OpenApiGate.Outcome outcome = OpenApiGate.judge(validate, comparing(NO_BREAK), List.of(declared(
                OpenApiGate.FINDING + "ccc581e500e3")));

        assertThat(outcome.passed()).isFalse();
        assertThat(lines(outcome)).containsExactly(
                "invalid info-required: must be an object",
                "invalid example-violates-schema: invalid example: Value: \"x\"",
                "stale finding ccc581e500e3");
    }

    @Test
    void GIVEN_a_validation_that_did_not_judge_the_document_WHEN_judged_THEN_it_is_an_error() {
        List<OasdiffRun> runs = List.of(
                new OasdiffRun("validate", 102, "", "Error: failed to load"),
                new OasdiffRun("validate", 1, "[]", ""),
                new OasdiffRun("validate", 0, "{\"id\":", ""),
                new OasdiffRun("validate", 0, "{}", ""),
                new OasdiffRun("validate", 0, "[{\"text\":\"no id\"}]", ""),
                new OasdiffRun("validate", 1, "[3]", ""));

        assertThat(runs).allSatisfy(run -> assertThat(OpenApiGate.judge(run, null, List.of()).lines())
                .singleElement().satisfies(line -> assertThat(line.kind()).isEqualTo(OpenApiGate.Kind.ERROR)));
        assertThat(lines(OpenApiGate.judge(runs.get(0), null, List.of()))).containsExactly("error validate exit 102");
    }

    @Test
    void GIVEN_findings_at_err_warn_and_info_WHEN_nothing_is_declared_THEN_err_and_warn_fail_by_their_fingerprint() {
        OasdiffRun breaking = breaking(findings(
                finding(REQUIRED_ID, 3, "the `query` request parameter `aggregateId` became required"),
                finding(REQUIRED_TYPE, 2, "the `query` request parameter `aggregateType` became required"),
                finding("c0ffee000000", 1, "an info finding")));

        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparing(breaking), List.of());

        assertThat(outcome.passed()).isFalse();
        assertThat(lines(outcome)).containsExactly(
                "undeclared finding " + REQUIRED_ID + ": ERR " + BECAME_REQUIRED + " at GET " + MESSAGES
                        + ": the `query` request parameter `aggregateId` became required",
                "undeclared finding " + REQUIRED_TYPE + ": WARN " + BECAME_REQUIRED + " at GET " + MESSAGES
                        + ": the `query` request parameter `aggregateType` became required");
    }

    @Test
    void GIVEN_two_findings_of_one_rule_on_one_operation_WHEN_one_fingerprint_is_declared_THEN_only_its_finding_is_accepted() {
        OasdiffRun breaking = breaking(findings(finding(REQUIRED_ID, 3, "aggregateId"), finding(REQUIRED_TYPE, 3,
                "aggregateType")));
        DeclaredBreaks.Entry declaration = declared(OpenApiGate.FINDING + REQUIRED_ID);

        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparing(breaking), List.of(declaration));

        assertThat(outcome.passed()).isFalse();
        assertThat(outcome.lines()).extracting(OpenApiGate.Line::kind)
                .containsExactly(OpenApiGate.Kind.ACCEPTED, OpenApiGate.Kind.UNDECLARED);
        assertThat(outcome.lines().get(0).item()).isEqualTo(OpenApiGate.FINDING + REQUIRED_ID);
        assertThat(outcome.lines().get(1).item()).isEqualTo(OpenApiGate.FINDING + REQUIRED_TYPE);
        assertThat(outcome.undeclared()).containsExactly(OpenApiGate.FINDING + REQUIRED_TYPE);
        assertThat(outcome.accepted()).containsExactly(declaration);
        assertThat(outcome.stale()).isEmpty();
    }

    @Test
    void GIVEN_every_break_declared_WHEN_judged_THEN_the_gate_passes_reporting_what_it_accepted() {
        OasdiffRun breaking = breaking(findings(finding(REQUIRED_ID, 3, "aggregateId")));

        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparing(breaking),
                List.of(declared(OpenApiGate.FINDING + REQUIRED_ID)));

        assertThat(outcome.passed()).isTrue();
        assertThat(lines(outcome)).singleElement().asString().startsWith("accepted finding " + REQUIRED_ID);
    }

    @Test
    void GIVEN_a_declaration_matching_nothing_reported_WHEN_judged_THEN_it_fails_as_stale() {
        DeclaredBreaks.Entry finding = declared(OpenApiGate.FINDING + REQUIRED_ID);
        DeclaredBreaks.Entry server = declared(FrozenNames.SERVER + TEMPLATE);

        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparing(NO_BREAK), List.of(finding, server));

        assertThat(outcome.passed()).isFalse();
        assertThat(lines(outcome)).containsExactly("stale finding " + REQUIRED_ID, "stale server " + TEMPLATE);
        assertThat(outcome.stale()).containsExactly(finding, server);
    }

    @Test
    void GIVEN_a_comparison_that_did_not_run_WHEN_judged_THEN_it_fails_and_leaves_its_declarations_undecided() {
        DeclaredBreaks.Entry finding = declared(OpenApiGate.FINDING + REQUIRED_ID);
        DeclaredBreaks.Entry problemType = declared(FrozenNames.PROBLEM_TYPE + NOT_FOUND);
        List<OasdiffRun> failures = List.of(
                new OasdiffRun("breaking", 2, "", "panic: runtime error"),
                new OasdiffRun("breaking", 106, "", "Error: failed to load custom severity levels"),
                breaking("[{\"id\": \"truncated"),
                breaking("{\"not\": \"an array\"}"));

        for (OasdiffRun breaking : failures) {
            OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparing(breaking), List.of(finding, problemType));

            assertThat(outcome.passed()).as(breaking.toString()).isFalse();
            assertThat(outcome.lines()).as(breaking.toString()).extracting(OpenApiGate.Line::kind)
                    .containsExactly(OpenApiGate.Kind.ERROR, OpenApiGate.Kind.STALE);
            assertThat(outcome.stale()).as(breaking.toString()).containsExactly(problemType);
        }
        assertThat(lines(OpenApiGate.judge(VALID, comparing(failures.get(0)), List.of()))).containsExactly(
                "error breaking exit 2");
    }

    @Test
    void GIVEN_findings_without_one_identity_each_WHEN_judged_THEN_the_comparison_fails_whatever_is_declared() {
        Map<String, Object> withoutFingerprint = finding(REQUIRED_ID, 3, "aggregateId");
        withoutFingerprint.remove("fingerprint");
        Map<String, Object> blankFingerprint = finding("  ", 3, "aggregateId");
        Map<String, Object> unknownLevel = finding(REQUIRED_ID, 4, "aggregateId");
        DeclaredBreaks.Entry declaration = declared(OpenApiGate.FINDING + REQUIRED_ID);
        Map<String, String> runs = Map.of(
                "a finding has no fingerprint", findings(withoutFingerprint, finding(REQUIRED_TYPE, 3, "aggregateType")),
                "a finding has no fingerprint, ERR", findings(blankFingerprint),
                "the fingerprint " + REQUIRED_ID + " is shared by 2 findings",
                findings(finding(REQUIRED_ID, 3, "aggregateId"), finding(REQUIRED_ID, 2, "aggregateType")),
                "a finding has the unknown level 4", findings(unknownLevel));

        runs.forEach((error, json) -> {
            OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparing(breaking(json)), List.of(declaration));

            assertThat(outcome.passed()).isFalse();
            assertThat(outcome.lines()).singleElement().satisfies(line -> {
                assertThat(line.kind()).isEqualTo(OpenApiGate.Kind.ERROR);
                assertThat(line.item()).isEqualTo("breaking");
                assertThat(line.detail()).startsWith(error);
            });
            assertThat(outcome.accepted()).isEmpty();
        });
    }

    @Test
    void GIVEN_findings_outside_an_operation_WHEN_judged_THEN_each_is_located_by_its_section_else_the_document() {
        Map<String, Object> component = finding("aa0000000001", 3, "schema deleted");
        component.put("operation", "");
        component.put("path", "");
        component.put("section", "components");
        Map<String, Object> nowhere = finding("aa0000000002", 3, "somewhere");
        nowhere.remove("operation");
        nowhere.remove("path");
        nowhere.remove("section");

        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparing(breaking(findings(component, nowhere))),
                List.of());

        assertThat(lines(outcome)).containsExactly(
                "undeclared finding aa0000000001: ERR " + BECAME_REQUIRED + " at components: schema deleted",
                "undeclared finding aa0000000002: ERR " + BECAME_REQUIRED + " at the document: somewhere");
    }

    @Test
    void GIVEN_frozen_names_lost_WHEN_judged_THEN_each_fails_unless_its_item_is_declared() {
        OasdiffRun current = document("document", "/other/admin", "resource-not-found");
        DeclaredBreaks.Entry server = declared(FrozenNames.SERVER + TEMPLATE);

        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID,
                new OpenApiGate.Comparison(NO_BREAK, BASELINE_DOCUMENT, current), List.of(server));

        assertThat(lines(outcome)).containsExactly(
                "undeclared problem-type " + NOT_FOUND + ": no longer appears in the document",
                "accepted server " + TEMPLATE + ": basePath default /tandem/admin, now /other/admin");
        assertThat(outcome.accepted()).containsExactly(server);
    }

    @Test
    void GIVEN_an_operation_declaring_servers_of_its_own_WHEN_judged_THEN_it_is_invalid_whatever_is_declared() {
        OasdiffRun current = new OasdiffRun("document", 0, JsonOutput.toJson(Map.of(
                "servers", List.of(Map.of("url", TEMPLATE, "variables", Map.of("basePath",
                        Map.of("default", "/tandem/admin")))),
                "paths", Map.of(MESSAGES, Map.of("get", Map.of("servers", List.of(Map.of("url", "/messages/v1"))))),
                "info", Map.of("description", "https://tandem.codingful.com/problems/" + NOT_FOUND))), "");

        OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID,
                new OpenApiGate.Comparison(NO_BREAK, BASELINE_DOCUMENT, current), List.of());

        assertThat(outcome.passed()).isFalse();
        assertThat(outcome.lines()).extracting(OpenApiGate.Line::identity)
                .containsExactly("invalid " + OpenApiGate.SERVER_OVERRIDE + "operation GET " + MESSAGES);
    }

    @Test
    void GIVEN_a_document_oasdiff_could_not_render_WHEN_judged_THEN_the_names_fail_and_only_their_declarations_wait() {
        DeclaredBreaks.Entry finding = declared(OpenApiGate.FINDING + REQUIRED_ID);
        DeclaredBreaks.Entry problemType = declared(FrozenNames.PROBLEM_TYPE + NOT_FOUND);
        List<OpenApiGate.Comparison> failures = List.of(
                new OpenApiGate.Comparison(NO_BREAK, new OasdiffRun("baseline-document", 102, "", "Error"), SAME_DOCUMENT),
                new OpenApiGate.Comparison(NO_BREAK, BASELINE_DOCUMENT, new OasdiffRun("document", 0, "[]", "")),
                new OpenApiGate.Comparison(NO_BREAK, BASELINE_DOCUMENT, new OasdiffRun("document", 0, "{", "")));

        for (OpenApiGate.Comparison comparison : failures) {
            OpenApiGate.Outcome outcome = OpenApiGate.judge(VALID, comparison, List.of(finding, problemType));

            assertThat(outcome.lines()).extracting(OpenApiGate.Line::kind)
                    .containsExactly(OpenApiGate.Kind.ERROR, OpenApiGate.Kind.STALE);
            assertThat(outcome.stale()).containsExactly(finding);
        }
        assertThat(lines(OpenApiGate.judge(VALID, failures.get(0), List.of()))).containsExactly(
                "error baseline-document exit 102");
        assertThat(lines(OpenApiGate.judge(VALID, failures.get(1), List.of()))).containsExactly(
                "error document: the output is not a JSON object");
    }

    @Test
    void GIVEN_failing_lines_WHEN_rendered_for_a_failure_message_THEN_accepted_lines_are_left_out() {
        OpenApiGate.Outcome outcome = new OpenApiGate.Outcome(List.of(
                new OpenApiGate.Line(OpenApiGate.Kind.ACCEPTED, "finding a"),
                new OpenApiGate.Line(OpenApiGate.Kind.UNDECLARED, "finding b"),
                new OpenApiGate.Line(OpenApiGate.Kind.STALE, "finding c")), List.of(), List.of(), List.of());

        assertThat(OpenApiGate.failing(outcome)).isEqualTo("undeclared finding b\n  stale finding c");
    }
}
