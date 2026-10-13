package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class FrozenNamesTest {

    private static final String TEMPLATE = "{basePath}/v1";
    private static final String BASE_PATH = "/tandem/admin";
    private static final String STATUS = "/relay/status";
    private static final String WORKERS = "/relay/workers";
    private static final String DOCUMENT_SERVER = FrozenNames.SERVER + TEMPLATE;
    private static final String PROBLEMS = "https://tandem.codingful.com/problems/";

    private static Map<String, Object> server(String url, Map<String, Object> variables) {
        return Map.of("url", url, "description", "Where the API lives", "variables", variables);
    }

    private static Map<String, Object> basePath(String defaultValue, List<String> values) {
        Map<String, Object> variable = new LinkedHashMap<>();
        variable.put("default", defaultValue);
        if (values != null) {
            variable.put("enum", values);
        }
        return Map.of("basePath", variable);
    }

    private static final Map<String, Object> V1 = server(TEMPLATE, basePath(BASE_PATH, null));
    private static final Map<String, Object> FIXED = Map.of("url", "/admin/v1");

    private static Map<String, Object> operation(Object... servers) {
        Map<String, Object> operation = new LinkedHashMap<>(Map.of("responses", Map.of()));
        if (servers.length > 0) {
            operation.put("servers", List.of(servers));
        }
        return operation;
    }

    /** A document with three operations under two paths, and the given document-level servers. */
    private static Map<String, Object> document(List<?> servers, Map<String, Object> status, Map<String, Object> workers) {
        Map<String, Object> document = new LinkedHashMap<>();
        if (servers != null) {
            document.put("servers", servers);
        }
        document.put("paths", Map.of(STATUS, status, WORKERS, workers));
        return document;
    }

    private static Map<String, Object> plain(List<?> servers) {
        return document(servers, Map.of("get", operation(), "post", operation()), Map.of("get", operation()));
    }

    @Test
    void GIVEN_identical_documents_WHEN_compared_THEN_no_name_is_lost() {
        assertThat(FrozenNames.compare(plain(List.of(V1)), plain(List.of(V1)))).isEmpty();
    }

    @Test
    void GIVEN_problem_type_urls_WHEN_one_slug_disappears_THEN_only_that_slug_is_reported() {
        Map<String, Object> baseline = Map.of("components", Map.of(
                "example", Map.of("type", PROBLEMS + "not-found"),
                "examples", List.of(PROBLEMS + "relay-coordination-unsupported", 409),
                "description", "Always " + PROBLEMS + "{slug}, never about:blank; see " + PROBLEMS + "internal-error."));
        Map<String, Object> current = Map.of("info", Map.of(
                "description", "Moved: " + PROBLEMS + "not-found and " + PROBLEMS + "internal-error-v2",
                "type", PROBLEMS + "relay-coordination-unsupported"));

        assertThat(FrozenNames.compare(baseline, current))
                .containsExactly(Map.entry(FrozenNames.PROBLEM_TYPE + "internal-error", "no longer appears in the document"));
        assertThat(FrozenNames.slugs(baseline)).containsExactlyInAnyOrder("not-found", "relay-coordination-unsupported",
                "internal-error");
    }

    @Test
    void GIVEN_the_document_server_default_changed_WHEN_compared_THEN_one_finding_names_the_definition() {
        Map<String, Object> moved = server(TEMPLATE, basePath("/other/admin", null));

        assertThat(FrozenNames.compare(plain(List.of(V1)), plain(List.of(moved))))
                .containsExactly(Map.entry(DOCUMENT_SERVER, "basePath default /tandem/admin, now /other/admin"));
    }

    @Test
    void GIVEN_a_server_variable_enum_WHEN_it_loses_a_value_or_appears_THEN_it_is_reported_and_growth_is_not() {
        Map<String, Object> two = server(TEMPLATE, basePath(BASE_PATH, List.of(BASE_PATH, "/admin")));
        Map<String, Object> one = server(TEMPLATE, basePath(BASE_PATH, List.of(BASE_PATH)));

        assertThat(FrozenNames.compare(plain(List.of(two)), plain(List.of(one)))).containsExactly(Map.entry(
                DOCUMENT_SERVER, "basePath enum [/tandem/admin, /admin], now [/tandem/admin], without [/admin]"));
        assertThat(FrozenNames.compare(plain(List.of(V1)), plain(List.of(one))))
                .containsExactly(Map.entry(DOCUMENT_SERVER, "basePath had no enum, now [/tandem/admin]"));
        assertThat(FrozenNames.compare(plain(List.of(one)), plain(List.of(two)))).isEmpty();
        assertThat(FrozenNames.compare(plain(List.of(one)), plain(List.of(V1)))).isEmpty();
    }

    @Test
    void GIVEN_servers_added_reordered_or_redescribed_WHEN_compared_THEN_nothing_is_reported() {
        Map<String, Object> redescribed = Map.of("url", TEMPLATE, "description", "Elsewhere",
                "variables", basePath(BASE_PATH, null));

        assertThat(FrozenNames.compare(plain(List.of(V1)), plain(List.of(V1, FIXED)))).isEmpty();
        assertThat(FrozenNames.compare(plain(List.of(V1, FIXED)), plain(List.of(FIXED, redescribed)))).isEmpty();
    }

    @Test
    void GIVEN_a_template_no_longer_offered_WHEN_compared_THEN_the_finding_lists_the_servers_left() {
        assertThat(FrozenNames.compare(plain(List.of(V1, FIXED)), plain(List.of(FIXED))))
                .containsExactly(Map.entry(DOCUMENT_SERVER, "no longer offered, the servers are /admin/v1"));
    }

    @Test
    void GIVEN_a_document_without_servers_WHEN_it_declares_some_THEN_the_default_slash_is_lost() {
        assertThat(FrozenNames.compare(plain(null), plain(List.of())))
                .as("an empty list is OpenAPI's default too").isEmpty();
        assertThat(FrozenNames.compare(plain(null), plain(List.of(V1)))).containsExactly(Map.entry(
                FrozenNames.SERVER + "/", "no longer offered, the servers are " + TEMPLATE));
    }

    @Test
    void GIVEN_paths_and_operations_declaring_servers_WHEN_listed_THEN_each_is_named_by_its_level() {
        Map<String, Object> workers = new LinkedHashMap<>(Map.of("get", operation()));
        workers.put("servers", List.of(V1));
        Map<String, Object> overriding = document(List.of(V1),
                Map.of("get", operation(FIXED), "post", operation()), workers);

        assertThat(FrozenNames.overrides(plain(List.of(V1)))).isEmpty();
        assertThat(FrozenNames.overrides(overriding)).containsExactlyInAnyOrder("operation GET " + STATUS,
                "path " + WORKERS);
    }

    @Test
    void GIVEN_a_template_listed_twice_WHEN_compared_THEN_its_first_definition_is_the_one_compared() {
        Map<String, Object> other = server(TEMPLATE, basePath("/other/admin", null));

        assertThat(FrozenNames.compare(plain(List.of(V1, other)), plain(List.of(V1)))).isEmpty();
        assertThat(FrozenNames.compare(plain(List.of(other, V1)), plain(List.of(V1)))).containsOnlyKeys(DOCUMENT_SERVER);
    }
}
