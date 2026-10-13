package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import groovy.json.JsonOutput;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class SpringMetadataTest {

    private static final String MODULE = "tandem-spring-relay";
    private static final String ENABLED = "tandem.relay.enabled";
    private static final String BATCH_SIZE = "tandem.relay.batch-size";
    private static final String WAKEUP = "tandem.outbox.wakeup";
    private static final String NEW_KEY = "tandem.relay.new-key";
    private static final String BOOLEAN = "java.lang.Boolean";
    private static final String INTEGER = "java.lang.Integer";
    private static final String LONG = "java.lang.Long";
    private static final String STRING = "java.lang.String";
    private static final String WAKEUP_TYPE = "com.codingful.tandem.jdbc.Wakeup";
    private static final String NONE = "none";
    private static final String PG_NOTIFY = "pg-notify";

    /** A property as the configuration processor writes it, with the facts the gate ignores filled in. */
    private static Map<String, Object> property(String name, String type, Object defaultValue) {
        Map<String, Object> property = new LinkedHashMap<>();
        property.put("name", name);
        if (type != null) {
            property.put("type", type);
        }
        property.put("description", "What " + name + " does.");
        property.put("sourceType", "com.codingful.tandem.spring.relay.TandemRelayProperties");
        if (defaultValue != null) {
            property.put("defaultValue", defaultValue);
        }
        return property;
    }

    private static Map<String, Object> deprecated(Map<String, Object> property, String level) {
        Map<String, Object> deprecated = new LinkedHashMap<>(property);
        deprecated.put("deprecation", Map.of("level", level, "replacement", NEW_KEY));
        return deprecated;
    }

    private static Map<String, SpringMetadata.Property> metadata(Map<?, ?>... properties) {
        return SpringMetadata.parse(JsonOutput.toJson(Map.of(
                "groups", List.of(Map.of("name", "tandem.relay", "type", "TandemRelayProperties")),
                "properties", List.of(properties),
                "hints", List.of())), "test metadata");
    }

    /** The baseline of most tests: a property with a declared default, one without, one of an enum type. */
    private static final Map<String, SpringMetadata.Property> BASELINE = metadata(
            property(ENABLED, BOOLEAN, true),
            property(BATCH_SIZE, INTEGER, null),
            property(WAKEUP, WAKEUP_TYPE, NONE));

    private static DeclaredBreaks.Entry declared(String key) {
        return new DeclaredBreaks.Entry(SpringMetadata.GATE, MODULE, key, key + " is gone.", 3);
    }

    private static SpringMetadata.Outcome judge(Map<String, SpringMetadata.Property> current,
            DeclaredBreaks.Entry... declarations) {
        return SpringMetadata.judge(MODULE, BASELINE, current, List.of(declarations));
    }

    @Test
    void GIVEN_the_same_keys_types_and_defaults_with_other_descriptions_groups_and_hints_WHEN_compared_THEN_it_passes() {
        Map<String, Object> enabled = property(ENABLED, BOOLEAN, true);
        enabled.put("description", "Reworded.");
        enabled.put("sourceType", "com.codingful.tandem.spring.relay.Elsewhere");
        Map<String, SpringMetadata.Property> current = SpringMetadata.parse(JsonOutput.toJson(Map.of(
                "properties", List.of(enabled, property(BATCH_SIZE, INTEGER, null), property(WAKEUP, WAKEUP_TYPE, NONE)),
                "hints", List.of(Map.of("name", WAKEUP, "values", List.of(Map.of("value", NONE)))))), "test metadata");

        SpringMetadata.Outcome outcome = judge(current);

        assertThat(outcome.problems()).isEmpty();
        assertThat(outcome.accepted()).isEmpty();
        assertThat(outcome.compared()).isEqualTo(BASELINE.size());
    }

    @Test
    void GIVEN_a_new_property_and_a_default_declared_where_there_was_none_WHEN_compared_THEN_it_passes() {
        SpringMetadata.Outcome outcome = judge(metadata(
                property(ENABLED, BOOLEAN, true),
                property(BATCH_SIZE, INTEGER, 500),
                property(WAKEUP, WAKEUP_TYPE, NONE),
                property(NEW_KEY, BOOLEAN, false)));

        assertThat(outcome.problems()).isEmpty();
    }

    @Test
    void GIVEN_a_type_declared_where_the_baseline_declared_none_WHEN_compared_THEN_it_passes() {
        SpringMetadata.Outcome outcome = SpringMetadata.judge(MODULE, metadata(property(BATCH_SIZE, null, null)),
                metadata(property(BATCH_SIZE, INTEGER, null)), List.of());

        assertThat(outcome.problems()).isEmpty();
    }

    @Test
    void GIVEN_a_baseline_property_absent_WHEN_compared_THEN_it_fails_naming_the_key_and_the_declaration_to_write() {
        SpringMetadata.Outcome outcome = judge(metadata(
                property(ENABLED, BOOLEAN, true),
                property(WAKEUP, WAKEUP_TYPE, NONE),
                property(NEW_KEY, INTEGER, null)));

        assertThat(outcome.problems()).singleElement().asString()
                .startsWith(BATCH_SIZE + " is absent")
                .contains(SpringMetadata.GATE + " | " + MODULE + " | " + BATCH_SIZE + " | <release-notes line>");
    }

    @Test
    void GIVEN_a_baseline_property_declared_removed_WHEN_compared_THEN_it_fails_while_a_warning_passes() {
        SpringMetadata.Outcome removed = judge(metadata(
                deprecated(property(ENABLED, BOOLEAN, true), "error"),
                property(BATCH_SIZE, INTEGER, null),
                property(WAKEUP, WAKEUP_TYPE, NONE)));
        SpringMetadata.Outcome warned = judge(metadata(
                deprecated(property(ENABLED, BOOLEAN, true), "warning"),
                property(BATCH_SIZE, INTEGER, null),
                property(WAKEUP, WAKEUP_TYPE, NONE)));

        assertThat(removed.problems()).singleElement().asString()
                .startsWith(ENABLED + " carries a deprecation of level error");
        assertThat(warned.problems()).isEmpty();
    }

    @Test
    void GIVEN_a_baseline_property_with_another_type_or_none_WHEN_compared_THEN_it_fails_naming_both_types() {
        SpringMetadata.Outcome outcome = judge(metadata(
                property(ENABLED, BOOLEAN, true),
                property(BATCH_SIZE, LONG, null),
                property(WAKEUP, null, NONE)));

        assertThat(outcome.problems()).hasSize(2);
        assertThat(outcome.problems().get(0)).startsWith(WAKEUP + " had type " + WAKEUP_TYPE + ", now none");
        assertThat(outcome.problems().get(1)).startsWith(BATCH_SIZE + " had type " + INTEGER + ", now " + LONG);
    }

    @Test
    void GIVEN_a_declared_default_changed_or_removed_WHEN_compared_THEN_it_fails_naming_both_defaults() {
        SpringMetadata.Outcome outcome = judge(metadata(
                property(ENABLED, BOOLEAN, null),
                property(BATCH_SIZE, INTEGER, null),
                property(WAKEUP, WAKEUP_TYPE, PG_NOTIFY)));

        assertThat(outcome.problems()).hasSize(2);
        assertThat(outcome.problems().get(0))
                .startsWith(WAKEUP + " declared default \"" + NONE + "\", now \"" + PG_NOTIFY + "\"");
        assertThat(outcome.problems().get(1)).startsWith(ENABLED + " declared default true, now none");
    }

    @Test
    void GIVEN_a_property_whose_type_and_default_both_changed_WHEN_compared_THEN_it_is_one_break_with_both_reasons() {
        SpringMetadata.Outcome outcome = judge(metadata(
                property(ENABLED, STRING, "true"),
                property(BATCH_SIZE, INTEGER, null),
                property(WAKEUP, WAKEUP_TYPE, NONE)));

        assertThat(outcome.problems()).singleElement().asString()
                .startsWith(ENABLED + " had type " + BOOLEAN + ", now " + STRING + "; declared default true, now \"true\"");
    }

    @Test
    void GIVEN_a_break_the_declared_breaks_name_WHEN_compared_THEN_it_passes_and_reports_the_declaration() {
        DeclaredBreaks.Entry declaration = declared(BATCH_SIZE);

        SpringMetadata.Outcome outcome = judge(metadata(
                property(ENABLED, BOOLEAN, true),
                property(WAKEUP, WAKEUP_TYPE, NONE)), declaration);

        assertThat(outcome.problems()).isEmpty();
        assertThat(outcome.accepted()).containsExactly(declaration);
    }

    @Test
    void GIVEN_a_declaration_for_a_property_the_metadata_keeps_WHEN_compared_THEN_it_fails_as_stale() {
        SpringMetadata.Outcome outcome = judge(BASELINE, declared(BATCH_SIZE));

        assertThat(outcome.problems()).singleElement().asString()
                .contains("matches no property")
                .contains(DeclaredBreaks.PATH + " line 3")
                .contains(BATCH_SIZE);
        assertThat(outcome.accepted()).isEmpty();
    }

    @Test
    void GIVEN_a_property_the_baseline_already_declared_removed_WHEN_now_absent_or_still_removed_THEN_it_is_not_compared() {
        Map<String, SpringMetadata.Property> baseline = metadata(
                deprecated(property(ENABLED, BOOLEAN, true), "error"),
                property(BATCH_SIZE, INTEGER, null));

        SpringMetadata.Outcome absent = SpringMetadata.judge(MODULE, baseline, metadata(property(BATCH_SIZE, INTEGER, null)),
                List.of());
        SpringMetadata.Outcome kept = SpringMetadata.judge(MODULE, baseline, baseline, List.of());

        assertThat(absent.problems()).isEmpty();
        assertThat(kept.problems()).isEmpty();
        assertThat(kept.compared()).isEqualTo(1);
    }

    @Test
    void GIVEN_a_key_declared_twice_WHEN_read_THEN_identical_declarations_are_one_and_different_ones_an_error() {
        Map<String, Object> wakeup = property(WAKEUP, WAKEUP_TYPE, NONE);

        assertThat(metadata(wakeup, wakeup)).containsOnlyKeys(WAKEUP);
        assertThatThrownBy(() -> metadata(wakeup, property(WAKEUP, WAKEUP_TYPE, PG_NOTIFY)))
                .isInstanceOf(GradleException.class)
                .hasMessageContaining(WAKEUP + " twice");
    }

    @Test
    void GIVEN_text_that_is_not_the_metadata_the_processor_writes_WHEN_read_THEN_it_is_an_error() {
        for (String text : List.of("{ not json", "[]", "{\"properties\": {}}", "{\"properties\": [\"" + ENABLED + "\"]}",
                "{\"properties\": [{\"type\": \"" + BOOLEAN + "\"}]}")) {
            assertThatThrownBy(() -> SpringMetadata.parse(text, "test metadata"))
                    .as(text)
                    .isInstanceOf(GradleException.class)
                    .hasMessageStartingWith("Cannot read test metadata");
        }
    }

    @Test
    void GIVEN_metadata_with_groups_only_WHEN_read_THEN_it_declares_no_property() {
        assertThat(SpringMetadata.parse("{\"groups\": []}", "test metadata")).isEmpty();
    }

    @Test
    void GIVEN_a_jar_WHEN_read_THEN_its_metadata_is_the_file_at_the_published_path_and_a_jar_without_one_or_corrupt_is_an_error(
            @TempDir Path dir) throws IOException {
        File withMetadata = jar(dir.resolve("with.jar"), SpringMetadata.PATH,
                JsonOutput.toJson(Map.of("properties", List.of(property(ENABLED, BOOLEAN, true)))));
        File without = jar(dir.resolve("without.jar"), "META-INF/additional-spring-configuration-metadata.json",
                JsonOutput.toJson(Map.of("properties", List.of(property(ENABLED, BOOLEAN, true)))));
        File notAJar = Files.writeString(dir.resolve("truncated.jar"), "not a zip archive").toFile();

        assertThat(SpringMetadata.read(withMetadata)).containsOnlyKeys(ENABLED);
        assertThatThrownBy(() -> SpringMetadata.read(without))
                .isInstanceOf(GradleException.class)
                .hasMessageContaining("carries no " + SpringMetadata.PATH);
        assertThatThrownBy(() -> SpringMetadata.read(notAJar))
                .isInstanceOf(GradleException.class)
                .hasMessageStartingWith("Cannot read " + SpringMetadata.PATH);
    }

    private static File jar(Path path, String entry, String content) throws IOException {
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(path))) {
            zip.putNextEntry(new ZipEntry(entry));
            zip.write(content.getBytes(StandardCharsets.UTF_8));
            zip.closeEntry();
        }
        return path.toFile();
    }
}
