package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import java.util.Set;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;

class DeclaredBreaksTest {

    private static final String BASELINE = "1.0.0-rc.1";
    private static final String CORE = "tandem-core";
    private static final String PORT = "com.acme.Port";
    private static final String HEADER = "# Breaks accepted against the baseline below.\n";

    private static String file(String baselineLine, String... entries) {
        return HEADER + baselineLine + "\n" + String.join("\n", entries) + "\n";
    }

    @Test
    void GIVEN_a_file_without_entries_WHEN_bound_to_any_baseline_THEN_it_is_valid() {
        for (String text : List.of(HEADER, file("baseline"), file("baseline 0.9.0"))) {
            for (String baseline : List.of("", BASELINE)) {
                assertThat(DeclaredBreaks.parse(text).boundTo(baseline).entries("interface-kind", CORE)).isEmpty();
            }
        }
    }

    @Test
    void GIVEN_entries_written_against_the_baseline_WHEN_a_gate_reads_them_THEN_it_gets_its_own_kind_for_its_own_module() {
        DeclaredBreaks breaks = DeclaredBreaks.parse(file("baseline " + BASELINE,
                "interface-kind | " + CORE + " | " + PORT + " | Port is provided.",
                "interface-kind | tandem-jdbc | com.acme.Other | Other is provided.",
                "bom | tandem-bom | tandem-old | tandem-old is no longer managed.")).boundTo(BASELINE);

        assertThat(breaks.entries("interface-kind", CORE)).singleElement().satisfies(entry -> {
            assertThat(entry.item()).isEqualTo(PORT);
            assertThat(entry.releaseNote()).isEqualTo("Port is provided.");
            assertThat(entry.line()).isEqualTo(3);
        });
        assertThat(breaks.entries("bom", "tandem-bom")).extracting(DeclaredBreaks.Entry::item).containsExactly("tandem-old");
    }

    @Test
    void GIVEN_a_release_notes_line_holding_a_bar_WHEN_parsed_THEN_the_line_is_kept_whole() {
        DeclaredBreaks breaks = DeclaredBreaks.parse(file("baseline " + BASELINE,
                "interface-kind | " + CORE + " | " + PORT + " | Port: read | write split.")).boundTo(BASELINE);

        assertThat(breaks.entries("interface-kind", CORE)).singleElement()
                .extracting(DeclaredBreaks.Entry::releaseNote).isEqualTo("Port: read | write split.");
    }

    @Test
    void GIVEN_a_file_written_with_CRLF_WHEN_parsed_THEN_its_entries_read_as_with_LF() {
        String text = file("baseline " + BASELINE, "interface-kind | " + CORE + " | " + PORT + " | Note.")
                .replace("\n", "\r\n");

        assertThat(DeclaredBreaks.parse(text).boundTo(BASELINE).entries("interface-kind", CORE))
                .extracting(DeclaredBreaks.Entry::item).containsExactly(PORT);
    }

    @Test
    void GIVEN_entries_bound_to_another_baseline_WHEN_the_baseline_has_moved_THEN_every_gate_fails() {
        DeclaredBreaks other = DeclaredBreaks.parse(file("baseline 0.11.1", "bom | tandem-bom | tandem-old | Note."));
        DeclaredBreaks none = DeclaredBreaks.parse(HEADER + "bom | tandem-bom | tandem-old | Note.\n");

        assertThatThrownBy(() -> other.boundTo(BASELINE)).isInstanceOf(GradleException.class)
                .hasMessageContaining("0.11.1").hasMessageContaining(BASELINE);
        assertThatThrownBy(() -> none.boundTo(BASELINE)).isInstanceOf(GradleException.class);
    }

    @Test
    void GIVEN_entries_WHEN_no_baseline_is_set_THEN_every_gate_fails() {
        DeclaredBreaks breaks = DeclaredBreaks.parse(file("baseline", "bom | tandem-bom | tandem-old | Note."));

        assertThatThrownBy(() -> breaks.boundTo("")).isInstanceOf(GradleException.class)
                .hasMessageContaining("no compatibility baseline");
    }

    @Test
    void GIVEN_lines_that_break_the_format_WHEN_parsed_THEN_the_file_is_rejected_naming_the_line() {
        List<String> invalid = List.of(
                file("baseline " + BASELINE, "interface-kind | " + CORE + " | " + PORT),
                file("baseline " + BASELINE, "interface-kind | " + CORE + " |  | Note."),
                file("baseline " + BASELINE, "japi | " + CORE + " | " + PORT + " | Note."),
                file("baseline " + BASELINE, "baseline 0.11.1"),
                file("baseline " + BASELINE, "schema-compatibility | tandem-jdbc | tandem_outbox | Note."),
                file("baseline " + BASELINE, "openapi | tandem-admin | response-property-removed | Note."),
                file("baseline " + BASELINE, "bom | tandem-bom | tandem-old | Note.", "bom | tandem-bom | tandem-old | Again."));

        for (String text : invalid) {
            assertThatThrownBy(() -> DeclaredBreaks.parse(text)).as(text).isInstanceOf(GradleException.class)
                    .hasMessageContaining("line ");
        }
    }

    @Test
    void GIVEN_the_items_each_gate_accepts_WHEN_parsed_THEN_the_file_is_valid() {
        DeclaredBreaks breaks = DeclaredBreaks.parse(file("baseline " + BASELINE,
                "schema-compatibility | tandem-jdbc | * | Apply the migration first.",
                "openapi | tandem-admin | finding 4f2a | A field is required.",
                "openapi | tandem-admin | problem-type outbox-not-found | Renamed.",
                "openapi | tandem-admin | server document {basePath}/v1 | Moved.")).boundTo(BASELINE);

        assertThat(breaks.entries("openapi", "tandem-admin")).hasSize(3);
        assertThat(breaks.entries("schema-compatibility", "tandem-jdbc")).hasSize(1);
    }

    @Test
    void GIVEN_an_entry_naming_a_module_without_its_gate_WHEN_a_gate_reads_the_file_THEN_it_fails() {
        DeclaredBreaks breaks = DeclaredBreaks.parse(file("baseline " + BASELINE,
                "interface-kind | tandem-cor | " + PORT + " | Note."));

        assertThatThrownBy(() -> breaks.requireGateModules("interface-kind", Set.of(CORE, "tandem-jdbc")))
                .isInstanceOf(GradleException.class).hasMessageContaining("tandem-cor |");
        assertThat(breaks.requireGateModules("bom", Set.of("tandem-bom"))).isSameAs(breaks);
    }

    @Test
    void GIVEN_entries_naming_modules_that_run_their_gate_WHEN_a_gate_reads_the_file_THEN_they_are_accepted() {
        DeclaredBreaks breaks = DeclaredBreaks.parse(file("baseline " + BASELINE,
                "interface-kind | " + CORE + " | " + PORT + " | Note.",
                "interface-kind | tandem-jdbc | com.acme.Other | Note."));

        assertThat(breaks.requireGateModules("interface-kind", Set.of(CORE, "tandem-jdbc"))).isSameAs(breaks);
    }

    @Test
    void GIVEN_reported_breaks_and_declarations_WHEN_matched_THEN_each_is_accepted_undeclared_or_stale() {
        DeclaredBreaks.Entry declaredAndReported = new DeclaredBreaks.Entry("bom", "tandem-bom", "tandem-a", "A.", 2);
        DeclaredBreaks.Entry declaredOnly = new DeclaredBreaks.Entry("bom", "tandem-bom", "tandem-b", "B.", 3);

        DeclaredBreaks.Matching matching = DeclaredBreaks.match(List.of("tandem-a", "tandem-c"),
                List.of(declaredAndReported, declaredOnly));

        assertThat(matching.accepted()).containsOnlyKeys("tandem-a").containsValue(declaredAndReported);
        assertThat(matching.undeclared()).containsExactly("tandem-c");
        assertThat(matching.stale()).containsExactly(declaredOnly);
    }
}
