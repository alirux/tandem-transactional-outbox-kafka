package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ShippedChangesetsTest {

    private static final String V1 = "v1-baseline.sql";
    private static final String V2 = "v2-replays.sql";
    private static final String V3 = "v3-managed-seq.sql";
    private static final String V1_SQL = "--liquibase formatted sql\n--changeset tandem:v1\nCREATE TABLE t (id BIGINT);\n";
    private static final String V2_SQL = "--liquibase formatted sql\n--changeset tandem:v2\nALTER TABLE t ADD COLUMN r INT;\n";
    private static final String V3_SQL = "--liquibase formatted sql\n--changeset tandem:v3\nCREATE SEQUENCE s;\n";

    @TempDir
    Path dir;

    private Path current;

    @BeforeEach
    void createTheWorkingTreeChangelog() throws IOException {
        current = Files.createDirectories(dir.resolve("changelog"));
    }

    private static String master(String... includes) {
        StringBuilder xml = new StringBuilder("""
                <databaseChangeLog
                        xmlns="http://www.liquibase.org/xml/ns/dbchangelog"
                        xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
                        xsi:schemaLocation="http://www.liquibase.org/xml/ns/dbchangelog
                                            http://www.liquibase.org/xml/ns/dbchangelog/dbchangelog-latest.xsd">
                    <!-- A comment the comparison ignores. -->
                """);
        for (String include : includes) {
            xml.append("    ").append(include).append('\n');
        }
        return xml.append("</databaseChangeLog>\n").toString();
    }

    private static String include(String file) {
        return "<include file=\"" + file + "\" relativeToChangelogFile=\"true\"/>";
    }

    /** The baseline jar: every entry under the changelog's published path, plus whatever else a jar holds. */
    private File baselineJar(Map<String, String> changelog) throws IOException {
        File jar = dir.resolve("tandem-jdbc-baseline.jar").toFile();
        try (ZipOutputStream zip = new ZipOutputStream(Files.newOutputStream(jar.toPath()))) {
            put(zip, "com/codingful/tandem/jdbc/JdbcOutboxStore.class", "not compared");
            for (Map.Entry<String, String> file : changelog.entrySet()) {
                put(zip, ShippedChangesets.JAR_DIRECTORY + file.getKey(), file.getValue());
            }
        }
        return jar;
    }

    private static void put(ZipOutputStream zip, String name, String content) throws IOException {
        zip.putNextEntry(new ZipEntry(name));
        zip.write(content.getBytes(StandardCharsets.UTF_8));
        zip.closeEntry();
    }

    private void writeCurrent(String name, String content) throws IOException {
        Files.writeString(current.resolve(name), content, StandardCharsets.UTF_8);
    }

    private File shippedV1AndV2() throws IOException {
        Map<String, String> changelog = new LinkedHashMap<>();
        changelog.put(ShippedChangesets.MASTER, master(include(V1), include(V2)));
        changelog.put(V1, V1_SQL);
        changelog.put(V2, V2_SQL);
        changelog.put("README.txt", "not a changeset, not compared");
        return baselineJar(changelog);
    }

    @Test
    void GIVEN_every_shipped_changeset_unchanged_and_a_new_one_appended_WHEN_compared_THEN_it_passes() throws IOException {
        File jar = shippedV1AndV2();
        writeCurrent(V1, V1_SQL);
        writeCurrent(V2, V2_SQL);
        writeCurrent(V3, V3_SQL);
        writeCurrent(ShippedChangesets.MASTER, master(include(V1), include(V2), include(V3)));

        ShippedChangesets.Comparison comparison = ShippedChangesets.compare(jar, current);

        assertThat(comparison.problems()).isEmpty();
        assertThat(comparison.shipped()).containsExactly(V1, V2);
    }

    @Test
    void GIVEN_a_shipped_changeset_with_one_byte_changed_WHEN_compared_THEN_it_fails_naming_it() throws IOException {
        File jar = shippedV1AndV2();
        writeCurrent(V1, V1_SQL);
        writeCurrent(V2, V2_SQL.replace("INT", "INT "));
        writeCurrent(ShippedChangesets.MASTER, master(include(V1), include(V2)));

        assertThat(ShippedChangesets.compare(jar, current).problems()).singleElement().asString()
                .startsWith(V2).contains("differs");
    }

    @Test
    void GIVEN_a_shipped_changeset_removed_WHEN_compared_THEN_it_fails_naming_it() throws IOException {
        File jar = shippedV1AndV2();
        writeCurrent(V2, V2_SQL);
        writeCurrent(ShippedChangesets.MASTER, master(include(V1), include(V2)));

        assertThat(ShippedChangesets.compare(jar, current).problems()).singleElement().asString()
                .startsWith(V1).contains("missing");
    }

    @Test
    void GIVEN_a_master_that_no_longer_starts_with_the_baseline_includes_WHEN_compared_THEN_it_fails_at_the_first_difference()
            throws IOException {
        File jar = shippedV1AndV2();
        writeCurrent(V1, V1_SQL);
        writeCurrent(V2, V2_SQL);
        writeCurrent(V3, V3_SQL);

        writeCurrent(ShippedChangesets.MASTER, master(include(V1), include(V3), include(V2)));
        assertThat(ShippedChangesets.compare(jar, current).problems()).singleElement().asString()
                .contains("entry 2").contains(V2).contains(V3);

        writeCurrent(ShippedChangesets.MASTER, master(include(V1)));
        assertThat(ShippedChangesets.compare(jar, current).problems()).singleElement().asString()
                .contains("entry 2").contains(V2).contains("absent");

        writeCurrent(ShippedChangesets.MASTER, master(include(V1), "<include file=\"" + V2 + "\"/>"));
        assertThat(ShippedChangesets.compare(jar, current).problems()).singleElement().asString()
                .contains("entry 2").contains("relativeToChangelogFile");
    }

    @Test
    void GIVEN_no_master_on_either_side_WHEN_compared_THEN_it_fails() throws IOException {
        File withoutMaster = baselineJar(Map.of(V1, V1_SQL));
        writeCurrent(V1, V1_SQL);
        assertThat(ShippedChangesets.compare(withoutMaster, current).problems()).singleElement().asString()
                .contains(ShippedChangesets.JAR_DIRECTORY + ShippedChangesets.MASTER);

        File withMaster = baselineJar(Map.of(ShippedChangesets.MASTER, master(include(V1)), V1, V1_SQL));
        assertThat(ShippedChangesets.compare(withMaster, current).problems()).singleElement().asString()
                .isEqualTo(ShippedChangesets.MASTER + " is missing");
    }

    @Test
    void GIVEN_a_baseline_archive_that_is_not_a_jar_WHEN_compared_THEN_the_gate_fails_naming_it() throws IOException {
        File notAJar = dir.resolve("tandem-jdbc-baseline.jar").toFile();
        Files.writeString(notAJar.toPath(), "not a zip archive", StandardCharsets.UTF_8);

        assertThatThrownBy(() -> ShippedChangesets.compare(notAJar, current))
                .isInstanceOf(UncheckedIOException.class)
                .hasMessageContaining(notAJar.toString());
    }

    @Test
    void GIVEN_a_master_with_other_elements_WHEN_read_THEN_its_include_and_includeAll_entries_count_with_their_attributes() {
        String xml = master("<property name=\"schema\" value=\"public\"/>", include(V1),
                "<includeAll path=\"later/\" relativeToChangelogFile=\"true\"/>");

        assertThat(ShippedChangesets.includes(xml.getBytes(StandardCharsets.UTF_8))).containsExactly(
                "include file=\"" + V1 + "\" relativeToChangelogFile=\"true\"",
                "includeAll path=\"later/\" relativeToChangelogFile=\"true\"");
    }

    @Test
    void GIVEN_a_master_that_does_not_parse_WHEN_compared_THEN_the_gate_fails_rather_than_reading_it() throws IOException {
        File jar = shippedV1AndV2();
        writeCurrent(V1, V1_SQL);
        writeCurrent(V2, V2_SQL);
        try (OutputStream out = Files.newOutputStream(current.resolve(ShippedChangesets.MASTER))) {
            out.write(("<!DOCTYPE x [<!ENTITY e SYSTEM \"file:///etc/passwd\">]>" + master(include(V1)))
                    .getBytes(StandardCharsets.UTF_8));
        }

        assertThatThrownBy(() -> ShippedChangesets.compare(jar, current))
                .isInstanceOf(GradleException.class)
                .hasMessageContaining(ShippedChangesets.MASTER);
    }
}
