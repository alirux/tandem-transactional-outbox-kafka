package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class TandemTestContainerSchemaTest {

    @Test
    void GIVEN_a_consumer_project_outside_this_repository_WHEN_the_container_applies_the_schema_THEN_the_baseline_comes_from_the_published_jar()
            throws IOException {
        try (InputStream shipped = TandemTestContainer.class.getResourceAsStream(TandemTestContainer.BASELINE_RESOURCE)) {
            assertThat(shipped).as("baseline schema on the classpath").isNotNull();
            assertThat(new String(shipped.readAllBytes(), StandardCharsets.UTF_8)).isEqualTo(committedBaseline());
        }
    }

    /** The committed baseline, found from the repository root: only this test may assume a checkout. */
    private static String committedBaseline() throws IOException {
        Path dir = Path.of("").toAbsolutePath();
        while (!Files.exists(dir.resolve("schema/postgres/tandem-baseline.sql"))) {
            dir = dir.getParent();
        }
        return Files.readString(dir.resolve("schema/postgres/tandem-baseline.sql"), StandardCharsets.UTF_8);
    }
}
