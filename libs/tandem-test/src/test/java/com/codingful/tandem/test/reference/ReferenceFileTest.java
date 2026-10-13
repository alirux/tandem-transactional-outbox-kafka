package com.codingful.tandem.test.reference;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ReferenceFileTest {

    private static final String FILE_NAME = "sample.txt";
    private static final String COMMAND = "./gradlew :sample-module:test -PupdateReferenceFiles";
    private static final String COMMENT = "# Rendered by ReferenceFileTest; regenerate with " + COMMAND;

    @TempDir
    Path workspace;

    private Path module;
    private Path committed;

    @BeforeEach
    void aModuleWithTestSources() throws IOException {
        module = workspace.resolve("sample-module");
        Files.createDirectories(module.resolve("src/test/java"));
        committed = module.resolve("src/test/resources/reference").resolve(FILE_NAME);
    }

    @Test
    void GIVEN_a_rendering_equal_to_the_committed_file_WHEN_compared_THEN_it_passes() throws IOException {
        commit(COMMENT + "\nalpha=1\nbeta=2\n");

        reference(false).assertMatches(List.of("alpha=1", "beta=2"));
    }

    @Test
    void GIVEN_a_changed_value_WHEN_compared_THEN_it_fails_naming_the_file_and_the_regeneration_command()
            throws IOException {
        commit(COMMENT + "\nalpha=1\nbeta=2\n");

        assertThatThrownBy(() -> reference(false).assertMatches(List.of("alpha=1", "beta=20")))
                .isInstanceOf(AssertionError.class)
                .hasMessageContaining("src/test/resources/reference/sample.txt")
                .hasMessageContaining(COMMAND)
                .hasMessageContaining("beta=20");
    }

    @Test
    void GIVEN_update_mode_WHEN_the_rendering_differs_or_the_file_is_missing_THEN_the_file_is_written()
            throws IOException {
        reference(true).assertMatches(List.of("alpha=1"));
        assertThat(Files.readString(committed, StandardCharsets.UTF_8)).isEqualTo(COMMENT + "\nalpha=1\n");

        reference(true).assertMatches(List.of("alpha=2"));
        assertThat(Files.readString(committed, StandardCharsets.UTF_8)).isEqualTo(COMMENT + "\nalpha=2\n");
    }

    private ReferenceFile reference(boolean update) {
        return ReferenceFile.of(module, FILE_NAME, ReferenceFileTest.class, update);
    }

    private void commit(String content) throws IOException {
        Files.createDirectories(committed.getParent());
        Files.writeString(committed, content, StandardCharsets.UTF_8);
    }
}
