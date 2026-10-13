package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.file.Path;
import org.junit.jupiter.api.Test;

class OasdiffTest {

    private static final Path REPOSITORY = Path.of("/home/runner/work/tandem");
    private static final Path WORK = REPOSITORY.resolve("libs/tandem-admin/build/openapi");
    private static final Oasdiff OASDIFF = new Oasdiff("tufin/oasdiff:v1.33.0", REPOSITORY, WORK);

    @Test
    void GIVEN_files_of_the_two_mounted_directories_WHEN_handed_to_the_container_THEN_each_is_named_where_it_is_mounted() {
        assertThat(OASDIFF.inContainer(REPOSITORY.resolve("docs/admin-api.openapi.yaml")))
                .isEqualTo(Oasdiff.REPOSITORY + "/docs/admin-api.openapi.yaml");
        assertThat(OASDIFF.inContainer(WORK.resolve("baseline.yaml"))).isEqualTo(Oasdiff.WORK + "/baseline.yaml");
        assertThat(OASDIFF.inContainer(WORK.resolve("../openapi/./severity-levels.txt")))
                .isEqualTo(Oasdiff.WORK + "/severity-levels.txt");
    }

    @Test
    void GIVEN_a_file_outside_both_mounted_directories_WHEN_handed_to_the_container_THEN_it_is_refused() {
        assertThatThrownBy(() -> OASDIFF.inContainer(Path.of("/tmp/elsewhere.yaml")))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("/tmp/elsewhere.yaml");
    }
}
