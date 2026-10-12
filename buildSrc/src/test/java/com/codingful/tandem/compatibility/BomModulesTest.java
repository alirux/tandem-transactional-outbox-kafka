package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class BomModulesTest {

    private static final String BOM = "tandem-bom";
    private static final String CORE = "tandem-core";
    private static final String JDBC = "tandem-jdbc";
    private static final String NEW = "tandem-cloudevents";

    private static DeclaredBreaks.Entry declared(String module) {
        return new DeclaredBreaks.Entry(BomModules.GATE, BOM, module, module + " moved to a version of its own.", 3);
    }

    @Test
    void GIVEN_a_published_BOM_WHEN_read_THEN_its_managed_modules_are_the_Tandem_ones_in_dependency_management(
            @TempDir Path dir) throws IOException {
        File pom = dir.resolve("tandem-bom.pom").toFile();
        Files.writeString(pom.toPath(), """
                <?xml version="1.0" encoding="UTF-8"?>
                <project xmlns="http://maven.apache.org/POM/4.0.0">
                  <groupId>com.codingful</groupId>
                  <artifactId>tandem-bom</artifactId>
                  <dependencyManagement>
                    <dependencies>
                      <dependency><groupId>com.codingful</groupId><artifactId>%s</artifactId><version>1</version></dependency>
                      <dependency><groupId>org.other</groupId><artifactId>other-lib</artifactId><version>1</version></dependency>
                      <dependency><groupId>com.codingful</groupId><artifactId>%s</artifactId><version>1</version></dependency>
                    </dependencies>
                  </dependencyManagement>
                  <dependencies>
                    <dependency><groupId>com.codingful</groupId><artifactId>tandem-unmanaged</artifactId></dependency>
                  </dependencies>
                </project>
                """.formatted(JDBC, CORE));

        assertThat(BomModules.managed(pom)).containsExactly(CORE, JDBC);
    }

    @Test
    void GIVEN_the_current_BOM_manages_every_baseline_module_WHEN_compared_THEN_it_passes_even_with_new_modules() {
        assertThat(BomModules.problems(BOM, Set.of(CORE, JDBC), Set.of(CORE, JDBC), List.of())).isEmpty();
        assertThat(BomModules.problems(BOM, Set.of(CORE), Set.of(CORE, NEW), List.of())).isEmpty();
    }

    @Test
    void GIVEN_a_baseline_module_the_current_BOM_no_longer_manages_WHEN_undeclared_THEN_the_check_fails_with_the_declaration_to_write() {
        List<String> problems = BomModules.problems(BOM, Set.of(CORE, JDBC), Set.of(CORE), List.of());

        assertThat(problems).singleElement().asString()
                .startsWith(JDBC)
                .contains(BomModules.GATE + " | " + BOM + " | " + JDBC);
    }

    @Test
    void GIVEN_a_baseline_module_the_current_BOM_no_longer_manages_WHEN_declared_THEN_it_passes() {
        assertThat(BomModules.problems(BOM, Set.of(CORE, JDBC), Set.of(CORE), List.of(declared(JDBC)))).isEmpty();
    }

    @Test
    void GIVEN_a_declaration_for_a_module_still_managed_WHEN_compared_THEN_it_fails_as_stale() {
        List<String> problems = BomModules.problems(BOM, Set.of(CORE, JDBC), Set.of(CORE, JDBC), List.of(declared(JDBC)));

        assertThat(problems).singleElement().asString().contains("matches no module").contains(JDBC);
    }
}
