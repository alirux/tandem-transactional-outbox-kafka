package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.util.List;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;

class ResolvedComponentTest {

    private static final String BASELINE = "0.11.1";

    private static ResolvedComponent published(String group, String module, String version) {
        return new ResolvedComponent(group + ":" + module + ":" + version, false, group, module, version);
    }

    @Test
    void GIVEN_Tandem_modules_at_the_baseline_and_third_party_libraries_WHEN_verified_THEN_they_pass() {
        assertThatCode(() -> ResolvedComponent.verify(BASELINE, List.of(
                published(CompatibilityBaseline.GROUP, "tandem-core", BASELINE),
                published("org.apache.kafka", "kafka-clients", "3.9.2")))).doesNotThrowAnyException();
    }

    @Test
    void GIVEN_the_working_tree_was_substituted_for_the_baseline_WHEN_verified_THEN_the_gate_fails() {
        ResolvedComponent project = new ResolvedComponent("project :tandem-core", true, "", "", "");

        assertThatThrownBy(() -> ResolvedComponent.verify(BASELINE, List.of(project)))
                .isInstanceOf(GradleException.class)
                .hasMessageContaining("project :tandem-core")
                .hasMessageContaining("centralClasspaths");
    }

    @Test
    void GIVEN_a_Tandem_module_at_another_version_WHEN_verified_THEN_the_gate_fails() {
        ResolvedComponent other = published(CompatibilityBaseline.GROUP, "tandem-core", "0.12.0");

        assertThatThrownBy(() -> ResolvedComponent.verify(BASELINE, List.of(other)))
                .isInstanceOf(GradleException.class)
                .hasMessageContaining(other.displayName());
    }
}
