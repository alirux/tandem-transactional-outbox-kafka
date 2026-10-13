package com.codingful.tandem.compatibility;

import java.io.Serializable;
import java.util.Collection;
import java.util.List;
import org.gradle.api.GradleException;
import org.gradle.api.artifacts.component.ComponentIdentifier;
import org.gradle.api.artifacts.component.ModuleComponentIdentifier;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;

/**
 * A component a baseline configuration resolved: a published module, or a project of this build.
 *
 * @param displayName how Gradle names it
 * @param project     whether this build's working tree answered it
 * @param group       its group; empty for a project
 * @param module      its module name; empty for a project
 * @param version     its version; empty for a project
 */
public record ResolvedComponent(String displayName, boolean project, String group, String module, String version)
        implements Serializable {

    static ResolvedComponent of(ComponentIdentifier id) {
        if (id instanceof ModuleComponentIdentifier module) {
            return new ResolvedComponent(id.getDisplayName(), false, module.getGroup(), module.getModule(),
                    module.getVersion());
        }
        return new ResolvedComponent(id.getDisplayName(), id instanceof ProjectComponentIdentifier, "", "", "");
    }

    /**
     * Fails unless every Tandem component a baseline configuration resolved is a published module at the
     * baseline (LLD-api-stability §2.2). A baseline configuration left out of the root build's
     * {@code centralClasspaths} resolves to the working tree, and its gate would then compare the working
     * tree with itself and pass.
     */
    public static void verify(String baseline, Collection<ResolvedComponent> components) {
        List<String> wrong = components.stream()
                .filter(component -> component.project()
                        || component.group().equals(CompatibilityBaseline.GROUP)
                        && !component.version().equals(baseline))
                .map(ResolvedComponent::displayName)
                .distinct()
                .toList();
        if (!wrong.isEmpty()) {
            throw new GradleException("A baseline configuration resolved Tandem components other than the published "
                    + "modules at " + baseline + ": " + String.join(", ", wrong) + ". Every baseline configuration "
                    + "must be listed in centralClasspaths in the root build.gradle.kts, so the working tree is never "
                    + "substituted for it.");
        }
    }
}
