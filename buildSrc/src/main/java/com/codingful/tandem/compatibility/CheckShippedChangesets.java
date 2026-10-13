package com.codingful.tandem.compatibility;

import java.nio.file.Path;
import java.util.Set;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

/**
 * {@code checkShippedChangesets} (LLD-api-stability §5.1): every changeset the baseline release's
 * {@code tandem-jdbc} jar ships is unchanged in the working tree, and its changelog master still starts with
 * the baseline's includes. Nothing in the declared breaks accepts a difference; the file is still read, so
 * an invalid one fails this gate like every other.
 */
@CacheableTask
public abstract class CheckShippedChangesets extends DefaultTask implements BaselineGate {

    /** The module's jar at the baseline; empty when the module is not compared. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineArchive();

    /** The working tree's changelog, {@code schema/postgres/changelog}. */
    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getChangelog();

    /** The verdict, so the check stays up to date while none of its inputs changes. */
    @OutputFile
    public abstract RegularFileProperty getVerdict();

    /**
     * Registers the check in {@code tandem-jdbc}, whose baseline is already registered, as part of its
     * {@code check} and of {@code checkCompatibility}.
     *
     * @param changelog the working tree's changelog directory, the one the module's jar ships
     */
    public static TaskProvider<CheckShippedChangesets> register(Project project, Directory changelog) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        Configuration archive = baseline.configuration(CompatibilityBaseline.ARCHIVE);
        return BaselineGate.register(project, "checkShippedChangesets", CheckShippedChangesets.class,
                "Checks that every changeset the baseline release shipped is unchanged.", Set.of(project.getName()),
                task -> {
            task.getBaselineArchive().from(baseline.compared(archive));
            task.getBaselineComponents().addAll(baseline.comparedComponents(archive));
            task.getChangelog().set(changelog);
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/shipped-changesets.txt"));
        });
    }

    @TaskAction
    public void check() {
        declaredBreaks();
        Path verdict = getVerdict().get().getAsFile().toPath();
        if (!compared()) {
            BaselineGate.write(verdict, notCompared() + "\n");
            return;
        }
        Path changelog = getChangelog().get().getAsFile().toPath();
        ShippedChangesets.Comparison comparison =
                ShippedChangesets.compare(getBaselineArchive().getSingleFile(), changelog);
        if (!comparison.problems().isEmpty()) {
            throw new GradleException("Changelog of " + getModuleName().get() + " against " + getBaseline().get()
                    + " (LLD-api-stability §5.1), in " + changelog + ":\n  "
                    + String.join("\n  ", comparison.problems())
                    + "\nA shipped changeset never changes, and no declaration accepts it: restore it, and put the "
                    + "schema change in a new changeset appended to the master.");
        }
        BaselineGate.write(verdict, "Every changeset " + getBaseline().get() + " shipped is unchanged: "
                + comparison.shipped() + "\n");
    }
}
