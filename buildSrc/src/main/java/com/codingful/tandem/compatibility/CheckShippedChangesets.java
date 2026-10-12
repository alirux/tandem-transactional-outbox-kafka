package com.codingful.tandem.compatibility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
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
public abstract class CheckShippedChangesets extends DefaultTask {

    /** The baseline release; empty when none is set. */
    @Input
    public abstract Property<String> getBaseline();

    @Input
    public abstract Property<String> getModuleName();

    /** The baseline's BOM POM, which says whether the module is new; empty when no baseline is set. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineBom();

    /** The module's jar at the baseline; empty when the module is not compared. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineArchive();

    /** The working tree's changelog, {@code schema/postgres/changelog}. */
    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getChangelog();

    /** Why the module is not compared with the baseline; absent when it is. Derived from the inputs above. */
    @Internal
    public abstract Property<String> getSkipReason();

    /** What the baseline configurations resolved. Derived from the files above. */
    @Internal
    public abstract ListProperty<ResolvedComponent> getBaselineComponents();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getDeclaredBreaks();

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
        TaskProvider<CheckShippedChangesets> gate = project.getTasks().register("checkShippedChangesets",
                CheckShippedChangesets.class, task -> {
            task.setGroup("verification");
            task.setDescription("Checks that every changeset the baseline release shipped is unchanged.");
            task.getBaseline().set(baseline.version());
            task.getModuleName().set(project.getName());
            task.getBaselineBom().from(baseline.bom());
            task.getBaselineArchive().from(baseline.compared(archive));
            task.getChangelog().set(changelog);
            task.getSkipReason().set(baseline.skipReason());
            task.getBaselineComponents().addAll(baseline.components(baseline.bom()));
            task.getBaselineComponents().addAll(baseline.comparedComponents(archive));
            task.getDeclaredBreaks().set(CompatibilityBaseline.declaredBreaks(project));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/shipped-changesets.txt"));
        });
        CompatibilityBaseline.registerGate(project, gate);
        return gate;
    }

    @TaskAction
    public void check() {
        String baseline = getBaseline().get();
        String module = getModuleName().get();
        DeclaredBreaks.read(getDeclaredBreaks().get().getAsFile()).boundTo(baseline);
        String verdict;
        if (getSkipReason().isPresent()) {
            getLogger().lifecycle("Baseline comparison skipped: {}", getSkipReason().get());
            verdict = "Not compared: " + getSkipReason().get();
        } else {
            ResolvedComponent.verify(baseline, getBaselineComponents().get());
            Path changelog = getChangelog().get().getAsFile().toPath();
            ShippedChangesets.Comparison comparison =
                    ShippedChangesets.compare(getBaselineArchive().getSingleFile(), changelog);
            if (!comparison.problems().isEmpty()) {
                throw new GradleException("Changelog of " + module + " against " + baseline
                        + " (LLD-api-stability §5.1), in " + changelog + ":\n  "
                        + String.join("\n  ", comparison.problems())
                        + "\nA shipped changeset never changes, and no declaration accepts it: restore it, and put the "
                        + "schema change in a new changeset appended to the master.");
            }
            verdict = "Every changeset " + baseline + " shipped is unchanged: " + comparison.shipped();
        }
        try {
            Files.writeString(getVerdict().get().getAsFile().toPath(), verdict + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
