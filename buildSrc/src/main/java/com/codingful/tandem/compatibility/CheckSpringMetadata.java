package com.codingful.tandem.compatibility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

/**
 * {@code checkSpringMetadata} (LLD-api-stability §7): the Spring configuration metadata in the module's jar
 * keeps every property of the metadata in its jar at the baseline release, with its type and its declared
 * default, unless the declared breaks name the key. {@link SpringMetadata} decides.
 */
@CacheableTask
public abstract class CheckSpringMetadata extends DefaultTask {

    /** The baseline release; empty when none is set. */
    @Input
    public abstract Property<String> getBaseline();

    @Input
    public abstract Property<String> getModuleName();

    /** The modules that run this check, so a declaration naming any other fails. */
    @Input
    public abstract SetProperty<String> getGateModules();

    /** The baseline's BOM POM, which says whether the module is new; empty when no baseline is set. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineBom();

    /** The module's jar at the baseline; empty when the module is not compared. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineArchive();

    /** The module's jar as this build makes it. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getCurrentArchive();

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
     * Registers the check in a module that ships Spring configuration metadata, whose baseline is already
     * registered, as part of its {@code check} and of {@code checkCompatibility}.
     *
     * @param gateModules every module that runs the check
     */
    public static TaskProvider<CheckSpringMetadata> register(Project project, Set<String> gateModules) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        Configuration archive = baseline.configuration(CompatibilityBaseline.ARCHIVE);
        TaskProvider<CheckSpringMetadata> gate = project.getTasks().register("checkSpringMetadata",
                CheckSpringMetadata.class, task -> {
            task.setGroup("verification");
            task.setDescription("Checks the module's Spring configuration metadata against the baseline release.");
            task.getBaseline().set(baseline.version());
            task.getModuleName().set(project.getName());
            task.getGateModules().set(gateModules);
            task.getBaselineBom().from(baseline.bom());
            task.getBaselineArchive().from(baseline.compared(archive));
            task.getCurrentArchive().from(project.getTasks().named("jar"));
            task.getSkipReason().set(baseline.skipReason());
            task.getBaselineComponents().addAll(baseline.components(baseline.bom()));
            task.getBaselineComponents().addAll(baseline.comparedComponents(archive));
            task.getDeclaredBreaks().set(CompatibilityBaseline.declaredBreaks(project));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/spring-metadata.txt"));
        });
        CompatibilityBaseline.registerGate(project, gate);
        return gate;
    }

    @TaskAction
    public void check() {
        String baseline = getBaseline().get();
        String module = getModuleName().get();
        List<DeclaredBreaks.Entry> declarations = DeclaredBreaks.read(getDeclaredBreaks().get().getAsFile())
                .boundTo(baseline)
                .requireGateModules(SpringMetadata.GATE, getGateModules().get())
                .entries(SpringMetadata.GATE, module);
        String verdict;
        if (getSkipReason().isPresent()) {
            getLogger().lifecycle("Baseline comparison skipped: {}", getSkipReason().get());
            verdict = "Not compared: " + getSkipReason().get();
        } else {
            ResolvedComponent.verify(baseline, getBaselineComponents().get());
            SpringMetadata.Outcome outcome = SpringMetadata.judge(module,
                    SpringMetadata.read(getBaselineArchive().getSingleFile()),
                    SpringMetadata.read(getCurrentArchive().getSingleFile()), declarations);
            if (!outcome.problems().isEmpty()) {
                throw new GradleException("Spring configuration metadata of " + module + " against " + baseline
                        + " (LLD-api-stability §7):\n  " + String.join("\n  ", outcome.problems()));
            }
            outcome.accepted().forEach(entry -> getLogger().lifecycle("Declared break accepted: {}, release notes: {}",
                    entry, entry.releaseNote()));
            verdict = "Every property of " + baseline + " keeps its key, type and declared default, or is declared: "
                    + outcome.compared() + outcome.accepted().stream().map(entry -> "\naccepted " + entry)
                            .collect(Collectors.joining());
        }
        try {
            Files.writeString(getVerdict().get().getAsFile().toPath(), verdict + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
