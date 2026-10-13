package com.codingful.tandem.compatibility;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFiles;
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
public abstract class CheckSpringMetadata extends DefaultTask implements BaselineGate {

    /** The module's jar at the baseline; empty when the module is not compared. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineArchive();

    /** The module's jar as this build makes it. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getCurrentArchive();

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
        return BaselineGate.register(project, "checkSpringMetadata", CheckSpringMetadata.class,
                "Checks the module's Spring configuration metadata against the baseline release.", gateModules,
                task -> {
            task.getBaselineArchive().from(baseline.compared(archive));
            task.getBaselineComponents().addAll(baseline.comparedComponents(archive));
            task.getCurrentArchive().from(project.getTasks().named("jar"));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/spring-metadata.txt"));
        });
    }

    @TaskAction
    public void check() {
        List<DeclaredBreaks.Entry> declarations = declarations(SpringMetadata.GATE);
        Path verdict = getVerdict().get().getAsFile().toPath();
        if (!compared()) {
            BaselineGate.write(verdict, notCompared() + "\n");
            return;
        }
        String module = getModuleName().get();
        SpringMetadata.Outcome outcome = SpringMetadata.judge(module,
                SpringMetadata.read(getBaselineArchive().getSingleFile()),
                SpringMetadata.read(getCurrentArchive().getSingleFile()), declarations);
        if (!outcome.problems().isEmpty()) {
            throw new GradleException("Spring configuration metadata of " + module + " against " + getBaseline().get()
                    + " (LLD-api-stability §7):\n  " + String.join("\n  ", outcome.problems()));
        }
        logAccepted(outcome.accepted());
        BaselineGate.write(verdict, "Every property of " + getBaseline().get()
                + " keeps its key, type and declared default, or is declared: " + outcome.compared()
                + outcome.accepted().stream().map(entry -> "\naccepted " + entry).collect(Collectors.joining()) + "\n");
    }
}
