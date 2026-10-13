package com.codingful.tandem.compatibility;

import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
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
 * {@code checkBomModules} (LLD-api-stability §2.4): every module the baseline release's BOM manages is
 * still managed by the BOM this build publishes, unless its removal is a declared break. A module removed
 * from the BOM, or moved to a version of its own, breaks every application that declared it without a
 * version, and no other gate can see it: the project that would run its gates is the one that is gone.
 */
@CacheableTask
public abstract class CheckBomModules extends DefaultTask implements BaselineGate {

    /** The POM this build publishes for the BOM. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getCurrentBom();

    /** The verdict, so the check stays up to date while none of its inputs changes. */
    @OutputFile
    public abstract RegularFileProperty getVerdict();

    /**
     * Registers the check in {@code tandem-bom}, with the baseline BOM's configuration it reads, as part of
     * its {@code check} and of {@code checkCompatibility}.
     */
    public static TaskProvider<CheckBomModules> register(Project project) {
        CompatibilityBaseline.registerBom(project);
        return BaselineGate.register(project, "checkBomModules", CheckBomModules.class,
                "Checks that this BOM still manages every module the baseline release's BOM manages.",
                Set.of(project.getName()), task -> {
            task.getCurrentBom().from(project.getTasks().named("generatePomFileForMavenPublication"));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/bom-modules.txt"));
        });
    }

    @TaskAction
    public void check() {
        List<DeclaredBreaks.Entry> declarations = declarations(BomModules.GATE);
        Path verdict = getVerdict().get().getAsFile().toPath();
        if (!compared()) {
            BaselineGate.write(verdict, notCompared() + "\n");
            return;
        }
        SortedSet<String> baselineModules = BomModules.managed(getBaselineBom().getSingleFile());
        SortedSet<String> currentModules = BomModules.managed(getCurrentBom().getSingleFile());
        List<String> problems = BomModules.problems(getModuleName().get(), baselineModules, currentModules,
                declarations);
        if (!problems.isEmpty()) {
            throw new GradleException("Modules of " + getModuleName().get() + " against " + getBaseline().get()
                    + " (LLD-api-stability §2.4):\n  " + String.join("\n  ", problems));
        }
        logAccepted(declarations);
        BaselineGate.write(verdict, "Every module the BOM of " + getBaseline().get()
                + " manages is still managed or declared: " + baselineModules + "\n");
    }
}
