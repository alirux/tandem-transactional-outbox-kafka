package com.codingful.tandem.compatibility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Set;
import java.util.SortedSet;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
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
 * {@code checkBomModules} (LLD-api-stability §2.4): every module the baseline release's BOM manages is
 * still managed by the BOM this build publishes, unless its removal is a declared break. A module removed
 * from the BOM, or moved to a version of its own, breaks every application that declared it without a
 * version, and no other gate can see it: the project that would run its gates is the one that is gone.
 */
@CacheableTask
public abstract class CheckBomModules extends DefaultTask {

    /** The baseline release; empty when none is set. */
    @Input
    public abstract Property<String> getBaseline();

    @Input
    public abstract Property<String> getModuleName();

    /** The baseline's BOM POM; empty when no baseline is set. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineBom();

    /** The POM this build publishes for the BOM. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getCurrentBom();

    /** Why the BOMs are not compared; absent when they are. Derived from the inputs above. */
    @Internal
    public abstract Property<String> getSkipReason();

    /** What the baseline BOM's configuration resolved. Derived from its file above. */
    @Internal
    public abstract ListProperty<ResolvedComponent> getBaselineComponents();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getDeclaredBreaks();

    /** The verdict, so the check stays up to date while none of its inputs changes. */
    @OutputFile
    public abstract RegularFileProperty getVerdict();

    /**
     * Registers the check in {@code tandem-bom}, with the baseline BOM's configuration it reads, as part of
     * its {@code check} and of {@code checkCompatibility}.
     */
    public static TaskProvider<CheckBomModules> register(Project project) {
        CompatibilityBaseline baseline = CompatibilityBaseline.registerBom(project);
        TaskProvider<CheckBomModules> gate = project.getTasks().register("checkBomModules", CheckBomModules.class,
                task -> {
            task.setGroup("verification");
            task.setDescription("Checks that this BOM still manages every module the baseline release's BOM manages.");
            task.getBaseline().set(baseline.version());
            task.getModuleName().set(project.getName());
            task.getBaselineBom().from(baseline.bom());
            task.getCurrentBom().from(project.getTasks().named("generatePomFileForMavenPublication"));
            task.getSkipReason().set(baseline.skipReason());
            task.getBaselineComponents().addAll(baseline.components(baseline.bom()));
            task.getDeclaredBreaks().set(CompatibilityBaseline.declaredBreaks(project));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/bom-modules.txt"));
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
                .requireGateModules(BomModules.GATE, Set.of(module))
                .entries(BomModules.GATE, module);
        String verdict;
        if (getSkipReason().isPresent()) {
            getLogger().lifecycle("Baseline comparison skipped: {}", getSkipReason().get());
            verdict = "Not compared: " + getSkipReason().get();
        } else {
            ResolvedComponent.verify(baseline, getBaselineComponents().get());
            SortedSet<String> baselineModules = BomModules.managed(getBaselineBom().getSingleFile());
            SortedSet<String> currentModules = BomModules.managed(getCurrentBom().getSingleFile());
            List<String> problems = BomModules.problems(module, baselineModules, currentModules, declarations);
            if (!problems.isEmpty()) {
                throw new GradleException("Modules of " + module + " against " + baseline
                        + " (LLD-api-stability §2.4):\n  " + String.join("\n  ", problems));
            }
            declarations.forEach(entry -> getLogger().lifecycle("Declared break accepted: {}, release notes: {}",
                    entry, entry.releaseNote()));
            verdict = "Every module the BOM of " + baseline + " manages is still managed or declared: "
                    + baselineModules;
        }
        try {
            Files.writeString(getVerdict().get().getAsFile().toPath(), verdict + "\n", StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
