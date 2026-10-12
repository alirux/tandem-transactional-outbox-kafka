package com.codingful.tandem.compatibility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.List;
import java.util.Map;
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
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

/**
 * {@code checkInterfaceKinds} (LLD-api-stability §4.3): every public interface of the module states in its
 * javadoc whether it is SPI or provided, and none the baseline released as SPI turned provided without a
 * declared break. Writes the binary names of the interfaces whose kind for the gates is provided, which
 * the Java API gate reads.
 */
@CacheableTask
public abstract class CheckInterfaceKinds extends DefaultTask {

    /** The baseline release; empty when none is set. */
    @Input
    public abstract Property<String> getBaseline();

    @Input
    public abstract Property<String> getModuleName();

    /** The modules that run this check, so a declaration naming any other fails. */
    @Input
    public abstract SetProperty<String> getGateModules();

    /** The module's Java sources. */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSources();

    /** The baseline's BOM POM; empty when no baseline is set. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineBom();

    /** The module's sources jar at the baseline; empty when the module is not compared. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineSources();

    /** Why the module is not compared with the baseline; absent when it is. Derived from the inputs above. */
    @Internal
    public abstract Property<String> getSkipReason();

    /** What the baseline configurations resolved. Derived from the files above. */
    @Internal
    public abstract ListProperty<ResolvedComponent> getBaselineComponents();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getDeclaredBreaks();

    /** One binary name per line, sorted, of each interface whose kind for the gates is provided. */
    @OutputFile
    public abstract RegularFileProperty getProvidedInterfaces();

    /**
     * Registers the check in a published library module, whose baseline is already registered, as part of
     * its {@code check} and of {@code checkCompatibility}.
     */
    public static TaskProvider<CheckInterfaceKinds> register(Project project, Set<String> gateModules) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        Configuration sources = baseline.configuration(CompatibilityBaseline.SOURCES);
        TaskProvider<CheckInterfaceKinds> gate = project.getTasks().register("checkInterfaceKinds",
                CheckInterfaceKinds.class, task -> {
            task.setGroup("verification");
            task.setDescription("Checks that every public interface states its kind, SPI or provided, and that no "
                    + "SPI interface of the baseline became provided undeclared.");
            task.getBaseline().set(baseline.version());
            task.getModuleName().set(project.getName());
            task.getGateModules().set(gateModules);
            task.getSources().from(project.getExtensions().getByType(SourceSetContainer.class).getByName("main")
                    .getJava());
            task.getBaselineBom().from(baseline.bom());
            task.getBaselineSources().from(baseline.compared(sources));
            task.getSkipReason().set(baseline.skipReason());
            task.getBaselineComponents().addAll(baseline.components(baseline.bom()));
            task.getBaselineComponents().addAll(baseline.comparedComponents(sources));
            task.getDeclaredBreaks().set(CompatibilityBaseline.declaredBreaks(project));
            task.getProvidedInterfaces().set(project.getLayout().getBuildDirectory()
                    .file("compatibility/provided-interfaces.txt"));
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
                .requireGateModules(InterfaceKinds.GATE, getGateModules().get())
                .entries(InterfaceKinds.GATE, module);
        ResolvedComponent.verify(baseline, getBaselineComponents().get());
        Map<String, StatedKind> baselineKinds = null;
        if (getSkipReason().isPresent()) {
            getLogger().lifecycle("Baseline comparison skipped: {}", getSkipReason().get());
        } else {
            baselineKinds = JavaInterfaces.inSourcesJar(getBaselineSources().getSingleFile()).stream()
                    .collect(Collectors.toMap(PublicInterface::binaryName, PublicInterface::kind));
            getLogger().info("Baseline interfaces read baseline:{}, interfaces:{}", baseline, baselineKinds.size());
        }
        InterfaceKinds.Verdict verdict = InterfaceKinds.settle(module,
                JavaInterfaces.inFiles(getSources().getFiles()), baselineKinds, declarations);
        if (!verdict.problems().isEmpty()) {
            throw new GradleException("Interface kinds of " + module + " (LLD-api-stability §4.3):\n  "
                    + String.join("\n  ", verdict.problems()));
        }
        verdict.accepted().forEach(entry -> getLogger().lifecycle("Declared break accepted: {}, release notes: {}",
                entry, entry.releaseNote()));
        try {
            Files.writeString(getProvidedInterfaces().get().getAsFile().toPath(),
                    verdict.provided().stream().map(name -> name + "\n").collect(Collectors.joining()),
                    StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
