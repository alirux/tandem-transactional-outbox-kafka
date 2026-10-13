package com.codingful.tandem.compatibility;

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
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFiles;
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
public abstract class CheckInterfaceKinds extends DefaultTask implements BaselineGate {

    /** The module's Java sources. */
    @InputFiles
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract ConfigurableFileCollection getSources();

    /** The module's sources jar at the baseline; empty when the module is not compared. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineSources();

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
        return BaselineGate.register(project, "checkInterfaceKinds", CheckInterfaceKinds.class,
                "Checks that every public interface states its kind, SPI or provided, and that no SPI interface of "
                        + "the baseline became provided undeclared.", gateModules, task -> {
            task.getSources().from(project.getExtensions().getByType(SourceSetContainer.class).getByName("main")
                    .getJava());
            task.getBaselineSources().from(baseline.compared(sources));
            task.getBaselineComponents().addAll(baseline.comparedComponents(sources));
            task.getProvidedInterfaces().set(project.getLayout().getBuildDirectory()
                    .file("compatibility/provided-interfaces.txt"));
        });
    }

    @TaskAction
    public void check() {
        String module = getModuleName().get();
        List<DeclaredBreaks.Entry> declarations = declarations(InterfaceKinds.GATE);
        Map<String, StatedKind> baselineKinds = null;
        if (compared()) {
            baselineKinds = JavaInterfaces.inSourcesJar(getBaselineSources().getSingleFile()).stream()
                    .collect(Collectors.toMap(PublicInterface::binaryName, PublicInterface::kind));
            getLogger().info("Baseline interfaces read baseline:{}, interfaces:{}", getBaseline().get(),
                    baselineKinds.size());
        }
        InterfaceKinds.Verdict verdict = InterfaceKinds.settle(module,
                JavaInterfaces.inFiles(getSources().getFiles()), baselineKinds, declarations);
        if (!verdict.problems().isEmpty()) {
            throw new GradleException("Interface kinds of " + module + " (LLD-api-stability §4.3):\n  "
                    + String.join("\n  ", verdict.problems()));
        }
        logAccepted(verdict.accepted());
        BaselineGate.write(getProvidedInterfaces().get().getAsFile().toPath(),
                verdict.provided().stream().map(name -> name + "\n").collect(Collectors.joining()));
    }
}
