package com.codingful.tandem.compatibility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import org.gradle.api.Action;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.SetProperty;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskProvider;

/**
 * What every gate comparing the build with the baseline release shares (LLD-api-stability §2, §3): the
 * baseline, the module, the baseline BOM that says whether the module is new, the components the baseline
 * configurations resolved, and the declared breaks. An interface rather than a base class, because two gates
 * extend a task type of their own: {@code checkJavaApi} the japicmp plugin's, {@code schemaCompatTest}
 * Gradle's {@code Test}.
 */
public interface BaselineGate extends Task {

    /** The baseline release; empty when none is set. */
    @Input
    Property<String> getBaseline();

    @Input
    Property<String> getModuleName();

    /** The modules that run this gate, so a declaration naming any other fails. */
    @Input
    SetProperty<String> getGateModules();

    /** The baseline's BOM POM, which says whether the module is new; empty when no baseline is set. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    ConfigurableFileCollection getBaselineBom();

    /** Why the module is not compared with the baseline; absent when it is. Derived from the inputs above. */
    @Internal
    Property<String> getSkipReason();

    /** What the baseline configurations resolved. Derived from the baseline files. */
    @Internal
    ListProperty<ResolvedComponent> getBaselineComponents();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    RegularFileProperty getDeclaredBreaks();

    /**
     * Registers a gate in a module whose baseline is already registered, with the inputs every gate shares,
     * as part of its {@code check} and of {@code checkCompatibility}.
     *
     * @param gateModules every module that runs the gate
     * @param configure   what the gate adds of its own
     */
    static <T extends BaselineGate> TaskProvider<T> register(Project project, String name, Class<T> type,
            String description, Set<String> gateModules, Action<? super T> configure) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        TaskProvider<T> gate = project.getTasks().register(name, type, task -> {
            task.setGroup("verification");
            task.setDescription(description);
            task.getBaseline().set(baseline.version());
            task.getModuleName().set(project.getName());
            task.getGateModules().set(gateModules);
            task.getBaselineBom().from(baseline.bom());
            task.getSkipReason().set(baseline.skipReason());
            task.getBaselineComponents().addAll(baseline.components(baseline.bom()));
            task.getDeclaredBreaks().set(CompatibilityBaseline.declaredBreaks(project));
            configure.execute(task);
        });
        CompatibilityBaseline.registerGate(project, gate);
        return gate;
    }

    /** The declared breaks, once their entries are known to be bound to the baseline (§3, rule 1). */
    default DeclaredBreaks declaredBreaks() {
        return DeclaredBreaks.read(getDeclaredBreaks().get().getAsFile()).boundTo(getBaseline().get());
    }

    /**
     * The entries of {@code gate} for this module, after failing on an entry naming a module where the gate does
     * not run (§3, rule 2).
     */
    default List<DeclaredBreaks.Entry> declarations(String gate) {
        return declaredBreaks().requireGateModules(gate, getGateModules().get()).entries(gate, getModuleName().get());
    }

    /**
     * Whether the module is compared with the baseline. When it is not, logs why (§2.1); when it is, fails
     * unless every Tandem component the baseline configurations resolved is published at the baseline (§2.2).
     */
    default boolean compared() {
        if (getSkipReason().isPresent()) {
            getLogger().lifecycle("Baseline comparison skipped: {}", getSkipReason().get());
            return false;
        }
        ResolvedComponent.verify(getBaseline().get(), getBaselineComponents().get());
        return true;
    }

    /** What a gate writes as its verdict when it does not compare. */
    default String notCompared() {
        return "Not compared: " + getSkipReason().get();
    }

    default void logAccepted(Collection<DeclaredBreaks.Entry> entries) {
        entries.forEach(entry -> getLogger().lifecycle("Declared break accepted: {}, release notes: {}", entry,
                entry.releaseNote()));
    }

    /** Writes a verdict or a report, creating its directory. */
    static void write(Path file, String text) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, text, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
