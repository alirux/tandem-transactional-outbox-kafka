package com.codingful.tandem.compatibility;

import java.io.File;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.gradle.api.Project;
import org.gradle.api.Task;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.attributes.Bundling;
import org.gradle.api.attributes.Category;
import org.gradle.api.attributes.LibraryElements;
import org.gradle.api.attributes.Usage;
import org.gradle.api.file.FileCollection;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskProvider;

/**
 * The library release the compatibility gates compare the build with (LLD-api-stability §2): the Gradle
 * property {@code tandemBaseline}, and the configurations that resolve that release from Maven Central for
 * one module.
 *
 * <p>A module is compared only when a baseline is set and the baseline's own BOM manages it; otherwise
 * its baseline gates skip, with the reason {@link #skipReason()} gives. The compared files are lazy, so a
 * module new since the baseline never asks Maven Central for an artifact that does not exist, while a
 * module the baseline BOM manages fails the build when its artifact does not resolve (§2.4).
 */
public final class CompatibilityBaseline {

    public static final String PROPERTY = "tandemBaseline";
    public static final String GROUP = "com.codingful";
    public static final String BOM_MODULE = "tandem-bom";

    public static final String BOM = "compatibilityBaselineBom";
    public static final String ARCHIVE = "compatibilityBaselineArchive";
    public static final String CLASSPATH = "compatibilityBaselineClasspath";

    /** The root task that runs every gate reading the baseline (§9.2); the root build registers it. */
    public static final String CHECK_COMPATIBILITY = "checkCompatibility";

    /**
     * Every configuration that resolves the baseline, the schema compatibility test's two classpaths
     * included. The root build exempts them by name from the substitution of the working tree for Tandem's
     * coordinates (§2.2).
     */
    public static final Set<String> CONFIGURATIONS = Set.of(BOM, ARCHIVE, CLASSPATH,
            SchemaCompatTest.COMPILE_CLASSPATH, SchemaCompatTest.RUNTIME_CLASSPATH);

    private final Project project;
    private final String version;
    private final Configuration bom;
    private final Provider<String> skipReason;

    private CompatibilityBaseline(Project project, boolean library) {
        this.project = project;
        this.version = project.getProviders().gradleProperty(PROPERTY).getOrElse("").strip();
        this.bom = resolvable(BOM, "The baseline release's tandem-bom POM, which lists the modules it contains.");
        bom.setTransitive(false);
        dependOnBaseline(BOM, BOM_MODULE + ":" + version + "@pom");
        String module = project.getName();
        // The BOM itself is in every release; a library module is in the baseline when the baseline's BOM
        // manages it.
        Callable<Boolean> inBaseline = library
                ? new Memoized<>(() -> BomModules.managed(bom.getSingleFile()).contains(module))
                : () -> true;
        this.skipReason = project.provider(() -> version.isEmpty() ? "no compatibility baseline"
                : inBaseline.call() ? null : "new since " + version);
    }

    /**
     * Adds the three baseline configurations of LLD-api-stability §2.2 to a published library module, and
     * returns the baseline as that module sees it.
     */
    public static CompatibilityBaseline registerLibrary(Project project) {
        CompatibilityBaseline baseline = new CompatibilityBaseline(project, true);
        String module = project.getName();
        baseline.resolvable(ARCHIVE, "The module's jar at the baseline release.").setTransitive(false);
        baseline.dependOnBaseline(ARCHIVE, module + ":" + baseline.version + "@jar");
        Configuration classpath = baseline.resolvable(CLASSPATH,
                "The module at the baseline release, with its runtime dependencies.");
        ObjectFactory objects = project.getObjects();
        classpath.attributes(attributes -> {
            attributes.attribute(Usage.USAGE_ATTRIBUTE, objects.named(Usage.class, Usage.JAVA_RUNTIME));
            attributes.attribute(Category.CATEGORY_ATTRIBUTE, objects.named(Category.class, Category.LIBRARY));
            attributes.attribute(LibraryElements.LIBRARY_ELEMENTS_ATTRIBUTE,
                    objects.named(LibraryElements.class, LibraryElements.JAR));
            attributes.attribute(Bundling.BUNDLING_ATTRIBUTE, objects.named(Bundling.class, Bundling.EXTERNAL));
        });
        baseline.dependOnBaseline(CLASSPATH, module + ":" + baseline.version);
        project.getExtensions().add(CompatibilityBaseline.class, "compatibilityBaseline", baseline);
        return baseline;
    }

    /** Adds the baseline BOM's configuration to {@code tandem-bom}, the one module without code. */
    public static CompatibilityBaseline registerBom(Project project) {
        CompatibilityBaseline baseline = new CompatibilityBaseline(project, false);
        project.getExtensions().add(CompatibilityBaseline.class, "compatibilityBaseline", baseline);
        return baseline;
    }

    /** The baseline a module registered with {@link #registerLibrary} or {@link #registerBom}. */
    public static CompatibilityBaseline of(Project project) {
        return project.getExtensions().getByType(CompatibilityBaseline.class);
    }

    /**
     * Makes {@code gate} part of its module's {@code check} and of the root's {@code checkCompatibility}
     * (§9.2). Every gate that reads the baseline joins both this way, as it is registered, so neither
     * keeps a list of the gates.
     */
    public static void registerGate(Project project, TaskProvider<? extends Task> gate) {
        project.getTasks().named("check").configure(check -> check.dependsOn(gate));
        project.getRootProject().getTasks().named(CHECK_COMPATIBILITY).configure(all -> all.dependsOn(gate));
    }

    /** The declared breaks (LLD-api-stability §3), one file for the whole build. */
    public static File declaredBreaks(Project project) {
        return new File(project.getRootDir(), DeclaredBreaks.PATH);
    }

    /** The baseline release; empty when none is set. */
    public String version() {
        return version;
    }

    /** Why the module is not compared with the baseline; absent when it is. */
    public Provider<String> skipReason() {
        return skipReason;
    }

    public Configuration bom() {
        return bom;
    }

    public Configuration configuration(String name) {
        return project.getConfigurations().getByName(name);
    }

    /** The configuration's files when the module is compared with the baseline, and none otherwise. */
    public FileCollection compared(Configuration configuration) {
        return project.files(project.provider(() -> skipReason.isPresent() ? List.of() : configuration));
    }

    /** The components the configuration resolved, for {@link ResolvedComponent#verify}. */
    public Provider<List<ResolvedComponent>> components(Configuration configuration) {
        return configuration.getIncoming().getArtifacts().getResolvedArtifacts()
                .map(artifacts -> artifacts.stream()
                        .map(artifact -> ResolvedComponent.of(artifact.getId().getComponentIdentifier()))
                        .toList());
    }

    /** The components the configuration resolved when the module is compared, and none otherwise. */
    public Provider<List<ResolvedComponent>> comparedComponents(Configuration configuration) {
        Provider<List<ResolvedComponent>> components = components(configuration);
        return project.provider(() -> skipReason.isPresent() ? List.<ResolvedComponent>of() : components.get());
    }

    private Configuration resolvable(String name, String description) {
        return project.getConfigurations().create(name, configuration -> {
            configuration.setCanBeConsumed(false);
            configuration.setDescription(description);
        });
    }

    /** Declares the baseline's {@code notation} (a coordinate without its group) once a baseline is set. */
    void dependOnBaseline(String configuration, String notation) {
        if (!version.isEmpty()) {
            project.getDependencies().add(configuration, GROUP + ":" + notation);
        }
    }

    /** Computes a value once, on first use: the baseline BOM is parsed at most once per module. */
    private static final class Memoized<T> implements Callable<T> {

        private final Callable<T> computation;
        private T value;

        Memoized(Callable<T> computation) {
            this.computation = computation;
        }

        @Override
        public synchronized T call() throws Exception {
            if (value == null) {
                value = computation.call();
            }
            return value;
        }
    }
}
