package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import javax.inject.Inject;
import me.champeau.gradle.japicmp.JapicmpTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.artifacts.component.ModuleComponentIdentifier;
import org.gradle.api.artifacts.component.ProjectComponentIdentifier;
import org.gradle.api.file.Directory;
import org.gradle.api.file.FileCollection;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.workers.WorkerExecutionException;
import org.gradle.workers.WorkerExecutor;

/**
 * {@code checkJavaApi} (LLD-api-stability §4): the module's jar compared with its jar at the baseline release
 * by japicmp, judged by the rules of {@link JavaApiRules}. Every binary or source incompatible change fails
 * it, except a method added to a provided interface and a break the declared breaks name; a declaration
 * that matches no break fails it too.
 *
 * <p>Like the other gates, it reports a skip as a log line: without a baseline, or for a module new since
 * it, the declared breaks are still validated and nothing is compared.
 */
@CacheableTask
public abstract class CheckJavaApi extends JapicmpTask implements BaselineGate {

    /** The packages japicmp leaves out: an {@code internal} package is not API (HLD-api-stability §3). */
    static final List<String> PACKAGE_EXCLUDES = List.of("*.internal", "*.internal.*");

    /** A protected member of a public non-final class is API to a subclass. */
    static final String ACCESS_MODIFIER = "protected";

    /** The interfaces whose kind for the gate is provided, as {@code checkInterfaceKinds} writes them (§4.3). */
    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getProvidedInterfaces();

    /** What the comparison concluded: the declarations it used and the problems it found. */
    @OutputFile
    public abstract RegularFileProperty getOutcome();

    @Inject
    protected abstract WorkerExecutor getWorkers();

    /**
     * Registers the check in a published library module, whose baseline and {@code checkInterfaceKinds} are
     * already registered, as part of its {@code check} and of {@code checkCompatibility}.
     */
    public static TaskProvider<CheckJavaApi> register(Project project, Set<String> gateModules,
            TaskProvider<CheckInterfaceKinds> interfaceKinds) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        Configuration archive = baseline.configuration(CompatibilityBaseline.ARCHIVE);
        Configuration classpath = baseline.configuration(CompatibilityBaseline.CLASSPATH);
        Configuration compileClasspath = project.getConfigurations().getByName("compileClasspath");
        Provider<Directory> reports = project.getLayout().getBuildDirectory().dir("reports/japicmp");
        String module = project.getName();
        return BaselineGate.register(project, "checkJavaApi", CheckJavaApi.class,
                "Checks the module's Java API against the baseline release with japicmp.", gateModules, task -> {
            task.getBaselineComponents().addAll(baseline.comparedComponents(archive));
            task.getBaselineComponents().addAll(baseline.comparedComponents(classpath));
            task.getProvidedInterfaces().set(interfaceKinds.flatMap(CheckInterfaceKinds::getProvidedInterfaces));
            task.getOutcome().set(project.getLayout().getBuildDirectory().file("compatibility/java-api.txt"));

            task.getOldArchives().from(baseline.compared(archive));
            task.getOldClasspath().from(baseline.compared(classpath), thirdParty(compileClasspath));
            task.getNewArchives().from(project.getTasks().named("jar"));
            task.getNewClasspath().from(compileClasspath);
            task.getIgnoreMissingClasses().set(false);
            task.getPackageExcludes().addAll(PACKAGE_EXCLUDES);
            task.getAccessModifier().set(ACCESS_MODIFIER);
            task.getOnlyModified().set(true);
            // The rich report's rules decide; the plugin's own switches would fail on every incompatible change.
            task.getFailOnModification().set(false);
            task.getFailOnSourceIncompatibility().set(false);
            // Writing the markdown report filters the comparison in place, by access level and to modified,
            // non-synthetic elements, before the rich report's rules see it (LLD-api-stability §4.1).
            task.getMdOutputFile().set(reports.map(directory -> directory.file("checkJavaApi.md")));
            RegularFileProperty declaredBreaks = task.getDeclaredBreaks();
            RegularFileProperty providedInterfaces = task.getProvidedInterfaces();
            RegularFileProperty outcome = task.getOutcome();
            task.richReport(report -> {
                report.getDestinationDir().set(reports);
                report.getReportName().set("checkJavaApi.html");
                report.getTitle().set("Java API of " + module + " against " + baseline.version());
                report.getRules().addAll(project.provider(() -> JavaApiRules.configurations(module,
                        declaredBreaks.get().getAsFile(), providedInterfaces.get().getAsFile(),
                        outcome.get().getAsFile())));
            });
        });
    }

    /**
     * The compile classpath without Tandem's own modules. The old side needs it to resolve supertypes: a
     * Spring module declares Spring {@code compileOnly}, so the baseline's POM never lists it (§4.1).
     */
    private static FileCollection thirdParty(Configuration compileClasspath) {
        return compileClasspath.getIncoming().artifactView(view -> view.componentFilter(component ->
                !(component instanceof ProjectComponentIdentifier)
                        && !(component instanceof ModuleComponentIdentifier published
                                && published.getGroup().equals(CompatibilityBaseline.GROUP)))).getFiles();
    }

    @TaskAction
    @Override
    public void exec() {
        declarations(JavaApiBreaks.GATE);
        Path outcomeFile = getOutcome().get().getAsFile().toPath();
        File report = getRichReport().get().getOutputFile().get().getAsFile();
        // A report or an outcome left by an earlier run must never be read as this run's.
        delete(outcomeFile, report.toPath(), getMdOutputFile().get().getAsFile().toPath());
        if (!compared()) {
            BaselineGate.write(outcomeFile, notCompared() + "\n");
            return;
        }
        super.exec();
        WorkerExecutionException failure = null;
        try {
            getWorkers().await();
        } catch (WorkerExecutionException e) {
            failure = e;
        }
        if (!Files.exists(outcomeFile)) {
            // The comparison itself failed (a class japicmp cannot resolve, say): no declaration can accept that.
            throw failure != null ? failure : new IllegalStateException("japicmp wrote no outcome to " + outcomeFile);
        }
        JavaApiBreaks.Outcome outcome = JavaApiBreaks.Outcome.read(outcomeFile);
        outcome.accepted().forEach(line -> getLogger().lifecycle("Declared break accepted: {}", line));
        if (!outcome.problems().isEmpty()) {
            throw new GradleException("Java API of " + getModuleName().get() + " against " + getBaseline().get()
                    + " (LLD-api-stability §4.2):\n  " + String.join("\n  ", outcome.problems()) + "\nReport: "
                    + report.toURI());
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static void delete(Path... files) {
        try {
            for (Path file : files) {
                Files.deleteIfExists(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
