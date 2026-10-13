package com.codingful.tandem.compatibility;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.file.FileCollection;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.SourceSet;
import org.gradle.api.tasks.SourceSetContainer;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;
import org.gradle.api.tasks.testing.Test;
import org.gradle.api.tasks.testing.TestDescriptor;
import org.gradle.api.tasks.testing.TestListener;
import org.gradle.api.tasks.testing.TestResult;
import org.gradle.process.CommandLineArgumentProvider;
import org.gradle.testing.jacoco.plugins.JacocoPlugin;
import org.gradle.testing.jacoco.plugins.JacocoTaskExtension;

/**
 * {@code schemaCompatTest} (LLD-api-stability §5.2): the scenario of the {@code schemaCompatTest} source set,
 * compiled and run against the baseline release of the module resolved from Maven Central, on a database the
 * baseline populated and the current changelog upgraded. The task applies the declared breaks to the
 * scenario's outcome ({@link SchemaCompatOutcome}): it runs the tests without failing on them, then fails on
 * every failure no declaration accepts and on a stale declaration.
 *
 * <p>Without a baseline, or for a module new since it, the source set's compilation and this task are both
 * skipped, and the task logs the reason.
 */
@CacheableTask
public abstract class SchemaCompatTest extends Test implements BaselineGate {

    /** The source set and the task. */
    public static final String NAME = "schemaCompatTest";

    public static final String COMPILE_CLASSPATH = NAME + "CompileClasspath";
    public static final String RUNTIME_CLASSPATH = NAME + "RuntimeClasspath";

    /**
     * The system property naming the directory that holds the current changelog under its published path,
     * the module's processed resources.
     */
    public static final String CURRENT_RESOURCES_PROPERTY = "tandem.schema-compat.current-resources";

    /**
     * Adds the {@code schemaCompatTest} source set to the module, with the module at the baseline release on
     * its classpaths, and registers the task as part of its {@code check} and of {@code checkCompatibility}.
     * The module's build script adds the source set's other libraries.
     */
    public static TaskProvider<SchemaCompatTest> register(Project project) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        SourceSetContainer sourceSets = project.getExtensions().getByType(SourceSetContainer.class);
        SourceSet scenario = sourceSets.create(NAME);
        SourceSet main = sourceSets.getByName(SourceSet.MAIN_SOURCE_SET_NAME);
        baseline.dependOnBaseline(scenario.getImplementationConfigurationName(),
                project.getName() + ":" + baseline.version());
        Provider<String> skipReason = baseline.skipReason();
        // The scenario compiles against the baseline's API only, so without a baseline it has nothing to
        // compile against.
        project.getTasks().named(scenario.getCompileJavaTaskName()).configure(compile ->
                compile.onlyIf("the module is compared with a compatibility baseline", task -> !skipReason.isPresent()));
        Configuration compileClasspath = project.getConfigurations().getByName(COMPILE_CLASSPATH);
        Configuration runtimeClasspath = project.getConfigurations().getByName(RUNTIME_CLASSPATH);
        FileCollection currentResources = project.files(project.getTasks().named(main.getProcessResourcesTaskName()));
        TaskProvider<SchemaCompatTest> gate = BaselineGate.register(project, NAME, SchemaCompatTest.class,
                "Runs the baseline release's adapter on a database it populated, upgraded with the current changelog "
                        + "(needs Docker).", Set.of(project.getName()), task -> {
            task.setTestClassesDirs(scenario.getOutput().getClassesDirs());
            task.setClasspath(scenario.getRuntimeClasspath());
            // The task decides from the failed tests and the declared breaks, once they have all run.
            task.setIgnoreFailures(true);
            task.getBaselineComponents().addAll(baseline.comparedComponents(compileClasspath));
            task.getBaselineComponents().addAll(baseline.comparedComponents(runtimeClasspath));
            task.getJvmArgumentProviders().add(new CurrentResources(currentResources));
            task.onlyIf("the module is compared with a compatibility baseline", self -> {
                if (skipReason.isPresent()) {
                    self.getLogger().lifecycle("Baseline comparison skipped: {}", skipReason.get());
                    return false;
                }
                return true;
            });
            task.shouldRunAfter(project.getTasks().named("test"), project.getTasks().named("integrationTest"));
        });
        // The scenario runs the baseline release's classes, whose names are the working tree's: their execution
        // data would reach the module's merged report and the aggregated one, which read every *.exec file,
        // and be attributed to the current classes. Ordered after the JaCoCo plugin's own configuration of
        // every Test task, which adds the extension.
        project.getPlugins().withType(JacocoPlugin.class, jacoco -> project.getTasks().withType(SchemaCompatTest.class)
                .configureEach(task -> task.getExtensions().getByType(JacocoTaskExtension.class).setEnabled(false)));
        return gate;
    }

    @TaskAction
    @Override
    public void executeTests() {
        List<DeclaredBreaks.Entry> declarations = declarations(SchemaCompatOutcome.GATE);
        // The task runs only when the module is compared: this checks what the baseline classpaths resolved.
        compared();
        FailureRecorder recorder = new FailureRecorder();
        addTestListener(recorder);
        try {
            super.executeTests();
        } finally {
            removeTestListener(recorder);
        }
        String module = getModuleName().get();
        SchemaCompatOutcome.Verdict verdict = SchemaCompatOutcome.judge(module, recorder.failures(), declarations);
        verdict.accepted().forEach(line -> getLogger().lifecycle("Declared break accepted: {}", line));
        if (!verdict.problems().isEmpty()) {
            throw new GradleException("Schema compatibility of " + module + " against " + getBaseline().get()
                    + " (LLD-api-stability §5.2):\n  " + String.join("\n  ", verdict.problems()) + "\nReport: "
                    + getReports().getHtml().getEntryPoint().toURI());
        }
    }

    /** Collects every test that failed, step or not, as the run reports it. */
    private static final class FailureRecorder implements TestListener {

        private final List<SchemaCompatOutcome.FailedTest> failures = new ArrayList<>();

        @Override
        public void beforeSuite(TestDescriptor suite) {
        }

        @Override
        public void afterSuite(TestDescriptor suite, TestResult result) {
        }

        @Override
        public void beforeTest(TestDescriptor test) {
        }

        @Override
        public synchronized void afterTest(TestDescriptor test, TestResult result) {
            if (result.getResultType() == TestResult.ResultType.FAILURE) {
                failures.add(new SchemaCompatOutcome.FailedTest(String.valueOf(test.getClassName()), test.getName()));
            }
        }

        synchronized List<SchemaCompatOutcome.FailedTest> failures() {
            return List.copyOf(failures);
        }
    }

    /**
     * Hands the scenario the directory of the current changelog, the module's processed resources, as an input
     * compared by its content and relative paths rather than its absolute location.
     */
    public static final class CurrentResources implements CommandLineArgumentProvider {

        private final FileCollection directory;

        CurrentResources(FileCollection directory) {
            this.directory = directory;
        }

        @InputFiles
        @PathSensitive(PathSensitivity.RELATIVE)
        public FileCollection getDirectory() {
            return directory;
        }

        @Override
        public Iterable<String> asArguments() {
            return List.of("-D" + CURRENT_RESOURCES_PROPERTY + "=" + directory.getSingleFile().getAbsolutePath());
        }
    }
}
