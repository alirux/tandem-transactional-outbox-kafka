package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputDirectory;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

/**
 * {@code checkOpenApiCases} (LLD-api-stability §6.4): runs the OpenAPI gate, with the pinned image and the
 * severity policy, on each acceptance case of {@code src/openApiGateCases/}, and fails unless every case
 * concludes exactly the lines its {@code expected.txt} states ({@link OpenApiGateCase}). The directory holds a
 * copy of the contract frozen as the cases' baseline, {@code baseline.yaml}, and one directory per case.
 */
@CacheableTask
public abstract class CheckOpenApiCases extends DefaultTask {

    /** The cases' directory, relative to the module. */
    public static final String CASES = "src/openApiGateCases";
    /** The frozen contract every case compares with, unless it carries a baseline of its own. */
    public static final String FROZEN_BASELINE = "baseline.yaml";

    /** Containers run at once: each lives for one command, and the cases are independent. */
    private static final int PARALLEL_RUNS = 4;

    @InputDirectory
    @PathSensitive(PathSensitivity.RELATIVE)
    public abstract DirectoryProperty getCases();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getSeverityPolicy();

    @Input
    public abstract Property<String> getImage();

    /** The repository the container mounts. Only where it is, never what it holds, so not an input. */
    @Internal
    public abstract DirectoryProperty getRepository();

    /** The policy without its comments, the file oasdiff reads, in the directory the container mounts. */
    @OutputFile
    public abstract RegularFileProperty getSeverityLevels();

    /** What each oasdiff run printed and what each case concluded, one directory per case. */
    @OutputDirectory
    public abstract DirectoryProperty getReports();

    /** The cases that ran, so the task stays up to date while none of its inputs changes. */
    @OutputFile
    public abstract RegularFileProperty getVerdict();

    /** Registers the task in {@code tandem-admin} as part of its {@code check}. */
    public static TaskProvider<CheckOpenApiCases> register(Project project, Provider<String> image) {
        TaskProvider<CheckOpenApiCases> cases = project.getTasks().register("checkOpenApiCases",
                CheckOpenApiCases.class, task -> {
            task.setGroup("verification");
            task.setDescription("Runs the OpenAPI gate on its acceptance cases with the pinned oasdiff image (needs "
                    + "Docker).");
            task.getCases().set(project.getLayout().getProjectDirectory().dir(CASES));
            task.getSeverityPolicy().set(new File(project.getRootDir(), SeverityPolicy.PATH));
            task.getImage().set(image);
            task.getRepository().set(project.getRootDir());
            task.getSeverityLevels().set(project.getLayout().getBuildDirectory()
                    .file("openApiGateCases/severity-levels.txt"));
            task.getReports().set(project.getLayout().getBuildDirectory().dir("reports/openApiGateCases"));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/openapi-cases.txt"));
        });
        project.getTasks().named("check").configure(check -> check.dependsOn(cases));
        return cases;
    }

    @TaskAction
    public void check() throws InterruptedException {
        Path casesDirectory = getCases().get().getAsFile().toPath();
        Path reports = getReports().get().getAsFile().toPath();
        Path severityLevels = getSeverityLevels().get().getAsFile().toPath();
        delete(reports);
        Oasdiff.write(severityLevels, SeverityPolicy.read(getSeverityPolicy().get().getAsFile()).oasdiffFile());
        Oasdiff oasdiff = new Oasdiff(getImage().get(), getRepository().get().getAsFile().toPath(),
                severityLevels.getParent());
        Path frozen = casesDirectory.resolve(FROZEN_BASELINE);
        OasdiffRun frozenDocument = oasdiff.run("baseline-document", reports, "upgrade", frozen, "--format", "json");
        List<Path> cases;
        try (Stream<Path> children = Files.list(casesDirectory)) {
            cases = children.filter(Files::isDirectory).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        Map<String, Future<List<String>>> runs = new TreeMap<>();
        ExecutorService pool = Executors.newFixedThreadPool(PARALLEL_RUNS);
        try {
            for (Path directory : cases) {
                runs.put(directory.getFileName().toString(), pool.submit(() -> run(oasdiff, directory, frozen,
                        frozenDocument, severityLevels, reports.resolve(directory.getFileName().toString()))));
            }
            List<String> problems = new ArrayList<>();
            for (Map.Entry<String, Future<List<String>>> run : runs.entrySet()) {
                try {
                    run.getValue().get().forEach(mismatch -> problems.add(run.getKey() + ": " + mismatch));
                } catch (ExecutionException e) {
                    problems.add(run.getKey() + ": " + e.getCause());
                }
            }
            if (!problems.isEmpty()) {
                throw new GradleException("OpenAPI gate cases (LLD-api-stability §6.4), in " + casesDirectory
                        + ":\n  " + String.join("\n  ", problems) + "\nReports: " + reports.toUri());
            }
        } finally {
            pool.shutdownNow();
        }
        getLogger().lifecycle("OpenAPI gate cases concluded as expected cases:{}", cases.size());
        Oasdiff.write(getVerdict().get().getAsFile().toPath(), "Every case concluded as expected: "
                + String.join(", ", runs.keySet()) + "\n");
    }

    /** Runs the gate on one case, and returns how its outcome differs from the case's expectation. */
    private static List<String> run(Oasdiff oasdiff, Path directory, Path frozen, OasdiffRun frozenDocument,
            Path severityLevels, Path reports) throws IOException {
        OpenApiGateCase.Expectation expected = OpenApiGateCase.parse(
                Files.readString(directory.resolve(OpenApiGateCase.EXPECTED), StandardCharsets.UTF_8));
        Path document = directory.resolve(OpenApiGateCase.DOCUMENT);
        OasdiffRun validate = oasdiff.run("validate", reports, "validate", document, "--fail-on", "INFO", "--format",
                "json");
        OpenApiGate.Comparison comparison = null;
        List<DeclaredBreaks.Entry> declarations = List.of();
        if (expected.compared()) {
            Path own = directory.resolve(OpenApiGateCase.BASELINE);
            Path baseline = Files.isRegularFile(own) ? own : frozen;
            Path caseSeverity = directory.resolve(OpenApiGateCase.SEVERITY_LEVELS);
            comparison = new OpenApiGate.Comparison(
                    oasdiff.run("breaking", reports, "breaking", baseline, document, "--format", "json",
                            "--severity-levels", Files.isRegularFile(caseSeverity) ? caseSeverity : severityLevels),
                    baseline == frozen ? frozenDocument
                            : oasdiff.run("baseline-document", reports, "upgrade", baseline, "--format", "json"),
                    oasdiff.run("document", reports, "upgrade", document, "--format", "json"));
            Path declared = directory.resolve(OpenApiGateCase.DECLARED_BREAKS);
            if (Files.isRegularFile(declared)) {
                declarations = DeclaredBreaks.read(declared.toFile()).entries(OpenApiGate.GATE, OpenApiGate.MODULE);
            }
        }
        OpenApiGate.Outcome outcome = OpenApiGate.judge(validate, comparison, declarations);
        Oasdiff.write(reports.resolve("outcome.txt"), OpenApiGateCase.render(outcome));
        return OpenApiGateCase.mismatches(expected, outcome);
    }

    private static void delete(Path directory) {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file : files.sorted(java.util.Comparator.reverseOrder()).toList()) {
                Files.delete(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
