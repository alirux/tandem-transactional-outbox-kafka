package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * {@code checkOpenApiCases} (LLD-api-stability §6.4): runs the parts of the OpenAPI gate each acceptance case of
 * {@code src/openApiGateCases/} names, with the pinned image and the severity policy, one case after the other,
 * and fails unless every case meets its {@code expected.txt} ({@link OpenApiGateCase}). The directory holds a
 * frozen copy of the contract, {@code contract.yaml}, and one directory per case.
 */
@CacheableTask
public abstract class CheckOpenApiCases extends DefaultTask {

    /** The cases' directory, relative to the module. */
    public static final String CASES = "src/openApiGateCases";
    /** The frozen contract, a case's document and baseline unless it carries its own. */
    public static final String CONTRACT = "contract.yaml";

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
    public void check() {
        Path casesDirectory = getCases().get().getAsFile().toPath();
        Path reports = getReports().get().getAsFile().toPath();
        Path severityLevels = getSeverityLevels().get().getAsFile().toPath();
        delete(reports);
        BaselineGate.write(severityLevels, SeverityPolicy.read(getSeverityPolicy().get().getAsFile()).oasdiffFile());
        Oasdiff oasdiff = new Oasdiff(getImage().get(), getRepository().get().getAsFile().toPath(),
                severityLevels.getParent());
        Path contract = casesDirectory.resolve(CONTRACT);
        // A document as oasdiff loads it, read once whichever cases compare it: the contract, mostly.
        Map<Path, OasdiffRun> loaded = new HashMap<>();
        List<String> names = new ArrayList<>();
        List<String> problems = new ArrayList<>();
        for (Path directory : caseDirectories(casesDirectory)) {
            String name = directory.getFileName().toString();
            Path caseReports = reports.resolve(name);
            OpenApiGateCase.Expectation expected =
                    OpenApiGateCase.parse(read(directory.resolve(OpenApiGateCase.EXPECTED)));
            Path document = own(directory, OpenApiGateCase.DOCUMENT, contract);
            Path baseline = own(directory, OpenApiGateCase.BASELINE, contract);

            OasdiffRun validate = !expected.runs(OpenApiGateCase.Part.VALIDITY) ? null
                    : oasdiff.run("validate", caseReports, "validate", document, "--fail-on", "INFO", "--format",
                            "json");
            OasdiffRun breaking = !expected.runs(OpenApiGateCase.Part.COMPATIBILITY) ? null
                    : oasdiff.run("breaking", caseReports, "breaking", baseline, document, "--format", "json",
                            "--severity-levels", own(directory, OpenApiGateCase.SEVERITY_LEVELS, severityLevels));
            OasdiffRun baselineDocument = null;
            OasdiffRun currentDocument = null;
            if (expected.runs(OpenApiGateCase.Part.FROZEN_NAMES)) {
                baselineDocument = loaded.computeIfAbsent(baseline,
                        file -> oasdiff.run("baseline-document", caseReports, "upgrade", file, "--format", "json"));
                currentDocument = loaded.computeIfAbsent(document,
                        file -> oasdiff.run("document", caseReports, "upgrade", file, "--format", "json"));
            }
            OpenApiGate.Comparison comparison = breaking == null && currentDocument == null ? null
                    : new OpenApiGate.Comparison(breaking, baselineDocument, currentDocument);
            OpenApiGate.Outcome outcome = OpenApiGate.judge(validate, comparison, declarations(directory));

            BaselineGate.write(caseReports.resolve("outcome.txt"), outcome.render());
            OpenApiGateCase.mismatches(expected, outcome).forEach(mismatch -> problems.add(name + ": " + mismatch));
            names.add(name);
        }
        if (!problems.isEmpty()) {
            throw new GradleException("OpenAPI gate cases (LLD-api-stability §6.4), in " + casesDirectory
                    + ":\n  " + String.join("\n  ", problems) + "\nReports: " + reports.toUri());
        }
        getLogger().lifecycle("OpenAPI gate cases concluded as expected cases:{}", names.size());
        BaselineGate.write(getVerdict().get().getAsFile().toPath(), "Every case concluded as expected: "
                + String.join(", ", names) + "\n");
    }

    private static List<Path> caseDirectories(Path casesDirectory) {
        try (Stream<Path> children = Files.list(casesDirectory)) {
            return children.filter(Files::isDirectory).sorted().toList();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The case's own file, else {@code fallback}. */
    private static Path own(Path directory, String file, Path fallback) {
        Path own = directory.resolve(file);
        return Files.isRegularFile(own) ? own : fallback;
    }

    private static List<DeclaredBreaks.Entry> declarations(Path directory) {
        Path declared = directory.resolve(OpenApiGateCase.DECLARED_BREAKS);
        return Files.isRegularFile(declared)
                ? DeclaredBreaks.read(declared.toFile()).entries(OpenApiGate.GATE, OpenApiGate.MODULE)
                : List.of();
    }

    private static String read(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void delete(Path directory) {
        if (!Files.exists(directory)) {
            return;
        }
        try (Stream<Path> files = Files.walk(directory)) {
            for (Path file : files.sorted(Comparator.reverseOrder()).toList()) {
                Files.delete(file);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
