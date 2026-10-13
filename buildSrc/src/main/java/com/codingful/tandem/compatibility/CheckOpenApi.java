package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.Optional;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskProvider;

/**
 * {@code checkOpenApi} (LLD-api-stability §6): the Admin API contract is valid OpenAPI on every build (§6.1),
 * and, with a baseline, compatible with the document at the baseline tag under Tandem's severity policy
 * (§6.2) and keeps the names the contract freezes (§6.3). oasdiff runs from its pinned image; the task decides
 * from what it prints, with {@link OpenApiGate}.
 *
 * <p>The policy file is checked on every build too: it must classify every rule the pinned image classes as
 * {@code info}, and name none the image does not have.
 */
@CacheableTask
public abstract class CheckOpenApi extends DefaultTask implements BaselineGate {

    /** The contract, relative to the root project. */
    public static final String DOCUMENT = "docs/admin-api.openapi.yaml";

    /**
     * The git object of the contract at the baseline tag, so the comparison reruns when the content it compares
     * changes, not only the tag's name; empty when the tag or the document is missing from this clone, absent
     * when the module is not compared.
     */
    @Input
    @Optional
    public abstract Property<String> getBaselineDocumentId();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getDocument();

    /** The committed policy, {@code gradle/oasdiff-severity-levels.txt}. */
    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getSeverityPolicy();

    /** The oasdiff image, by tag and digest. */
    @Input
    public abstract Property<String> getImage();

    /** The repository git runs in and the container mounts. Only where it is, so not an input. */
    @Internal
    public abstract DirectoryProperty getRepository();

    /**
     * The policy without its comments, the file oasdiff reads, in the directory the container mounts, where the
     * task also writes the baseline document.
     */
    @OutputFile
    public abstract RegularFileProperty getSeverityLevels();

    /** What each oasdiff run printed. */
    @OutputDirectory
    public abstract DirectoryProperty getReports();

    /** The lines the gate concluded, so it stays up to date while none of its inputs changes. */
    @OutputFile
    public abstract RegularFileProperty getVerdict();

    /**
     * Registers the gate in {@code tandem-admin}, whose baseline is already registered, as part of its
     * {@code check} and of {@code checkCompatibility}, with the acceptance cases of §6.4.
     *
     * @param image the oasdiff image, by tag and digest, from the version catalog
     */
    public static TaskProvider<CheckOpenApi> register(Project project, Provider<String> image) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        File root = project.getRootDir();
        Provider<String> skipReason = baseline.skipReason();
        Provider<Directory> work = project.getLayout().getBuildDirectory().dir("openapi");
        TaskProvider<CheckOpenApi> gate = BaselineGate.register(project, "checkOpenApi", CheckOpenApi.class,
                "Validates the Admin API contract and checks it against the baseline release with oasdiff (needs "
                        + "Docker).", Set.of(project.getName()), task -> {
            task.getBaselineDocumentId().set(project.provider(() -> skipReason.isPresent() ? null
                    : documentId(root, baseline.version())));
            task.getDocument().set(new File(root, DOCUMENT));
            task.getSeverityPolicy().set(new File(root, SeverityPolicy.PATH));
            task.getImage().set(image);
            task.getRepository().set(root);
            task.getSeverityLevels().set(work.map(directory -> directory.file("severity-levels.txt")));
            task.getReports().set(project.getLayout().getBuildDirectory().dir("reports/openapi"));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/openapi.txt"));
        });
        CheckOpenApiCases.register(project, image);
        return gate;
    }

    @TaskAction
    public void check() {
        List<DeclaredBreaks.Entry> declarations = declarations(OpenApiGate.GATE);
        Path reports = getReports().get().getAsFile().toPath();
        Path verdict = getVerdict().get().getAsFile().toPath();
        // A report or a verdict left by an earlier run must never be read as this run's.
        clean(reports, verdict);
        Path severityLevels = getSeverityLevels().get().getAsFile().toPath();
        SeverityPolicy policy = SeverityPolicy.read(getSeverityPolicy().get().getAsFile());
        BaselineGate.write(severityLevels, policy.oasdiffFile());
        Oasdiff oasdiff = new Oasdiff(getImage().get(), getRepository().get().getAsFile().toPath(),
                severityLevels.getParent());
        List<String> problems = new ArrayList<>(policy.problems(SeverityPolicy.catalog(
                oasdiff.run("catalog", reports, "checks", "changelog", "--format", "json"))));
        Path document = getDocument().get().getAsFile().toPath();
        List<OasdiffRun> runs = new ArrayList<>();
        OasdiffRun validate = oasdiff.run("validate", reports, "validate", document, "--fail-on", "INFO", "--format",
                "json");
        runs.add(validate);
        OpenApiGate.Comparison comparison = null;
        if (compared()) {
            Path baselineDocument = severityLevels.resolveSibling("baseline.yaml");
            extractBaseline(baselineDocument);
            comparison = new OpenApiGate.Comparison(
                    oasdiff.run("breaking", reports, "breaking", baselineDocument, document, "--format", "json",
                            "--severity-levels", severityLevels),
                    oasdiff.run("baseline-document", reports, "upgrade", baselineDocument, "--format", "json"),
                    oasdiff.run("document", reports, "upgrade", document, "--format", "json"));
            runs.addAll(List.of(comparison.breaking(), comparison.baselineDocument(), comparison.currentDocument()));
        }
        OpenApiGate.Outcome outcome = OpenApiGate.judge(validate, comparison, declarations);
        BaselineGate.write(verdict, (comparison == null ? notCompared() + "\n" : "") + outcome.render());
        logAccepted(outcome.accepted());
        if (!outcome.passed()) {
            problems.add(OpenApiGate.failing(outcome));
        }
        if (!problems.isEmpty()) {
            throw new GradleException("OpenAPI contract of " + getModuleName().get()
                    + (comparison == null ? "" : " against " + getBaseline().get()) + " (LLD-api-stability §6):\n  "
                    + String.join("\n  ", problems) + advice(outcome, runs) + "\nReports: " + reports.toUri());
        }
    }

    /**
     * Writes the contract at the baseline tag (§2.3). The document is in no published jar, so it comes from the
     * tag, and a tag missing from this clone fails the build rather than skipping the comparison.
     */
    private void extractBaseline(Path file) {
        String id = getBaselineDocumentId().getOrElse("");
        if (id.isEmpty()) {
            throw new GradleException("The baseline tag v" + getBaseline().get() + " is not in this clone, or holds no "
                    + DOCUMENT + ", so the OpenAPI contract cannot be compared with it. Run `git fetch --tags` "
                    + "(LLD-api-stability §2.3).");
        }
        Git show = git(getRepository().get().getAsFile(), "cat-file", "blob", id);
        if (show.exitCode() != 0) {
            throw new GradleException("Cannot read " + DOCUMENT + " at v" + getBaseline().get() + ": "
                    + show.text().strip());
        }
        try {
            Files.write(file, show.output());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    /** The git object of the contract at the baseline tag; empty when the clone has no such object. */
    private static String documentId(File repository, String baseline) {
        Git id = git(repository, "rev-parse", "--verify", "--quiet", "refs/tags/v" + baseline + ":" + DOCUMENT);
        return id.exitCode() == 0 ? id.text().strip() : "";
    }

    /** What git printed, its standard error included, and its exit code. */
    private record Git(int exitCode, byte[] output) {

        String text() {
            return new String(output, StandardCharsets.UTF_8);
        }
    }

    private static Git git(File repository, String... arguments) {
        List<String> line = new ArrayList<>(List.of("git"));
        line.addAll(List.of(arguments));
        try {
            Process process = new ProcessBuilder(line).directory(repository).redirectErrorStream(true).start();
            byte[] output = process.getInputStream().readAllBytes();
            return new Git(process.waitFor(), output);
        } catch (IOException e) {
            throw new GradleException("Cannot run " + String.join(" ", line), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GradleException("Interrupted while running " + String.join(" ", line), e);
        }
    }

    /** How to act on the lines that fail, and what oasdiff printed on its standard error when it failed. */
    static String advice(OpenApiGate.Outcome outcome, List<OasdiffRun> runs) {
        StringBuilder advice = new StringBuilder();
        if (!outcome.undeclared().isEmpty()) {
            advice.append("\nA break intended in a major version is declared in ").append(DeclaredBreaks.PATH)
                    .append(" (LLD-api-stability §3):");
            outcome.undeclared().forEach(item -> advice.append("\n  ")
                    .append(DeclaredBreaks.declaration(OpenApiGate.GATE, OpenApiGate.MODULE, item)));
        }
        if (!outcome.stale().isEmpty()) {
            advice.append("\nDeclared breaks that match nothing, to remove:\n")
                    .append(DeclaredBreaks.lines(outcome.stale()));
        }
        // validate exits 1 when it has findings, which its lines already show.
        runs.stream().filter(run -> run.exitCode() != 0 && !(run.command().equals("validate") && run.exitCode() == 1))
                .forEach(run -> advice.append("\noasdiff ").append(run.command()).append(" printed: ")
                        .append(run.stderr().strip()));
        return advice.toString();
    }

    private static void clean(Path reports, Path verdict) {
        try {
            Files.deleteIfExists(verdict);
            if (Files.isDirectory(reports)) {
                try (Stream<Path> files = Files.list(reports)) {
                    for (Path file : files.toList()) {
                        Files.delete(file);
                    }
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
