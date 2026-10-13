package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Stream;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.Project;
import org.gradle.api.file.ConfigurableFileCollection;
import org.gradle.api.file.Directory;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.ListProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.CacheableTask;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.InputFile;
import org.gradle.api.tasks.InputFiles;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputDirectory;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.PathSensitive;
import org.gradle.api.tasks.PathSensitivity;
import org.gradle.api.tasks.TaskAction;
import org.gradle.api.tasks.TaskContainer;
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
public abstract class CheckOpenApi extends DefaultTask {

    /** The contract, relative to the root project. */
    public static final String DOCUMENT = "docs/admin-api.openapi.yaml";

    /** The baseline release; empty when none is set. */
    @Input
    public abstract Property<String> getBaseline();

    @Input
    public abstract Property<String> getModuleName();

    /** The baseline's BOM POM, which says whether the module is new; empty when no baseline is set. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineBom();

    /** Why the module is not compared with the baseline; absent when it is. Derived from the inputs above. */
    @Internal
    public abstract Property<String> getSkipReason();

    /** What the baseline BOM's configuration resolved. Derived from its file above. */
    @Internal
    public abstract ListProperty<ResolvedComponent> getBaselineComponents();

    /** The document at the baseline tag, as {@code extractOpenApiBaseline} writes it; absent when not compared. */
    @InputFiles
    @PathSensitive(PathSensitivity.NONE)
    public abstract ConfigurableFileCollection getBaselineDocument();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getDocument();

    /** The committed policy, {@code gradle/oasdiff-severity-levels.txt}. */
    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getSeverityPolicy();

    @InputFile
    @PathSensitive(PathSensitivity.NONE)
    public abstract RegularFileProperty getDeclaredBreaks();

    /** The oasdiff image, by tag and digest. */
    @Input
    public abstract Property<String> getImage();

    /** The repository the container mounts. Only where it is, never what it holds, so not an input. */
    @Internal
    public abstract DirectoryProperty getRepository();

    /** The policy without its comments, the file oasdiff reads, in the directory the container mounts. */
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
     * {@code check} and of {@code checkCompatibility}, with the baseline's extraction, the acceptance cases of
     * §6.4 and the helper that writes the policy file from the catalog.
     *
     * @param image the oasdiff image, by tag and digest, from the version catalog
     */
    public static TaskProvider<CheckOpenApi> register(Project project, Provider<String> image) {
        CompatibilityBaseline baseline = CompatibilityBaseline.of(project);
        TaskContainer tasks = project.getTasks();
        File root = project.getRootDir();
        Provider<Directory> work = project.getLayout().getBuildDirectory().dir("openapi");
        TaskProvider<ExtractOpenApiBaseline> extract = tasks.register("extractOpenApiBaseline",
                ExtractOpenApiBaseline.class, task -> {
            task.setDescription("Writes the Admin API contract at the baseline tag to build/openapi/baseline.yaml.");
            task.getBaseline().set(baseline.version());
            task.getDocumentPath().set(DOCUMENT);
            task.getSkipReason().set(baseline.skipReason());
            task.getRepository().set(root);
            task.getDocument().set(work.map(directory -> directory.file("baseline.yaml")));
            // One git call, and the tag may have moved or been fetched since: never up to date (§6.2).
            task.getOutputs().upToDateWhen(self -> false);
        });
        TaskProvider<CheckOpenApi> gate = tasks.register("checkOpenApi", CheckOpenApi.class, task -> {
            task.setGroup("verification");
            task.setDescription("Validates the Admin API contract and checks it against the baseline release with "
                    + "oasdiff (needs Docker).");
            task.getBaseline().set(baseline.version());
            task.getModuleName().set(project.getName());
            task.getBaselineBom().from(baseline.bom());
            task.getSkipReason().set(baseline.skipReason());
            task.getBaselineComponents().addAll(baseline.components(baseline.bom()));
            task.getBaselineDocument().from(extract.flatMap(ExtractOpenApiBaseline::getDocument));
            task.getDocument().set(new File(root, DOCUMENT));
            task.getSeverityPolicy().set(new File(root, SeverityPolicy.PATH));
            task.getDeclaredBreaks().set(CompatibilityBaseline.declaredBreaks(project));
            task.getImage().set(image);
            task.getRepository().set(root);
            task.getSeverityLevels().set(work.map(directory -> directory.file("severity-levels.txt")));
            task.getReports().set(project.getLayout().getBuildDirectory().dir("reports/openapi"));
            task.getVerdict().set(project.getLayout().getBuildDirectory().file("compatibility/openapi.txt"));
        });
        CompatibilityBaseline.registerGate(project, gate);
        CheckOpenApiCases.register(project, image);
        tasks.register("writeOasdiffSeverityLevels", WriteOasdiffSeverityLevels.class, task -> {
            task.setDescription("Writes " + SeverityPolicy.PATH + " from the pinned oasdiff image's catalog, when "
                    + "the file does not exist.");
            task.getImage().set(image);
            task.getRepository().set(root);
            task.getPolicy().set(new File(root, SeverityPolicy.PATH));
            task.getReports().set(project.getLayout().getBuildDirectory().dir("reports/openapi-catalog"));
        });
        return gate;
    }

    @TaskAction
    public void check() {
        String baseline = getBaseline().get();
        String module = getModuleName().get();
        List<DeclaredBreaks.Entry> declarations = DeclaredBreaks.read(getDeclaredBreaks().get().getAsFile())
                .boundTo(baseline)
                .requireGateModules(OpenApiGate.GATE, Set.of(module))
                .entries(OpenApiGate.GATE, module);
        Path reports = getReports().get().getAsFile().toPath();
        Path verdict = getVerdict().get().getAsFile().toPath();
        // A report or a verdict left by an earlier run must never be read as this run's.
        clean(reports, verdict);
        Path severityLevels = getSeverityLevels().get().getAsFile().toPath();
        SeverityPolicy policy = SeverityPolicy.read(getSeverityPolicy().get().getAsFile());
        Oasdiff.write(severityLevels, policy.oasdiffFile());
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
        if (getSkipReason().isPresent()) {
            getLogger().lifecycle("Baseline comparison skipped: {}", getSkipReason().get());
        } else {
            ResolvedComponent.verify(baseline, getBaselineComponents().get());
            Path baselineDocument = getBaselineDocument().getFiles().stream().map(File::toPath)
                    .filter(Files::isRegularFile).findFirst()
                    .orElseThrow(() -> new GradleException("extractOpenApiBaseline wrote no baseline document"));
            comparison = new OpenApiGate.Comparison(
                    oasdiff.run("breaking", reports, "breaking", baselineDocument, document, "--format", "json",
                            "--severity-levels", severityLevels),
                    oasdiff.run("baseline-document", reports, "upgrade", baselineDocument, "--format", "json"),
                    oasdiff.run("document", reports, "upgrade", document, "--format", "json"));
            runs.addAll(List.of(comparison.breaking(), comparison.baselineDocument(), comparison.currentDocument()));
        }
        OpenApiGate.Outcome outcome = OpenApiGate.judge(validate, comparison, declarations);
        Oasdiff.write(verdict, (comparison == null ? "Not compared: " + getSkipReason().get() + "\n" : "")
                + OpenApiGateCase.render(outcome));
        outcome.accepted().forEach(entry -> getLogger().lifecycle("Declared break accepted: {}, release notes: {}",
                entry, entry.releaseNote()));
        if (!outcome.passed()) {
            problems.add(OpenApiGate.failing(outcome));
        }
        if (!problems.isEmpty()) {
            throw new GradleException("OpenAPI contract of " + module
                    + (comparison == null ? "" : " against " + baseline) + " (LLD-api-stability §6):\n  "
                    + String.join("\n  ", problems) + advice(outcome, runs) + "\nReports: " + reports.toUri());
        }
    }

    /** How to act on the lines that fail, and what oasdiff printed on its standard error when it failed. */
    static String advice(OpenApiGate.Outcome outcome, List<OasdiffRun> runs) {
        StringBuilder advice = new StringBuilder();
        if (!outcome.undeclared().isEmpty()) {
            advice.append("\nA break intended in a major version is declared in ").append(DeclaredBreaks.PATH)
                    .append(" (LLD-api-stability §3):");
            outcome.undeclared().forEach(item -> advice.append("\n  ").append(OpenApiGate.GATE).append(" | ")
                    .append(OpenApiGate.MODULE).append(" | ").append(item).append(" | <release-notes line>"));
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
