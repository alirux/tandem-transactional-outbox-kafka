package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.gradle.api.DefaultTask;
import org.gradle.api.GradleException;
import org.gradle.api.file.DirectoryProperty;
import org.gradle.api.file.RegularFileProperty;
import org.gradle.api.provider.Property;
import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Internal;
import org.gradle.api.tasks.OutputFile;
import org.gradle.api.tasks.TaskAction;

/**
 * {@code extractOpenApiBaseline} (LLD-api-stability §2.3, §6.2): writes the contract at the baseline tag,
 * {@code git show v<baseline>:docs/admin-api.openapi.yaml}, for {@code checkOpenApi} to compare. The document
 * is in no published jar, so it comes from the tag. A missing tag fails the build rather than skipping the
 * comparison. Without a baseline, or for a module new since it, the task leaves no document.
 */
public abstract class ExtractOpenApiBaseline extends DefaultTask {

    /** The baseline release; empty when none is set. */
    @Input
    public abstract Property<String> getBaseline();

    /** The document's path in the repository. */
    @Input
    public abstract Property<String> getDocumentPath();

    /** Why the module is not compared with the baseline; absent when it is. */
    @Internal
    public abstract Property<String> getSkipReason();

    /** The repository git runs in. */
    @Internal
    public abstract DirectoryProperty getRepository();

    @OutputFile
    public abstract RegularFileProperty getDocument();

    @TaskAction
    public void extract() {
        Path document = getDocument().get().getAsFile().toPath();
        delete(document);
        if (getSkipReason().isPresent()) {
            return;
        }
        String tag = "refs/tags/v" + getBaseline().get();
        File repository = getRepository().get().getAsFile();
        boolean tagged = git(repository, document, "rev-parse", "--verify", "--quiet", tag + "^{commit}") == 0;
        delete(document);
        if (!tagged) {
            throw new GradleException("The baseline tag v" + getBaseline().get() + " is not in this clone, so the "
                    + "OpenAPI contract cannot be compared with it. Run `git fetch --tags` (LLD-api-stability §2.3).");
        }
        if (git(repository, document, "show", tag + ":" + getDocumentPath().get()) != 0) {
            String error = read(document);
            delete(document);
            throw new GradleException("Cannot read " + getDocumentPath().get() + " at v" + getBaseline().get() + ": "
                    + error.strip());
        }
    }

    /** Runs git, its standard output and error into {@code output}, and returns its exit code. */
    private static int git(File repository, Path output, String... arguments) {
        List<String> line = new ArrayList<>(List.of("git"));
        line.addAll(List.of(arguments));
        try {
            Process process = new ProcessBuilder(line).directory(repository)
                    .redirectErrorStream(true)
                    .redirectOutput(output.toFile())
                    .start();
            return process.waitFor();
        } catch (IOException e) {
            throw new GradleException("Cannot run " + String.join(" ", line), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GradleException("Interrupted while running " + String.join(" ", line), e);
        }
    }

    private static void delete(Path file) {
        try {
            Files.deleteIfExists(file);
            Files.createDirectories(file.getParent());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static String read(Path file) {
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
