package com.codingful.tandem.compatibility;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.gradle.api.GradleException;

/**
 * The pinned oasdiff image, one container per command (LLD-api-stability §6): {@code docker run --rm} with no
 * network, the repository and the task's own directory mounted read-only. oasdiff prints its findings, and
 * the task keeps what it printed as its report.
 */
final class Oasdiff {

    static final String REPOSITORY = "/repo";
    static final String WORK = "/work";

    private final String image;
    private final Path repository;
    private final Path work;

    /**
     * @param image      the image reference, by tag and digest
     * @param repository the repository root
     * @param work       the task's own directory, where it writes the files oasdiff reads
     */
    Oasdiff(String image, Path repository, Path work) {
        this.image = image;
        this.repository = repository.toAbsolutePath().normalize();
        this.work = work.toAbsolutePath().normalize();
    }

    /**
     * Runs oasdiff with {@code arguments}, a {@link Path} being passed as the container sees it, and keeps its
     * standard output in {@code <reports>/<command>.json} and its standard error next to it.
     */
    OasdiffRun run(String command, Path reports, Object... arguments) {
        List<String> line = new ArrayList<>(List.of("docker", "run", "--rm", "--network", "none",
                "--volume", repository + ":" + REPOSITORY + ":ro", "--volume", work + ":" + WORK + ":ro", image));
        for (Object argument : arguments) {
            line.add(argument instanceof Path file ? inContainer(file) : argument.toString());
        }
        Path stdout = reports.resolve(command + ".json");
        Path stderr = reports.resolve(command + ".stderr.txt");
        try {
            Files.createDirectories(reports);
            Process process = new ProcessBuilder(line)
                    .redirectOutput(stdout.toFile())
                    .redirectError(stderr.toFile())
                    .start();
            int exitCode = process.waitFor();
            return new OasdiffRun(command, exitCode, Files.readString(stdout, StandardCharsets.UTF_8),
                    Files.readString(stderr, StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new GradleException("Cannot run " + String.join(" ", line) + ": the OpenAPI gate needs Docker "
                    + "(LLD-api-stability §6)", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new GradleException("Interrupted while running oasdiff " + command, e);
        }
    }

    /** Where the container sees {@code file}, which must be under one of the two mounted directories. */
    String inContainer(Path file) {
        Path absolute = file.toAbsolutePath().normalize();
        if (absolute.startsWith(work)) {
            return WORK + "/" + slashed(work.relativize(absolute));
        }
        if (absolute.startsWith(repository)) {
            return REPOSITORY + "/" + slashed(repository.relativize(absolute));
        }
        throw new IllegalArgumentException(file + " is outside " + repository + " and " + work
                + ", the two directories the container mounts");
    }

    private static String slashed(Path relative) {
        return relative.toString().replace('\\', '/');
    }

    static void write(Path file, String text) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, text, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
