package com.codingful.tandem.test.reference;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * A committed reference file and the rendering its test compares with it (LLD-api-stability §8.1). The
 * file lives under the module's {@code src/test/resources/reference/}, is UTF-8 with LF line endings and
 * a final newline, and opens with a comment line naming the test that renders it and the command that
 * regenerates it.
 *
 * <p>In the ordinary mode the comparison fails with the two texts, which the IDE and the test report show as
 * a diff, and that command. Run with {@code -PupdateReferenceFiles}, the build sets the system property
 * {@value #UPDATE_PROPERTY} on every test task and the file is rewritten instead, in the source tree, so the change shows in the working
 * copy's diff where review judges it. Paths resolve against the module directory, the working directory
 * Gradle gives a test task. A test fixture of this repository, never published.
 */
public final class ReferenceFile {

    /** The system property the build sets from {@code -PupdateReferenceFiles}. */
    public static final String UPDATE_PROPERTY = "tandem.updateReferenceFiles";

    private static final Path DIRECTORY = Path.of("src", "test", "resources", "reference");

    private final Path file;
    private final String displayPath;
    private final String renderingTest;
    private final String regenerationCommand;
    private final boolean update;

    private ReferenceFile(Path directory, String fileName, String renderingTest, String regenerationCommand,
            boolean update) {
        this.file = directory.resolve(fileName);
        this.displayPath = DIRECTORY.resolve(fileName).toString().replace('\\', '/');
        this.renderingTest = renderingTest;
        this.regenerationCommand = regenerationCommand;
        this.update = update;
    }

    /**
     * @param fileName      the file's name under {@code src/test/resources/reference/}
     * @param renderingTest the test class that renders the file and owns it
     * @return the reference file of the module the test runs in
     * @throws IllegalStateException if the working directory is not a module directory
     */
    public static ReferenceFile of(String fileName, Class<?> renderingTest) {
        return of(Path.of("").toAbsolutePath(), fileName, renderingTest, Boolean.getBoolean(UPDATE_PROPERTY));
    }

    static ReferenceFile of(Path moduleDirectory, String fileName, Class<?> renderingTest, boolean update) {
        if (!Files.isDirectory(moduleDirectory.resolve("src").resolve("test"))) {
            throw new IllegalStateException("Reference files resolve against the module directory, the working"
                    + " directory Gradle gives a test task, workingDirectory:" + moduleDirectory);
        }
        String command = "./gradlew :" + moduleDirectory.getFileName() + ":test -PupdateReferenceFiles";
        return new ReferenceFile(moduleDirectory.resolve(DIRECTORY), fileName, renderingTest.getSimpleName(),
                command, update);
    }

    /**
     * Compares {@code lines}, preceded by the file's comment line, with the committed file; in update mode,
     * writes them to it when they differ.
     *
     * @param lines the rendering, one fact per line, without line terminators
     * @throws AssertionError if the rendering differs from the committed file, or the file is missing,
     *                        outside update mode
     */
    public void assertMatches(List<String> lines) {
        Objects.requireNonNull(lines, "lines");
        String rendered = render(lines);
        String committed = read();
        if (update && !rendered.equals(committed)) {
            write(rendered);
            return;
        }
        assertThat(rendered)
                .as("Reference file %s, rendered by %s. If the change is intended, regenerate the file with %s and"
                        + " review its diff: a line changed or removed is a breaking change (LLD-api-stability §8.1)",
                        displayPath, renderingTest, regenerationCommand)
                .isEqualTo(committed);
    }

    private String render(List<String> lines) {
        StringBuilder text = new StringBuilder("# Rendered by ").append(renderingTest)
                .append("; regenerate with ").append(regenerationCommand).append('\n');
        lines.forEach(line -> text.append(line).append('\n'));
        return text.toString();
    }

    private String read() {
        if (!Files.exists(file)) {
            return null;
        }
        try {
            return Files.readString(file, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private void write(String rendered) {
        try {
            Files.createDirectories(file.getParent());
            Files.writeString(file, rendered, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
