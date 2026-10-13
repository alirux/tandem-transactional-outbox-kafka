package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.gradle.api.GradleException;

/**
 * Tandem's compatibility policy for the Admin API, {@code gradle/oasdiff-severity-levels.txt}: the level of
 * each oasdiff rule Tandem sets apart from upstream, grouped under comments (LLD-api-stability §6.2), and the
 * image the file was reviewed against, {@code reviewed-with <image>}. oasdiff rejects a comment or a blank
 * line, so it reads {@link #oasdiffFile()}, the rules alone.
 */
public final class SeverityPolicy {

    /** Where the file lives, relative to the root project. */
    public static final String PATH = "gradle/oasdiff-severity-levels.txt";

    /** The levels a line may set: oasdiff accepts {@code none} too, which would hide a rule altogether. */
    static final Set<String> LEVELS = Set.of("err", "warn", "info");

    private static final Pattern REVIEWED_WITH = Pattern.compile("reviewed-with\\s+(\\S+)");
    private static final Pattern RULE = Pattern.compile("([a-z0-9]+(?:-[a-z0-9]+)*)\\s+(\\S+)");

    private final String reviewedWith;
    private final Map<String, String> levels;

    private SeverityPolicy(String reviewedWith, Map<String, String> levels) {
        this.reviewedWith = reviewedWith;
        this.levels = levels;
    }

    public static SeverityPolicy read(File file) {
        try {
            return parse(Files.readString(file.toPath(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
    }

    /**
     * Reads comment and blank lines, one {@code reviewed-with <image>} line, and one {@code <rule-id> <level>}
     * line per rule.
     *
     * @throws GradleException naming the line that is neither, sets another level, or repeats a rule or the
     *                         reviewed-with line; or when the reviewed-with line is missing
     */
    public static SeverityPolicy parse(String text) {
        String reviewedWith = null;
        Map<String, String> levels = new LinkedHashMap<>();
        String[] lines = text.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            Matcher reviewed = REVIEWED_WITH.matcher(line);
            if (reviewed.matches()) {
                if (reviewedWith != null) {
                    throw invalid(i + 1, "repeats the reviewed-with line");
                }
                reviewedWith = reviewed.group(1);
                continue;
            }
            Matcher rule = RULE.matcher(line);
            if (!rule.matches()) {
                throw invalid(i + 1, "is not '<rule-id> <level>'");
            }
            if (!LEVELS.contains(rule.group(2))) {
                throw invalid(i + 1, "sets the level '" + rule.group(2) + "', expected err, warn or info");
            }
            if (levels.putIfAbsent(rule.group(1), rule.group(2)) != null) {
                throw invalid(i + 1, "repeats the rule " + rule.group(1));
            }
        }
        if (reviewedWith == null) {
            throw new GradleException(PATH + " has no 'reviewed-with <image>' line (LLD-api-stability §6.2)");
        }
        return new SeverityPolicy(reviewedWith, levels);
    }

    private static GradleException invalid(int line, String what) {
        return new GradleException(PATH + " line " + line + " " + what + " (LLD-api-stability §6.2)");
    }

    /** The file oasdiff reads: one {@code <rule-id> <level>} line per rule, in the committed file's order. */
    public String oasdiffFile() {
        return levels.entrySet().stream().map(rule -> rule.getKey() + " " + rule.getValue() + "\n")
                .collect(Collectors.joining());
    }

    /**
     * What keeps the file from applying to the pinned image: it was reviewed against another one, whose new
     * {@code info} rules nobody has classified yet.
     *
     * @param image the pinned image reference, by tag and digest
     */
    public List<String> problems(String image) {
        String tag = image.split("@", 2)[0];
        return tag.equals(reviewedWith) ? List.of()
                : List.of(PATH + " was reviewed with " + reviewedWith + ", but the version catalog pins " + tag
                        + ": classify the info rules the new image adds, with the command in the file's header, "
                        + "then move its reviewed-with line");
    }
}
