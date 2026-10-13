package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.gradle.api.GradleException;

/**
 * Tandem's compatibility policy for the Admin API, {@code gradle/oasdiff-severity-levels.txt}: the level of
 * each oasdiff rule, grouped under comments (LLD-api-stability §6.2). oasdiff rejects a comment or a blank
 * line, so it reads {@link #oasdiffFile()}, the rules alone. The file must give a level to every rule the
 * pinned image classes as {@code info}, and name no rule the image does not have.
 */
public final class SeverityPolicy {

    /** Where the file lives, relative to the root project. */
    public static final String PATH = "gradle/oasdiff-severity-levels.txt";

    /** The levels a line may set: oasdiff accepts {@code none} too, which would hide a rule altogether. */
    static final Set<String> LEVELS = Set.of("err", "warn", "info");

    private static final Pattern RULE = Pattern.compile("([a-z0-9]+(?:-[a-z0-9]+)*)\\s+(\\S+)");

    private final Map<String, String> levels;

    private SeverityPolicy(Map<String, String> levels) {
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
     * Reads comment and blank lines, and one {@code <rule-id> <level>} line per rule.
     *
     * @throws GradleException naming the line that is neither, sets another level, or repeats a rule
     */
    public static SeverityPolicy parse(String text) {
        Map<String, String> levels = new LinkedHashMap<>();
        String[] lines = text.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            String line = lines[i].strip();
            if (line.isEmpty() || line.startsWith("#")) {
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
        return new SeverityPolicy(levels);
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
     * What keeps the file from covering the catalog: a rule the catalog classes as {@code info} that the file
     * does not list, and a rule the file lists that the catalog does not have.
     *
     * @param catalog every rule of the pinned image, with its upstream level
     */
    public List<String> problems(Map<String, String> catalog) {
        List<String> problems = new ArrayList<>();
        catalog.forEach((rule, level) -> {
            if (level.equals("info") && !levels.containsKey(rule)) {
                problems.add(rule + " is info in the pinned oasdiff image and has no line: add '" + rule
                        + " err' or '" + rule + " info' under the group of the contract rule it falls in");
            }
        });
        levels.keySet().stream().filter(rule -> !catalog.containsKey(rule)).forEach(rule ->
                problems.add(rule + " is not a rule of the pinned oasdiff image: remove its line"));
        return problems;
    }

    /**
     * The rules of {@code oasdiff checks changelog --format json}, each with its upstream level.
     *
     * @throws GradleException when the run failed or printed something else
     */
    public static Map<String, String> catalog(OasdiffRun checks) {
        if (checks.exitCode() != 0) {
            throw new GradleException("oasdiff " + checks.command() + " failed with exit code " + checks.exitCode()
                    + ": " + checks.stderr().strip());
        }
        Map<String, String> catalog = new TreeMap<>();
        Object json;
        try {
            json = OpenApiGate.json(checks);
        } catch (IllegalArgumentException e) {
            throw new GradleException("oasdiff " + checks.command() + ": " + e.getMessage(), e);
        }
        if (json instanceof List<?> rules) {
            for (Object rule : rules) {
                // A rule the task cannot read would escape the completeness check.
                if (!(rule instanceof Map<?, ?> map && map.get("id") instanceof String id
                        && map.get("level") instanceof String level)) {
                    throw new GradleException("oasdiff " + checks.command() + " printed an element that is not a rule: "
                            + rule);
                }
                catalog.put(id, level);
            }
        }
        if (catalog.isEmpty()) {
            throw new GradleException("oasdiff " + checks.command() + " printed no rule: " + checks.stdout().strip());
        }
        return catalog;
    }
}
