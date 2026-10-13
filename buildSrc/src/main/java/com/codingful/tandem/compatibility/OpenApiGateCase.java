package com.codingful.tandem.compatibility;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import org.gradle.api.GradleException;

/**
 * The expectation of one acceptance case of the OpenAPI gate (LLD-api-stability §6.4), its {@code expected.txt}:
 * comment and blank lines, the verdict ({@code verdict pass} or {@code verdict fail}), {@code baseline none}
 * for a case run without a baseline, and every line the gate must conclude, exactly as {@link OpenApiGate.Line}
 * renders it. A case passes when the gate concludes those lines and no other, so a mutation cannot pass on a
 * finding it did not cause.
 */
public final class OpenApiGateCase {

    /** The files of a case directory; all but the expectation and the document are optional. */
    public static final String EXPECTED = "expected.txt";
    public static final String DOCUMENT = "openapi.yaml";
    /** The case's own baseline, for a case that changes an element the frozen contract does not have. */
    public static final String BASELINE = "baseline.yaml";
    public static final String DECLARED_BREAKS = "accepted-breaks.txt";
    /** A severity file handed to oasdiff as it is, in place of the policy. */
    public static final String SEVERITY_LEVELS = "severity-levels.txt";

    /**
     * @param passes   the expected verdict
     * @param compared whether the case compares with a baseline
     * @param lines    every line the gate must conclude, sorted
     */
    public record Expectation(boolean passes, boolean compared, List<String> lines) {
    }

    private OpenApiGateCase() {
    }

    /**
     * @throws GradleException when a line is not part of the format, the verdict is missing, or it contradicts
     *                         the expected lines
     */
    public static Expectation parse(String text) {
        Boolean passes = null;
        boolean compared = true;
        List<String> lines = new ArrayList<>();
        List<String> kinds = Arrays.stream(OpenApiGate.Kind.values()).map(OpenApiGate.Kind::label).toList();
        for (String raw : text.split("\n", -1)) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.equals("verdict pass") || line.equals("verdict fail")) {
                if (passes != null) {
                    throw new GradleException(EXPECTED + " states two verdicts");
                }
                passes = line.equals("verdict pass");
            } else if (line.equals("baseline none")) {
                compared = false;
            } else if (kinds.contains(line.split(" ", 2)[0])) {
                lines.add(line);
            } else {
                throw new GradleException(EXPECTED + " has a line that is neither a verdict, 'baseline none' nor a "
                        + "line of the gate: " + line);
            }
        }
        if (passes == null) {
            throw new GradleException(EXPECTED + " states no verdict");
        }
        boolean anyFails = lines.stream().anyMatch(line -> !line.startsWith(OpenApiGate.Kind.ACCEPTED.label() + " "));
        if (passes == anyFails) {
            throw new GradleException(EXPECTED + " states 'verdict " + (passes ? "pass" : "fail")
                    + "', which its lines contradict");
        }
        return new Expectation(passes, compared, lines.stream().sorted().toList());
    }

    /** How the gate's outcome differs from the expectation; empty when it matches. */
    public static List<String> mismatches(Expectation expected, OpenApiGate.Outcome outcome) {
        List<String> actual = outcome.lines().stream().map(OpenApiGate.Line::toString).sorted().toList();
        List<String> mismatches = new ArrayList<>();
        if (outcome.passed() != expected.passes()) {
            mismatches.add("the gate " + (outcome.passed() ? "passes" : "fails") + ", expected to "
                    + (expected.passes() ? "pass" : "fail"));
        }
        List<String> missing = new ArrayList<>(expected.lines());
        actual.forEach(missing::remove);
        List<String> unexpected = new ArrayList<>(actual);
        expected.lines().forEach(unexpected::remove);
        missing.forEach(line -> mismatches.add("expected, not concluded: " + line));
        unexpected.forEach(line -> mismatches.add("concluded, not expected: " + line));
        return mismatches;
    }

    /** Renders the gate's lines as an expectation would state them, for a new case. */
    public static String render(OpenApiGate.Outcome outcome) {
        return "verdict " + (outcome.passed() ? "pass" : "fail") + "\n"
                + outcome.lines().stream().map(line -> line + "\n").collect(Collectors.joining());
    }
}
