package com.codingful.tandem.compatibility;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Locale;
import java.util.Set;
import org.gradle.api.GradleException;

/**
 * The expectation of one acceptance case of the OpenAPI gate (LLD-api-stability §6.4), its {@code expected.txt}:
 * comment and blank lines, {@code checks} followed by the parts of the gate the case runs, the verdict
 * ({@code verdict pass} or {@code verdict fail}), and the identity of every line the gate must conclude
 * ({@link OpenApiGate.Line#identity()}). A case holds when the gate reaches that verdict with exactly those
 * identities, so a mutation cannot pass on a finding it did not cause.
 */
public final class OpenApiGateCase {

    /** The files of a case directory; a missing document or baseline is the frozen contract. */
    public static final String EXPECTED = "expected.txt";
    public static final String DOCUMENT = "openapi.yaml";
    public static final String BASELINE = "baseline.yaml";
    public static final String DECLARED_BREAKS = "accepted-breaks.txt";
    /** A severity file handed to oasdiff as it is, in place of the policy. */
    public static final String SEVERITY_LEVELS = "severity-levels.txt";

    /** The parts of the gate, each run by one oasdiff command. */
    public enum Part {
        /** {@code oasdiff validate} of the document (§6.1). */
        VALIDITY,
        /** {@code oasdiff breaking} of the baseline and the document, with the policy (§6.2). */
        COMPATIBILITY,
        /** The two documents as {@code oasdiff upgrade} loads them, for the frozen names (§6.3). */
        FROZEN_NAMES;

        String label() {
            return name().toLowerCase(Locale.ROOT).replace('_', '-');
        }
    }

    /**
     * @param parts      the parts of the gate the case runs
     * @param passes     the expected verdict
     * @param identities the identity of every line the gate must conclude, sorted
     */
    public record Expectation(Set<Part> parts, boolean passes, List<String> identities) {

        public boolean runs(Part part) {
            return parts.contains(part);
        }
    }

    private OpenApiGateCase() {
    }

    /**
     * @throws GradleException when a line is not part of the format, or the parts or the verdict are missing
     */
    public static Expectation parse(String text) {
        Set<Part> parts = EnumSet.noneOf(Part.class);
        Boolean passes = null;
        List<String> identities = new ArrayList<>();
        for (String raw : text.split("\n", -1)) {
            String line = raw.strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            if (line.startsWith("checks ")) {
                for (String label : line.substring("checks ".length()).strip().split("\\s+")) {
                    parts.add(part(label));
                }
            } else if (line.equals("verdict pass") || line.equals("verdict fail")) {
                passes = line.equals("verdict pass");
            } else if (isIdentity(line)) {
                identities.add(line);
            } else {
                throw new GradleException(EXPECTED + " has a line that is neither 'checks', a verdict nor the "
                        + "identity of a line of the gate: " + line);
            }
        }
        if (parts.isEmpty() || passes == null) {
            throw new GradleException(EXPECTED + " must state the parts it checks and a verdict");
        }
        return new Expectation(parts, passes, identities.stream().sorted().toList());
    }

    private static Part part(String label) {
        for (Part part : Part.values()) {
            if (part.label().equals(label)) {
                return part;
            }
        }
        throw new GradleException(EXPECTED + " checks an unknown part '" + label
                + "', expected validity, compatibility or frozen-names");
    }

    private static boolean isIdentity(String line) {
        for (OpenApiGate.Kind kind : OpenApiGate.Kind.values()) {
            if (line.startsWith(kind.label() + " ")) {
                return true;
            }
        }
        return false;
    }

    /** How the gate's outcome differs from the expectation; empty when it matches. */
    public static List<String> mismatches(Expectation expected, OpenApiGate.Outcome outcome) {
        List<String> mismatches = new ArrayList<>();
        if (outcome.passed() != expected.passes()) {
            mismatches.add("the gate " + (outcome.passed() ? "passes" : "fails") + ", expected to "
                    + (expected.passes() ? "pass" : "fail"));
        }
        List<String> unexpected = new ArrayList<>();
        List<String> missing = new ArrayList<>(expected.identities());
        for (OpenApiGate.Line line : outcome.lines()) {
            if (!missing.remove(line.identity())) {
                unexpected.add(line.toString());
            }
        }
        missing.forEach(identity -> mismatches.add("expected, not concluded: " + identity));
        unexpected.forEach(line -> mismatches.add("concluded, not expected: " + line));
        return mismatches;
    }
}
