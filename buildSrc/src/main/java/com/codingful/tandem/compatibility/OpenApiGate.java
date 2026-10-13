package com.codingful.tandem.compatibility;

import groovy.json.JsonSlurper;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * What {@code checkOpenApi} concludes from oasdiff's runs and the declared breaks (LLD-api-stability §6). Every
 * conclusion is one {@link Line}, and the gate passes when none of them fails:
 *
 * <ul>
 *   <li>validity (§6.1): every finding of {@code oasdiff validate} fails, and no declaration accepts one;</li>
 *   <li>compatibility (§6.2): every finding {@code oasdiff breaking} reports at {@code WARN} or {@code ERR}
 *       fails unless a declaration names its fingerprint;</li>
 *   <li>frozen names (§6.3, {@link FrozenNames}): every slug or server definition lost or changed fails
 *       unless a declaration names its item.</li>
 * </ul>
 *
 * <p>A run that kept the gate from comparing (an exit code other than oasdiff's success, output that is not
 * the JSON oasdiff writes, a finding without one identity of its own) is an error: it fails whatever is
 * declared, and leaves the declarations of what it would have compared undecided rather than stale (§3,
 * rule 4).
 */
public final class OpenApiGate {

    /** The gate kind of the declared breaks this gate reads. */
    public static final String GATE = "openapi";
    /** The one module that runs it. */
    public static final String MODULE = "tandem-admin";
    /** The item of a compatibility finding, followed by its fingerprint. */
    public static final String FINDING = "finding ";

    /** How a line concludes. */
    public enum Kind {
        /** A finding of the validation. */
        INVALID,
        /** A run that kept the gate from comparing. */
        ERROR,
        /** A break no declaration accepts. */
        UNDECLARED,
        /** A break a declaration accepts: the only kind that does not fail the gate. */
        ACCEPTED,
        /** A declaration that matches nothing this run reports. */
        STALE;

        String label() {
            return name().toLowerCase(java.util.Locale.ROOT);
        }
    }

    /**
     * One conclusion of the gate, rendered {@code <kind> <item>: <detail>}.
     *
     * @param item   what the line is about: a declaration's item, a validation rule, a command
     * @param detail what oasdiff or the gate says about it; empty when the item says it all
     */
    public record Line(Kind kind, String item, String detail) {

        public Line(Kind kind, String item) {
            this(kind, item, "");
        }

        public boolean fails() {
            return kind != Kind.ACCEPTED;
        }

        /** The line without its detail: what an acceptance case expects (§6.4). */
        public String identity() {
            return kind.label() + " " + item;
        }

        @Override
        public String toString() {
            return detail.isEmpty() ? identity() : identity() + ": " + detail;
        }
    }

    /**
     * @param lines      every conclusion, in a stable order
     * @param undeclared the items of the breaks no declaration accepts
     * @param accepted   the declarations that accepted a break
     * @param stale      the declarations that matched nothing
     */
    public record Outcome(List<Line> lines, List<String> undeclared, List<DeclaredBreaks.Entry> accepted,
            List<DeclaredBreaks.Entry> stale) {

        public boolean passed() {
            return lines.stream().noneMatch(Line::fails);
        }

        /** The verdict, then one line per conclusion. */
        public String render() {
            return "verdict " + (passed() ? "pass" : "fail") + "\n"
                    + lines.stream().map(line -> line + "\n").collect(Collectors.joining());
        }
    }

    /**
     * The runs that compare the current document with the baseline's. An acceptance case (§6.4) runs only the
     * part its change concerns: the runs of the other part are then {@code null}.
     *
     * @param breaking         {@code oasdiff breaking} of the two documents, with Tandem's severity levels
     * @param baselineDocument the baseline document as oasdiff loads it, in JSON
     * @param currentDocument  the current document as oasdiff loads it, in JSON
     */
    public record Comparison(OasdiffRun breaking, OasdiffRun baselineDocument, OasdiffRun currentDocument) {
    }

    /** Breaks a comparison reported, by item, with what each item names; or the errors that kept it from it. */
    private record Reported(Map<String, String> items, List<Line> errors) {

        static final Reported NONE = new Reported(Map.of(), List.of());

        static Reported error(String command, String text) {
            return new Reported(Map.of(), List.of(new Line(Kind.ERROR, command, text)));
        }
    }

    private OpenApiGate() {
    }

    /**
     * @param validate     {@code oasdiff validate} of the current document; {@code null} when not run
     * @param comparison   the comparison with the baseline; {@code null} when the document is not compared
     * @param declarations the module's {@code openapi} entries of the declared breaks, read only when compared
     */
    public static Outcome judge(OasdiffRun validate, Comparison comparison, List<DeclaredBreaks.Entry> declarations) {
        List<Line> lines = new ArrayList<>(validate == null ? List.of() : validity(validate));
        if (comparison == null) {
            return new Outcome(lines, List.of(), List.of(), List.of());
        }
        Reported findings = comparison.breaking() == null ? Reported.NONE : findings(comparison.breaking());
        Reported names = comparison.currentDocument() == null ? Reported.NONE
                : names(comparison.baselineDocument(), comparison.currentDocument());
        lines.addAll(findings.errors());
        lines.addAll(names.errors());
        Map<String, String> items = new LinkedHashMap<>(findings.items());
        items.putAll(names.items());
        DeclaredBreaks.Matching matching = DeclaredBreaks.match(items.keySet(), declarations);
        items.forEach((item, what) -> lines.add(new Line(matching.accepted().containsKey(item) ? Kind.ACCEPTED
                : Kind.UNDECLARED, item, what)));
        List<DeclaredBreaks.Entry> stale = matching.stale().stream()
                .filter(entry -> (entry.item().startsWith(FINDING) ? findings : names).errors().isEmpty())
                .toList();
        stale.forEach(entry -> lines.add(new Line(Kind.STALE, entry.item())));
        return new Outcome(lines, matching.undeclared(), List.copyOf(matching.accepted().values()), stale);
    }

    /** The validation's findings, each a line that fails (§6.1). */
    static List<Line> validity(OasdiffRun validate) {
        // --fail-on INFO: 0 without a finding, 1 with at least one.
        if (validate.exitCode() != 0 && validate.exitCode() != 1) {
            return List.of(exitError(validate));
        }
        List<Map<?, ?>> findings;
        try {
            findings = objects(validate);
        } catch (IllegalArgumentException e) {
            return List.of(new Line(Kind.ERROR, validate.command(), e.getMessage()));
        }
        if (validate.exitCode() == 1 && findings.isEmpty()) {
            return List.of(exitError(validate));
        }
        return findings.stream()
                .map(finding -> new Line(Kind.INVALID, String.valueOf(finding.get("id")),
                        oneLine(finding.get("text"))))
                .toList();
    }

    /** The findings of {@code oasdiff breaking} that fail unless declared, by {@code finding <fingerprint>}. */
    private static Reported findings(OasdiffRun breaking) {
        if (breaking.exitCode() != 0) {
            return new Reported(Map.of(), List.of(exitError(breaking)));
        }
        List<Map<?, ?>> all;
        try {
            all = objects(breaking);
        } catch (IllegalArgumentException e) {
            return Reported.error(breaking.command(), e.getMessage());
        }
        Map<String, List<String>> byFingerprint = new LinkedHashMap<>();
        for (Map<?, ?> finding : all) {
            String level = switch (String.valueOf(finding.get("level"))) {
                case "3" -> "ERR";
                case "2" -> "WARN";
                case "1" -> null;
                default -> "?";
            };
            if (level == null) {
                continue;
            }
            String described = level + " " + finding.get("id") + " at " + location(finding) + ": "
                    + oneLine(finding.get("text"));
            if (level.equals("?")) {
                return Reported.error(breaking.command(), "a finding has the unknown level " + finding.get("level")
                        + ", " + described);
            }
            if (!(finding.get("fingerprint") instanceof String fingerprint) || fingerprint.isBlank()) {
                return Reported.error(breaking.command(), "a finding has no fingerprint, " + described);
            }
            byFingerprint.computeIfAbsent(fingerprint, key -> new ArrayList<>()).add(described);
        }
        Map<String, String> items = new LinkedHashMap<>();
        for (Map.Entry<String, List<String>> entry : byFingerprint.entrySet()) {
            if (entry.getValue().size() > 1) {
                return Reported.error(breaking.command(), "the fingerprint " + entry.getKey() + " is shared by "
                        + entry.getValue().size() + " findings, " + String.join(" | ", entry.getValue()));
            }
            items.put(FINDING + entry.getKey(), entry.getValue().get(0));
        }
        return new Reported(items, List.of());
    }

    private static Reported names(OasdiffRun baseline, OasdiffRun current) {
        List<Line> errors = new ArrayList<>();
        Map<?, ?> baselineDocument = document(baseline, errors);
        Map<?, ?> currentDocument = document(current, errors);
        return errors.isEmpty() ? new Reported(FrozenNames.compare(baselineDocument, currentDocument), List.of())
                : new Reported(Map.of(), errors);
    }

    private static Map<?, ?> document(OasdiffRun run, List<Line> errors) {
        if (run.exitCode() != 0) {
            errors.add(exitError(run));
            return null;
        }
        try {
            if (json(run) instanceof Map<?, ?> document) {
                return document;
            }
            errors.add(new Line(Kind.ERROR, run.command(), "the output is not a JSON object"));
        } catch (IllegalArgumentException e) {
            errors.add(new Line(Kind.ERROR, run.command(), e.getMessage()));
        }
        return null;
    }

    private static Line exitError(OasdiffRun run) {
        return new Line(Kind.ERROR, run.command() + " exit " + run.exitCode());
    }

    /** Where a finding is: its operation and path, else its section of the document. */
    private static String location(Map<?, ?> finding) {
        String operation = (text(finding.get("operation")) + " " + text(finding.get("path"))).strip();
        if (!operation.isEmpty()) {
            return operation;
        }
        String section = text(finding.get("section"));
        return section.isEmpty() ? "the document" : section;
    }

    /** oasdiff's text on one line: a validation message can quote a schema over several. */
    private static String oneLine(Object value) {
        return text(value).replaceAll("\\s+", " ").strip();
    }

    private static String text(Object value) {
        return value == null ? "" : value.toString();
    }

    /**
     * The JSON array of objects a run printed.
     *
     * @throws IllegalArgumentException when it printed anything else
     */
    private static List<Map<?, ?>> objects(OasdiffRun run) {
        if (!(json(run) instanceof List<?> list)) {
            throw new IllegalArgumentException("the output is not a JSON array");
        }
        List<Map<?, ?>> objects = new ArrayList<>();
        for (Object element : list) {
            if (!(element instanceof Map<?, ?> object) || !(object.get("id") instanceof String)) {
                throw new IllegalArgumentException("the output holds an element that is not a finding: " + element);
            }
            objects.add(object);
        }
        return objects;
    }

    /**
     * Parses what a run printed.
     *
     * @throws IllegalArgumentException when it is not JSON
     */
    static Object json(OasdiffRun run) {
        try {
            return new JsonSlurper().parseText(run.stdout());
        } catch (RuntimeException e) {
            throw new IllegalArgumentException("the output is not JSON (" + String.valueOf(e.getMessage()).lines()
                    .findFirst().orElse("") + ")", e);
        }
    }

    /** The lines that fail, one per line of a failure message. */
    public static String failing(Outcome outcome) {
        return outcome.lines().stream().filter(Line::fails).map(Line::toString).collect(Collectors.joining("\n  "));
    }
}
