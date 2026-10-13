package com.codingful.tandem.compatibility;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.gradle.api.GradleException;

/**
 * The breaks declared intentional in {@code gradle/accepted-breaks.txt}, bound to the baseline they were
 * written against (LLD-api-stability §3). Every gate reads the whole file, takes the entries of its own
 * kind for its own module, and reports the entries that matched none of its breaks as stale.
 */
public final class DeclaredBreaks {

    /** Where the file lives, relative to the root project. */
    public static final String PATH = "gradle/accepted-breaks.txt";

    /** The gate kinds an entry may name, one per gate that compares with the baseline (§3, rule 3). */
    public static final Set<String> GATES = Set.of("japicmp", "spring-metadata", "bom", "openapi",
            "schema-compatibility");

    /** {@code baseline <release>}, or {@code baseline} alone while {@code tandemBaseline} is empty. */
    private static final Pattern BASELINE_LINE = Pattern.compile("baseline(?:\\s+(\\S+))?");
    private static final List<String> OPENAPI_ITEMS = List.of("finding ", "problem-type ", "server ");

    /**
     * One declared break.
     *
     * @param gate         the gate kind that reports the break
     * @param module       the module whose gate reports it
     * @param item         what the gate names the break by: a member, an interface, a key, a module
     * @param releaseNote  the line the release notes carry under Breaking (§3, rule 5)
     * @param line         the line of the file it was read from
     */
    public record Entry(String gate, String module, String item, String releaseNote, int line) {

        @Override
        public String toString() {
            return PATH + " line " + line + ": " + gate + " | " + module + " | " + item;
        }
    }

    /** How a gate's breaks and its declarations met (§3, rule 2). */
    public record Matching(Map<String, Entry> accepted, List<String> undeclared, List<Entry> stale) {
    }

    private final String baseline;
    private final List<Entry> entries;

    private DeclaredBreaks(String baseline, List<Entry> entries) {
        this.baseline = baseline;
        this.entries = List.copyOf(entries);
    }

    public static DeclaredBreaks read(File file) {
        try {
            return parse(Files.readString(file.toPath(), StandardCharsets.UTF_8));
        } catch (IOException e) {
            throw new UncheckedIOException("Cannot read " + file, e);
        }
    }

    /**
     * Reads the file's text: comment and blank lines, at most one {@code baseline <version>} line, and
     * entries of four {@code |}-separated fields.
     *
     * @throws GradleException when a line is neither, or an entry breaks a rule of §3
     */
    public static DeclaredBreaks parse(String text) {
        String baseline = null;
        List<Entry> entries = new ArrayList<>();
        Set<List<String>> declared = new HashSet<>();
        String[] lines = text.split("\n", -1);
        for (int i = 0; i < lines.length; i++) {
            int number = i + 1;
            String line = lines[i].strip();
            if (line.isEmpty() || line.startsWith("#")) {
                continue;
            }
            Matcher baselineLine = BASELINE_LINE.matcher(line);
            if (baselineLine.matches()) {
                if (baseline != null) {
                    throw invalid(number, "a second baseline line");
                }
                baseline = baselineLine.group(1) == null ? "" : baselineLine.group(1);
                continue;
            }
            String[] fields = line.split("\\|", 4);
            if (fields.length != 4) {
                throw invalid(number, "neither a baseline line nor an entry of four fields "
                        + "(gate | module | item | release-notes line)");
            }
            for (int f = 0; f < fields.length; f++) {
                fields[f] = fields[f].strip();
                if (fields[f].isEmpty()) {
                    throw invalid(number, "an entry with an empty field");
                }
            }
            Entry entry = new Entry(fields[0], fields[1], fields[2], fields[3], number);
            requireKnownItem(entry);
            if (!declared.add(List.of(entry.gate(), entry.module(), entry.item()))) {
                throw invalid(number, "an entry declared twice");
            }
            entries.add(entry);
        }
        return new DeclaredBreaks(baseline, entries);
    }

    private static void requireKnownItem(Entry entry) {
        if (!GATES.contains(entry.gate())) {
            throw invalid(entry.line(), "an unknown gate '" + entry.gate() + "', expected one of "
                    + GATES.stream().sorted().collect(Collectors.joining(", ")));
        }
        if (entry.gate().equals("schema-compatibility") && !entry.item().equals("*")) {
            throw invalid(entry.line(), "a schema-compatibility entry whose item is not '*'");
        }
        if (entry.gate().equals("openapi") && OPENAPI_ITEMS.stream().noneMatch(entry.item()::startsWith)) {
            throw invalid(entry.line(), "an openapi entry whose item is not 'finding <fingerprint>', "
                    + "'problem-type <slug>' or 'server <level> <template>'");
        }
    }

    private static GradleException invalid(int line, String what) {
        return new GradleException(PATH + " line " + line + " is " + what + " (LLD-api-stability §3)");
    }

    /**
     * The file, once its entries are known to be bound to {@code tandemBaseline} (§3, rule 1). A file
     * without entries is always valid.
     *
     * @throws GradleException when the file holds entries and its baseline line names another release, or
     *                         no baseline is set: then no gate compares, and every entry is stale (rule 2)
     */
    public DeclaredBreaks boundTo(String tandemBaseline) {
        if (!entries.isEmpty() && tandemBaseline.isEmpty()) {
            throw new GradleException(PATH + " declares breaks, but no compatibility baseline is set: no gate "
                    + "compares with a baseline, so no entry can match a break (LLD-api-stability §3).");
        }
        if (!entries.isEmpty() && !tandemBaseline.equals(baseline)) {
            throw new GradleException(PATH + " declares breaks against baseline "
                    + (baseline == null || baseline.isEmpty() ? "<none>" : baseline) + ", but tandemBaseline is "
                    + tandemBaseline
                    + ". Declarations are bound to their baseline: when the baseline moves, the file is emptied "
                    + "and its baseline line set to the new value (LLD-api-stability §3).");
        }
        return this;
    }

    /**
     * Fails on an entry of {@code gate} naming a module where that gate does not run: no run would ever
     * match it, so it could only be a misspelt module.
     */
    public DeclaredBreaks requireGateModules(String gate, Collection<String> modules) {
        List<Entry> elsewhere = entries.stream()
                .filter(entry -> entry.gate().equals(gate) && !modules.contains(entry.module()))
                .toList();
        if (!elsewhere.isEmpty()) {
            throw new GradleException("Declared breaks name a module without the " + gate + " gate, which runs in "
                    + modules.stream().sorted().collect(Collectors.joining(", ")) + ":\n" + lines(elsewhere));
        }
        return this;
    }

    public List<Entry> entries(String gate, String module) {
        return entries.stream().filter(entry -> entry.gate().equals(gate) && entry.module().equals(module)).toList();
    }

    /**
     * Matches the breaks a gate reported in this run with its declarations: a break with a declaration
     * is accepted, one without is undeclared, and a declaration no break used is stale (§3, rule 2).
     */
    public static Matching match(Collection<String> reported, List<Entry> declarations) {
        Map<String, Entry> byItem = new LinkedHashMap<>();
        declarations.forEach(entry -> byItem.put(entry.item(), entry));
        Map<String, Entry> accepted = new LinkedHashMap<>();
        List<String> undeclared = new ArrayList<>();
        for (String item : reported) {
            Entry entry = byItem.get(item);
            if (entry == null) {
                undeclared.add(item);
            } else {
                accepted.put(item, entry);
            }
        }
        List<Entry> stale = declarations.stream().filter(entry -> !accepted.containsKey(entry.item())).toList();
        return new Matching(accepted, undeclared, stale);
    }

    /** The entry that would accept a break of {@code item}, its release-notes line left to write. */
    public static String declaration(String gate, String module, String item) {
        return gate + " | " + module + " | " + item + " | <release-notes line>";
    }

    /** One line per entry, for a failure message. */
    public static String lines(Collection<Entry> entries) {
        return entries.stream().map(entry -> "  " + entry).collect(Collectors.joining("\n"));
    }
}
