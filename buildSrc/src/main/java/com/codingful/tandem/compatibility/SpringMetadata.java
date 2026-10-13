package com.codingful.tandem.compatibility;

import groovy.json.JsonOutput;
import groovy.json.JsonSlurper;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import org.gradle.api.GradleException;

/**
 * The Spring configuration metadata a module's jar carries, compared with the baseline's
 * (LLD-api-stability §7). A property of the baseline breaks when it is absent, carries a {@code deprecation}
 * of level {@code error} (Spring's way of declaring a key removed), declared a {@code type} and now declares
 * another or none, or declared a {@code defaultValue} and now declares another or none. New properties pass,
 * and so does a type or a default declared where the baseline declared none. Descriptions, groups, hints and
 * source types are not compared. A break is named by its key, the item a {@code spring-metadata} declaration
 * carries.
 *
 * <p>A file the gate cannot compare (missing from a jar, not the JSON the processor writes, or declaring a key
 * twice with different facts) is an error, which no declaration accepts (§3, rule 4).
 */
public final class SpringMetadata {

    /** The gate kind of the declared breaks {@code checkSpringMetadata} reads. */
    public static final String GATE = "spring-metadata";

    /** Where a jar carries the metadata the configuration processor writes. */
    public static final String PATH = "META-INF/spring-configuration-metadata.json";

    private static final String REMOVED = "error";

    /**
     * What the gate compares of one property.
     *
     * @param type         its declared type; {@code null} when it declares none
     * @param defaultValue its declared default, rendered as JSON; {@code null} when it declares none
     * @param removed      whether it carries a deprecation of level {@code error}
     */
    public record Property(String type, String defaultValue, boolean removed) {
    }

    /**
     * How the two files compared.
     *
     * @param compared the baseline's properties the gate compared
     * @param problems one line per problem; empty when the check passes
     * @param accepted the declarations that accepted a break
     */
    public record Outcome(int compared, List<String> problems, List<DeclaredBreaks.Entry> accepted) {
    }

    private SpringMetadata() {
    }

    /**
     * The properties the jar's metadata declares, by key.
     *
     * @throws GradleException when the jar carries no metadata, or metadata the gate cannot read
     */
    public static Map<String, Property> read(File jar) {
        String json;
        try (ZipFile zip = new ZipFile(jar)) {
            ZipEntry entry = zip.getEntry(PATH);
            if (entry == null) {
                throw new GradleException(jar + " carries no " + PATH + ": the configuration processor wrote none, "
                        + "and the gate has nothing to compare.");
            }
            try (InputStream in = zip.getInputStream(entry)) {
                json = new String(in.readAllBytes(), StandardCharsets.UTF_8);
            }
        } catch (IOException e) {
            throw new GradleException("Cannot read " + PATH + " in " + jar, e);
        }
        return parse(json, PATH + " in " + jar);
    }

    /**
     * The properties a metadata file declares, by key.
     *
     * @param source names the file in a failure message
     * @throws GradleException when the text is not the metadata the configuration processor writes, or
     *                         declares one key twice with different facts
     */
    static Map<String, Property> parse(String json, String source) {
        Object document;
        try {
            document = new JsonSlurper().parseText(json);
        } catch (RuntimeException e) {
            throw new GradleException("Cannot read " + source + ": it is not JSON", e);
        }
        if (!(document instanceof Map<?, ?> root)) {
            throw new GradleException("Cannot read " + source + ": it is not a JSON object");
        }
        Object properties = root.get("properties");
        if (properties == null) {
            properties = List.of();
        }
        if (!(properties instanceof List<?> list)) {
            throw new GradleException("Cannot read " + source + ": its properties are not a JSON array");
        }
        Map<String, Property> byKey = new TreeMap<>();
        for (Object element : list) {
            if (!(element instanceof Map<?, ?> object) || !(object.get("name") instanceof String key)) {
                throw new GradleException("Cannot read " + source + ": it holds a property without a name: " + element);
            }
            Property property = new Property(
                    object.get("type") instanceof String type ? type : null,
                    object.containsKey("defaultValue") ? JsonOutput.toJson(object.get("defaultValue")) : null,
                    object.get("deprecation") instanceof Map<?, ?> deprecation
                            && REMOVED.equals(deprecation.get("level")));
            Property previous = byKey.putIfAbsent(key, property);
            if (previous != null && !previous.equals(property)) {
                throw new GradleException("Cannot read " + source + ": it declares " + key
                        + " twice, with a different type, default or deprecation level");
            }
        }
        return byKey;
    }

    /**
     * The baseline's properties the current metadata breaks, each with why, by key. A property the baseline
     * already declares removed is not compared: nothing of it is left to keep.
     */
    static Map<String, String> breaks(Map<String, Property> baseline, Map<String, Property> current) {
        Map<String, String> breaks = new TreeMap<>();
        baseline.forEach((key, before) -> {
            if (before.removed()) {
                return;
            }
            Property after = current.get(key);
            if (after == null) {
                breaks.put(key, "is absent");
                return;
            }
            List<String> reasons = new ArrayList<>();
            if (after.removed()) {
                reasons.add("carries a deprecation of level error");
            }
            if (before.type() != null && !before.type().equals(after.type())) {
                reasons.add("had type " + before.type() + ", now " + Objects.requireNonNullElse(after.type(), "none"));
            }
            if (before.defaultValue() != null && !before.defaultValue().equals(after.defaultValue())) {
                reasons.add("declared default " + before.defaultValue() + ", now "
                        + Objects.requireNonNullElse(after.defaultValue(), "none"));
            }
            if (!reasons.isEmpty()) {
                breaks.put(key, String.join("; ", reasons));
            }
        });
        return breaks;
    }

    /**
     * What the comparison makes fail: a break no declaration accepts, and a declaration no break used.
     *
     * @param module       the module, as declarations name it
     * @param declarations the module's {@code spring-metadata} entries of the declared breaks
     */
    public static Outcome judge(String module, Map<String, Property> baseline, Map<String, Property> current,
            List<DeclaredBreaks.Entry> declarations) {
        Map<String, String> breaks = breaks(baseline, current);
        DeclaredBreaks.Matching matching = DeclaredBreaks.match(breaks.keySet(), declarations);
        List<String> problems = new ArrayList<>();
        for (String key : matching.undeclared()) {
            problems.add(key + " " + breaks.get(key) + "; if intended, declare it as '" + GATE + " | " + module
                    + " | " + key + " | <release-notes line>'");
        }
        for (DeclaredBreaks.Entry stale : matching.stale()) {
            problems.add("Declared break matches no property the metadata breaks, remove it: " + stale);
        }
        int compared = (int) baseline.values().stream().filter(property -> !property.removed()).count();
        return new Outcome(compared, problems, List.copyOf(matching.accepted().values()));
    }
}
