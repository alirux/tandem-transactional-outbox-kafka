package com.codingful.tandem.compatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * The names of the Admin API contract no comparison of schemas reaches (LLD-api-stability §6.3): the problem
 * {@code type} slugs, and the servers a client calls. Both documents are read as oasdiff loads them, a JSON
 * tree of maps, lists and scalars.
 *
 * <p>A slug of the baseline must still appear in some string of the current document. Servers are compared
 * where they are declared: the document's, else OpenAPI's default {@code /}, and those of every path and
 * operation that declares its own in either document and is present in both. At each such level the
 * effective servers are matched by their literal URL template: each baseline template must remain, every
 * variable it references must keep its default, and an enum may grow, but not lose a value nor appear on a
 * variable that had none. A level that inherits its servers in both documents is covered by the level it
 * inherits from, so a change is reported once per definition, not once per operation that uses it.
 */
public final class FrozenNames {

    /** The item of a problem type, followed by its slug. */
    public static final String PROBLEM_TYPE = "problem-type ";
    /** The item of a server definition, followed by its level and template. */
    public static final String SERVER = "server ";

    static final Pattern PROBLEM_TYPE_URL =
            Pattern.compile("https://tandem\\.codingful\\.com/problems/([a-z0-9]+(?:-[a-z0-9]+)*)");
    private static final Pattern VARIABLE = Pattern.compile("\\{([^{}]+)}");
    private static final List<String> METHODS =
            List.of("get", "put", "post", "delete", "options", "head", "patch", "trace", "query");
    private static final List<Object> DEFAULT_SERVERS = List.of(Map.of("url", "/"));

    private FrozenNames() {
    }

    /**
     * Every frozen name of {@code baseline} that {@code current} lost or changed, as item and what changed,
     * in a stable order: slugs first, then the document's servers, then each path and operation.
     */
    public static Map<String, String> compare(Map<?, ?> baseline, Map<?, ?> current) {
        Map<String, String> found = new LinkedHashMap<>();
        Set<String> currentSlugs = slugs(current);
        slugs(baseline).stream().filter(slug -> !currentSlugs.contains(slug))
                .forEach(slug -> found.put(PROBLEM_TYPE + slug, "no longer appears in the document"));
        List<?> baselineDocument = effective(baseline, DEFAULT_SERVERS);
        List<?> currentDocument = effective(current, DEFAULT_SERVERS);
        compareServers("document", baselineDocument, currentDocument, found);
        Map<?, ?> currentPaths = map(current.get("paths"));
        map(baseline.get("paths")).forEach((path, baselineItem) -> {
            if (!currentPaths.containsKey(path)) {
                return;
            }
            Map<?, ?> baselinePath = map(baselineItem);
            Map<?, ?> currentPath = map(currentPaths.get(path));
            List<?> baselinePathServers = effective(baselinePath, baselineDocument);
            List<?> currentPathServers = effective(currentPath, currentDocument);
            if (declares(baselinePath) || declares(currentPath)) {
                compareServers("path " + path, baselinePathServers, currentPathServers, found);
            }
            for (String method : METHODS) {
                if (baselinePath.containsKey(method) && currentPath.containsKey(method)) {
                    Map<?, ?> baselineOperation = map(baselinePath.get(method));
                    Map<?, ?> currentOperation = map(currentPath.get(method));
                    if (declares(baselineOperation) || declares(currentOperation)) {
                        compareServers("operation " + method.toUpperCase(Locale.ROOT) + " " + path,
                                effective(baselineOperation, baselinePathServers),
                                effective(currentOperation, currentPathServers), found);
                    }
                }
            }
        });
        return found;
    }

    /** The slugs of every problem type URL in the strings of a document. */
    static Set<String> slugs(Object node) {
        Set<String> slugs = new LinkedHashSet<>();
        collectSlugs(node, slugs);
        return slugs;
    }

    private static void collectSlugs(Object node, Set<String> slugs) {
        if (node instanceof Map<?, ?> map) {
            map.values().forEach(value -> collectSlugs(value, slugs));
        } else if (node instanceof Collection<?> list) {
            list.forEach(value -> collectSlugs(value, slugs));
        } else if (node instanceof String text) {
            Matcher matcher = PROBLEM_TYPE_URL.matcher(text);
            while (matcher.find()) {
                slugs.add(matcher.group(1));
            }
        }
    }

    private static void compareServers(String level, List<?> baseline, List<?> current, Map<String, String> found) {
        Map<String, Map<?, ?>> currentByTemplate = new LinkedHashMap<>();
        current.forEach(server -> currentByTemplate.putIfAbsent(url(server), map(server)));
        Set<String> compared = new LinkedHashSet<>();
        for (Object server : baseline) {
            String template = url(server);
            if (!compared.add(template)) {
                continue;
            }
            Map<?, ?> kept = currentByTemplate.get(template);
            List<String> changes = kept == null
                    ? List.of("no longer offered, the servers are " + String.join(", ", currentByTemplate.keySet()))
                    : variableChanges(template, map(server), kept);
            if (!changes.isEmpty()) {
                found.put(SERVER + level + " " + template, String.join("; ", changes));
            }
        }
    }

    private static List<String> variableChanges(String template, Map<?, ?> baseline, Map<?, ?> current) {
        List<String> changes = new ArrayList<>();
        Map<?, ?> baselineVariables = map(baseline.get("variables"));
        Map<?, ?> currentVariables = map(current.get("variables"));
        Set<String> referenced = new LinkedHashSet<>();
        Matcher matcher = VARIABLE.matcher(template);
        while (matcher.find()) {
            referenced.add(matcher.group(1));
        }
        for (String name : referenced) {
            Map<?, ?> was = map(baselineVariables.get(name));
            Map<?, ?> is = map(currentVariables.get(name));
            Object wasDefault = was.get("default");
            Object isDefault = is.get("default");
            if (!Objects.equals(wasDefault, isDefault)) {
                changes.add(name + " default " + wasDefault + ", now " + isDefault);
            }
            List<?> wasEnum = was.get("enum") instanceof List<?> values ? values : null;
            List<?> isEnum = is.get("enum") instanceof List<?> values ? values : null;
            if (wasEnum == null && isEnum != null) {
                changes.add(name + " had no enum, now " + isEnum);
            } else if (wasEnum != null && isEnum != null && !isEnum.containsAll(wasEnum)) {
                List<Object> lost = new ArrayList<>(wasEnum);
                lost.removeAll(isEnum);
                changes.add(name + " enum " + wasEnum + ", now " + isEnum + ", without " + lost);
            }
        }
        return changes;
    }

    /** A level's own servers when it declares any, else those it inherits. */
    private static List<?> effective(Map<?, ?> level, List<?> inherited) {
        return declares(level) ? (List<?>) level.get("servers") : inherited;
    }

    private static boolean declares(Map<?, ?> level) {
        return level.get("servers") instanceof List<?> servers && !servers.isEmpty();
    }

    private static String url(Object server) {
        return String.valueOf(map(server).get("url"));
    }

    private static Map<?, ?> map(Object node) {
        return node instanceof Map<?, ?> map ? map : Map.of();
    }
}
