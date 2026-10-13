package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipOutputStream;
import javax.tools.JavaCompiler;
import javax.tools.ToolProvider;
import me.champeau.gradle.japicmp.JApiCmpWorkerAction;
import me.champeau.gradle.japicmp.JapiCmpWorkerConfiguration;
import me.champeau.gradle.japicmp.report.GroovyReportRenderer;
import me.champeau.gradle.japicmp.report.RichReport;
import me.champeau.gradle.japicmp.report.RichReportData;
import me.champeau.gradle.japicmp.report.RichReportRenderer;
import me.champeau.gradle.japicmp.report.Violation;
import org.gradle.api.GradleException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Runs the plugin's own worker action, as {@code checkJavaApi} configures it, on jars compiled here: the rules
 * are loaded the way the plugin loads them, and japicmp's report filter applies the access level before they
 * run.
 */
class JavaApiRulesTest {

    private static final String MODULE = "tandem-core";
    private static final String RELEASE_NOTE = "Removed, use the builder.";

    private static final String STORE = "com.acme.Store";
    private static final String ROUTER = "com.acme.Router";
    private static final String STORE_FLUSH = STORE + ".flush()";
    private static final String ROUTER_FLUSH = ROUTER + ".flush()";
    private static final String RELAY = "com.acme.Relay";
    private static final String RELAY_START = RELAY + ".start(int)";

    private static final Map<String, String> STORE_WITHOUT_FLUSH = port("Store", "");
    private static final Map<String, String> STORE_WITH_FLUSH = port("Store", " void flush();");
    private static final Map<String, String> ROUTER_WITHOUT_FLUSH = port("Router", "");
    private static final Map<String, String> ROUTER_WITH_FLUSH = port("Router", " void flush();");

    private static final Map<String, String> RELAY_WITH_START = Map.of("com/acme/Relay.java", """
            package com.acme;
            public class Relay {
                public void start(int workers) {}
                public void stop() {}
            }
            """);
    private static final Map<String, String> RELAY_WITHOUT_START = Map.of("com/acme/Relay.java", """
            package com.acme;
            public class Relay {
                public void stop() {}
            }
            """);

    @TempDir
    Path directory;

    /**
     * What one comparison concluded.
     *
     * @param failed     whether an error in the rich report failed the worker
     * @param outcome    what the task reads: accepted declarations and problems
     * @param violations each reported element as {@code <severity> <element>}, by type
     * @param report     the HTML report
     */
    private record Comparison(boolean failed, JavaApiBreaks.Outcome outcome, Map<String, List<String>> violations,
            String report) {

        List<String> of(String type) {
            return violations.getOrDefault(type, List.of());
        }
    }

    private static Map<String, String> port(String name, String members) {
        return Map.of("com/acme/" + name + ".java",
                "package com.acme; public interface " + name + " { String name();" + members + " }");
    }

    @Test
    void GIVEN_a_provided_interface_WHEN_it_gains_an_abstract_method_THEN_the_gate_passes_accepting_the_method_and_its_type() {
        Comparison comparison = compare(STORE_WITHOUT_FLUSH, STORE_WITH_FLUSH, Set.of(STORE));

        assertThat(comparison.failed()).isFalse();
        assertThat(comparison.outcome().problems()).isEmpty();
        assertThat(comparison.of(STORE)).containsExactly("accepted Class " + STORE, "accepted Method " + STORE_FLUSH);
    }

    @Test
    void GIVEN_an_SPI_interface_WHEN_it_gains_the_same_abstract_method_THEN_the_gate_fails_on_the_method_and_its_type() {
        Comparison comparison = compare(ROUTER_WITHOUT_FLUSH, ROUTER_WITH_FLUSH, Set.of(STORE));

        assertThat(comparison.failed()).isTrue();
        assertThat(comparison.outcome().problems()).containsExactly(problem(ROUTER_FLUSH, "METHOD_ADDED_TO_INTERFACE"));
        assertThat(comparison.of(ROUTER)).containsExactly("error Class " + ROUTER, "error Method " + ROUTER_FLUSH);
    }

    @Test
    void GIVEN_a_public_method_removed_WHEN_no_declaration_names_it_THEN_the_gate_fails_naming_it() {
        Comparison comparison = compare(RELAY_WITH_START, RELAY_WITHOUT_START, Set.of());

        assertThat(comparison.failed()).isTrue();
        assertThat(comparison.outcome().problems()).containsExactly(problem(RELAY_START, "METHOD_REMOVED"));
        assertThat(comparison.of(RELAY)).containsExactly("error Class " + RELAY, "error Method " + RELAY_START);
        assertThat(comparison.report()).as("the declaration to write, readable in the HTML report")
                .contains("japicmp | " + MODULE + " | " + RELAY_START + " | &lt;release-notes line&gt;");
    }

    @Test
    void GIVEN_a_public_method_removed_WHEN_a_declaration_names_it_THEN_the_gate_passes_with_its_release_notes_line() {
        Comparison comparison = compare(RELAY_WITH_START, RELAY_WITHOUT_START, Set.of(),
                "japicmp | " + MODULE + " | " + RELAY_START + " | " + RELEASE_NOTE);

        assertThat(comparison.failed()).isFalse();
        assertThat(comparison.outcome().problems()).isEmpty();
        assertThat(comparison.outcome().accepted()).singleElement().asString()
                .contains(RELAY_START).endsWith("release notes: " + RELEASE_NOTE);
        assertThat(comparison.of(RELAY)).containsExactly("accepted Class " + RELAY, "accepted Method " + RELAY_START);
    }

    @Test
    void GIVEN_a_declaration_WHEN_no_change_matches_it_THEN_the_gate_fails_asking_to_remove_it() {
        Comparison comparison = compare(RELAY_WITH_START, RELAY_WITH_START, Set.of(),
                "japicmp | " + MODULE + " | " + RELAY_START + " | " + RELEASE_NOTE);

        assertThat(comparison.outcome().accepted()).isEmpty();
        assertThat(comparison.outcome().problems()).singleElement().asString()
                .startsWith("Declared break matches no incompatible change, remove it")
                .contains(RELAY_START);
    }

    @Test
    void GIVEN_a_declaration_naming_another_module_WHEN_this_module_breaks_the_same_member_THEN_the_break_stays_undeclared() {
        Comparison comparison = compare(RELAY_WITH_START, RELAY_WITHOUT_START, Set.of(),
                "japicmp | tandem-jdbc | " + RELAY_START + " | " + RELEASE_NOTE);

        assertThat(comparison.failed()).isTrue();
        assertThat(comparison.outcome().problems()).containsExactly(problem(RELAY_START, "METHOD_REMOVED"));
    }

    @Test
    void GIVEN_a_type_removed_or_no_longer_public_WHEN_compared_THEN_each_is_one_break_its_members_going_with_it() {
        Map<String, String> baseline = Map.of(
                "com/acme/Clock.java", "package com.acme; public class Clock { public long tick() { return 0; } }",
                "com/acme/Backoff.java", "package com.acme; public class Backoff { public Backoff() {} }");
        Map<String, String> current = Map.of(
                "com/acme/Backoff.java", "package com.acme; class Backoff { Backoff() {} }");

        Comparison comparison = compare(baseline, current, Set.of());

        assertThat(comparison.outcome().problems()).containsExactly(
                problem("com.acme.Backoff", "CLASS_NO_LONGER_PUBLIC, CLASS_LESS_ACCESSIBLE"),
                problem("com.acme.Clock", "CLASS_REMOVED, SUPERCLASS_REMOVED"));
        assertThat(comparison.of("com.acme.Clock")).containsExactly("error Class com.acme.Clock");
        assertThat(comparison.of("com.acme.Backoff")).containsExactly("error Class com.acme.Backoff");
    }

    @Test
    void GIVEN_a_class_made_final_and_one_no_longer_implementing_an_interface_WHEN_compared_THEN_each_change_is_a_break() {
        Map<String, String> baseline = Map.of(
                "com/acme/Relay.java", "package com.acme; public class Relay {}",
                "com/acme/Pool.java", "package com.acme; public class Pool implements AutoCloseable { "
                        + "public void close() {} }");
        Map<String, String> current = Map.of(
                "com/acme/Relay.java", "package com.acme; public final class Relay {}",
                "com/acme/Pool.java", "package com.acme; public class Pool { public void close() {} }");

        Comparison comparison = compare(baseline, current, Set.of());

        assertThat(comparison.outcome().problems()).containsExactly(
                problem("com.acme.Pool implements java.lang.AutoCloseable", "INTERFACE_REMOVED"),
                problem(RELAY, "CLASS_NOW_FINAL"));
        assertThat(comparison.of(RELAY)).containsExactly("error Class " + RELAY);
    }

    @Test
    void GIVEN_compatible_evolution_WHEN_compared_THEN_the_gate_passes_reporting_nothing() {
        Map<String, String> baseline = Map.of(
                "com/acme/Router.java", "package com.acme; public interface Router { String topic(); }",
                "com/acme/Status.java", "package com.acme; public enum Status { PENDING }",
                "com/acme/Criteria.java", "package com.acme; public record Criteria(String type) {}");
        Map<String, String> current = Map.of(
                "com/acme/Router.java", """
                        package com.acme;
                        public interface Router {
                            String topic();
                            default String key() { return ""; }
                        }
                        """,
                "com/acme/Status.java", "package com.acme; public enum Status { PENDING, DONE }",
                "com/acme/Criteria.java", """
                        package com.acme;
                        public record Criteria(String type, int limit) {
                            public Criteria(String type) { this(type, 100); }
                        }
                        """,
                "com/acme/Builder.java", "package com.acme; public final class Builder {}");

        Comparison comparison = compare(baseline, current, Set.of());

        assertThat(comparison.failed()).isFalse();
        assertThat(comparison.outcome().problems()).isEmpty();
        assertThat(comparison.violations()).isEmpty();
    }

    @Test
    void GIVEN_a_record_component_added_WHEN_the_previous_canonical_constructor_is_not_kept_THEN_the_gate_fails() {
        Comparison comparison = compare(
                Map.of("com/acme/Criteria.java", "package com.acme; public record Criteria(String type) {}"),
                Map.of("com/acme/Criteria.java", "package com.acme; public record Criteria(String type, int limit) {}"),
                Set.of());

        assertThat(comparison.outcome().problems()).containsExactly(
                problem("com.acme.Criteria(java.lang.String)", "CONSTRUCTOR_REMOVED"));
    }

    @Test
    void GIVEN_only_the_type_arguments_of_a_signature_change_WHEN_compared_THEN_the_gate_fails_on_the_source_break() {
        Comparison comparison = compare(
                Map.of("com/acme/Query.java", """
                        package com.acme;
                        public class Query {
                            public java.util.List<String> ids() { return null; }
                            public void exclude(java.util.List<String> ids) {}
                        }
                        """),
                Map.of("com/acme/Query.java", """
                        package com.acme;
                        public class Query {
                            public java.util.List<Long> ids() { return null; }
                            public void exclude(java.util.List<Long> ids) {}
                        }
                        """),
                Set.of());

        assertThat(comparison.outcome().problems()).containsExactly(
                problem("com.acme.Query.exclude(java.util.List)", "METHOD_PARAMETER_GENERICS_CHANGED"),
                problem("com.acme.Query.ids()", "METHOD_RETURN_TYPE_GENERICS_CHANGED"));
    }

    @Test
    void GIVEN_changes_outside_the_api_WHEN_compared_THEN_the_gate_passes_reporting_nothing() {
        Map<String, String> baseline = Map.of(
                "com/acme/internal/Mapper.java", "package com.acme.internal; public class Mapper {}",
                "com/acme/internal/dto/Response.java", "package com.acme.internal.dto; public record Response() {}",
                "com/acme/Codec.java", "package com.acme; class Codec {}",
                "com/acme/Relay.java", """
                        package com.acme;
                        public class Relay {
                            void poll() {}
                            private void sleep() {}
                        }
                        """);
        Map<String, String> current = Map.of("com/acme/Relay.java", "package com.acme; public class Relay {}");

        Comparison comparison = compare(baseline, current, Set.of());

        assertThat(comparison.failed()).isFalse();
        assertThat(comparison.outcome().problems()).isEmpty();
        assertThat(comparison.violations()).isEmpty();
    }

    private String problem(String item, String changes) {
        return item + ": " + changes + "; if intended, declare it as 'japicmp | " + MODULE + " | " + item
                + " | <release-notes line>'";
    }

    private Comparison compare(Map<String, String> baseline, Map<String, String> current, Set<String> provided,
            String... declarations) {
        File declaredBreaks = write("accepted-breaks.txt", "baseline 1.0.0\n" + String.join("\n", declarations) + "\n");
        File providedInterfaces = write("provided-interfaces.txt",
                provided.stream().map(name -> name + "\n").collect(Collectors.joining()));
        File outcome = directory.resolve("java-api.txt").toFile();
        File report = directory.resolve("report.html").toFile();
        RichReport.Configuration richReport = new RichReport.Configuration(report.getName(), RecordingRenderer.class,
                List.of(), List.of(), "Java API", null,
                new ArrayList<>(JavaApiRules.configurations(MODULE, declaredBreaks, providedInterfaces, outcome)), false,
                report);
        // The settings checkJavaApi gives the plugin: its own failure switches off, only modified elements, a
        // markdown report (writing it applies the access level to the comparison before the rules run).
        JapiCmpWorkerConfiguration configuration = new JapiCmpWorkerConfiguration(false, false,
                List.of(), CheckJavaApi.PACKAGE_EXCLUDES, List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(), List.of(),
                List.of(new JApiCmpWorkerAction.Archive(jar("baseline", baseline), "baseline")),
                List.of(new JApiCmpWorkerAction.Archive(jar("current", current), "current")),
                true, false, false, false, CheckJavaApi.ACCESS_MODIFIER, null, null,
                directory.resolve("report.md").toFile(), null, null, false, richReport);
        boolean failed = false;
        try {
            new JApiCmpWorkerAction(configuration).run();
        } catch (GradleException e) {
            failed = true;
        }
        String html;
        try {
            html = Files.readString(report.toPath(), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return new Comparison(failed, JavaApiBreaks.Outcome.read(outcome.toPath()), RecordingRenderer.read(report),
                html);
    }

    private File write(String name, String text) {
        Path file = directory.resolve(name);
        try {
            Files.writeString(file, text, StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return file.toFile();
    }

    private File jar(String name, Map<String, String> sources) {
        Path sourceRoot = directory.resolve(name + "-sources");
        Path classes = directory.resolve(name + "-classes");
        Path jar = directory.resolve(name + ".jar");
        List<String> arguments = new ArrayList<>(List.of("--release", "17", "-d", classes.toString()));
        try {
            Files.createDirectories(classes);
            for (Map.Entry<String, String> source : sources.entrySet()) {
                Path file = sourceRoot.resolve(source.getKey());
                Files.createDirectories(file.getParent());
                Files.writeString(file, source.getValue(), StandardCharsets.UTF_8);
                arguments.add(file.toString());
            }
            JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
            assertThat(compiler.run(null, null, null, arguments.toArray(String[]::new))).as("javac exit code").isZero();
            try (OutputStream out = Files.newOutputStream(jar); ZipOutputStream zip = new ZipOutputStream(out);
                    Stream<Path> files = Files.walk(classes)) {
                for (Path file : files.filter(Files::isRegularFile).sorted().toList()) {
                    zip.putNextEntry(new ZipEntry(classes.relativize(file).toString().replace(File.separatorChar, '/')));
                    zip.write(Files.readAllBytes(file));
                    zip.closeEntry();
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        return jar.toFile();
    }

    /**
     * Renders the real HTML report, and next to it the violations it holds, one {@code <severity> <element>}
     * line each under its type, so a test can read the verdict of a type as well as its members'.
     */
    public static final class RecordingRenderer implements RichReportRenderer {

        @Override
        public void render(File htmlReportFile, RichReportData data) {
            new GroovyReportRenderer().render(htmlReportFile, data);
            StringBuilder text = new StringBuilder();
            data.getViolations().forEach((type, violations) -> {
                for (Violation violation : violations) {
                    text.append(type).append('\t').append(violation.getSeverity()).append(' ')
                            .append(Violation.describe(violation.getMember())).append('\n');
                }
            });
            try {
                Files.writeString(recording(htmlReportFile), text, StandardCharsets.UTF_8);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        static Map<String, List<String>> read(File htmlReportFile) {
            Map<String, List<String>> violations = new LinkedHashMap<>();
            try {
                for (String line : Files.readAllLines(recording(htmlReportFile), StandardCharsets.UTF_8)) {
                    String[] fields = line.split("\t", 2);
                    violations.computeIfAbsent(fields[0], type -> new ArrayList<>()).add(fields[1]);
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            return violations;
        }

        private static Path recording(File htmlReportFile) {
            return htmlReportFile.toPath().resolveSibling(htmlReportFile.getName() + ".violations");
        }
    }
}
