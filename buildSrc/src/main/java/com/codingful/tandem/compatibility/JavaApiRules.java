package com.codingful.tandem.compatibility;

import japicmp.model.JApiCompatibility;
import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import me.champeau.gradle.japicmp.report.AbstractContextAwareViolationRule;
import me.champeau.gradle.japicmp.report.PostProcessRuleConfiguration;
import me.champeau.gradle.japicmp.report.PostProcessViolationsRule;
import me.champeau.gradle.japicmp.report.RuleConfiguration;
import me.champeau.gradle.japicmp.report.SetupRule;
import me.champeau.gradle.japicmp.report.SetupRuleConfiguration;
import me.champeau.gradle.japicmp.report.Violation;
import me.champeau.gradle.japicmp.report.ViolationCheckContext;
import me.champeau.gradle.japicmp.report.ViolationCheckContextWithViolations;
import me.champeau.gradle.japicmp.report.ViolationRuleConfiguration;

/**
 * The rich-report rules of {@code checkJavaApi} (LLD-api-stability §4.2). The plugin runs japicmp in a worker
 * process and loads each rule there from the jar its class comes from, which is why they live in
 * {@code buildSrc} (§4.4): {@link Setup} reads the declared breaks and the provided interfaces, {@link Judge}
 * gives each element of the comparison its verdict, and {@link Conclude} writes the outcome the task reports.
 * An error in the report fails the worker; the task then fails with the outcome's problems.
 */
public final class JavaApiRules {

    static final String MODULE = "module";
    static final String DECLARED_BREAKS = "declaredBreaks";
    static final String PROVIDED_INTERFACES = "providedInterfaces";
    static final String OUTCOME = "outcome";

    /** Where the rules share the judge of one comparison. */
    private static final String JUDGE = JavaApiBreaks.class.getName();

    private JavaApiRules() {
    }

    /**
     * The rules comparing one module, in the order the rich report runs them.
     *
     * @param module             the module compared, as declarations name it
     * @param declaredBreaks     {@code gradle/accepted-breaks.txt}
     * @param providedInterfaces the file {@code checkInterfaceKinds} writes (§4.3)
     * @param outcome            where {@link Conclude} writes the outcome
     */
    public static List<RuleConfiguration<?>> configurations(String module, File declaredBreaks, File providedInterfaces,
            File outcome) {
        Map<String, String> arguments = Map.of(MODULE, module, DECLARED_BREAKS, declaredBreaks.getAbsolutePath(),
                PROVIDED_INTERFACES, providedInterfaces.getAbsolutePath(), OUTCOME, outcome.getAbsolutePath());
        return List.of(new SetupRuleConfiguration(Setup.class, arguments),
                new ViolationRuleConfiguration(Judge.class, null),
                new PostProcessRuleConfiguration(Conclude.class, arguments));
    }

    /** Reads the module's {@code japicmp} declarations and provided interfaces, before the first element. */
    public static final class Setup implements SetupRule {

        private final Map<String, String> arguments;

        public Setup(Map<String, String> arguments) {
            this.arguments = arguments;
        }

        @Override
        public void execute(ViolationCheckContext context) {
            String module = arguments.get(MODULE);
            List<DeclaredBreaks.Entry> declarations = DeclaredBreaks.read(new File(arguments.get(DECLARED_BREAKS)))
                    .entries(JavaApiBreaks.GATE, module);
            Set<String> provided;
            try {
                // One binary name per line, as checkInterfaceKinds writes them.
                provided = Set.copyOf(Files.readAllLines(Path.of(arguments.get(PROVIDED_INTERFACES)),
                        StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
            context.putUserData(JUDGE, new JavaApiBreaks(module, provided, declarations));
        }
    }

    /** Reports every element that breaks the API: an error unless a provided interface or a declaration allows it. */
    public static final class Judge extends AbstractContextAwareViolationRule {

        @Override
        public Violation maybeViolation(JApiCompatibility element) {
            JavaApiBreaks breaks = getContext().getUserData(JUDGE);
            JavaApiBreaks.Judgement judgement = breaks.judgement(element);
            // The report's template writes the explanation into the HTML as it is, so it is escaped here.
            return switch (judgement.verdict()) {
                case COMPATIBLE -> null;
                case PROVIDED -> Violation.accept(element,
                        "Method added to a provided interface, which a minor version may do (HLD-api-stability §4.1)");
                case DECLARED -> Violation.accept(element, html("Declared break " + judgement.item()
                        + ", release notes: " + judgement.entry().releaseNote()));
                case UNDECLARED -> Violation.error(element, html("Breaks the API ("
                        + JavaApiBreaks.names(judgement.changes()) + "); if intended, declare it as '"
                        + breaks.declaration(judgement.item()) + "'"));
                case MEMBERS_ACCEPTED -> Violation.accept(element, "Incompatible only through members, each of them "
                        + "accepted");
                case MEMBERS_UNDECLARED -> Violation.error(element, "Incompatible through members not accepted");
            };
        }

        private static String html(String text) {
            return text.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;");
        }
    }

    /** Writes what the comparison concluded, stale declarations included, for the task to report. */
    public static final class Conclude implements PostProcessViolationsRule {

        private final Map<String, String> arguments;

        public Conclude(Map<String, String> arguments) {
            this.arguments = arguments;
        }

        @Override
        public void execute(ViolationCheckContextWithViolations context) {
            JavaApiBreaks breaks = context.getUserData(JUDGE);
            breaks.outcome().write(Path.of(arguments.get(OUTCOME)));
        }
    }
}
