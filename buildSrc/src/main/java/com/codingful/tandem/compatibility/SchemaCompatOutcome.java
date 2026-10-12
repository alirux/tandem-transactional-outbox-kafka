package com.codingful.tandem.compatibility;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * What {@code schemaCompatTest} concludes from the scenario's failed tests and its declared breaks
 * (LLD-api-stability §5.2).
 *
 * <p>The scenario runs its setup (the container, then phases 1 and 2) before its first test method, and each
 * step of phases 3 and 4 is one of its test methods. A failed test method is therefore a step of the
 * comparison; a failure JUnit reports against the class (a setup or teardown that threw, a class that does
 * not load) is not. A {@code schema-compatibility} declaration accepts every failed step against its
 * baseline, never a failure outside a step, and is stale when no step failed (§3, rules 2 and 4).
 */
public final class SchemaCompatOutcome {

    /** The gate kind of the declared breaks {@code schemaCompatTest} reads. */
    public static final String GATE = "schema-compatibility";

    /** The one item the gate accepts: the test reports no item with a stable identity (§3, rule 3). */
    static final String ANY_STEP = "*";

    /**
     * A test that failed, as Gradle reports it.
     *
     * @param className the test class
     * @param name      the test's name: a test method's carries its parameter list, {@code step()}; a failure
     *                  of the class itself gets a name of Gradle's, such as {@code initializationError}
     */
    public record FailedTest(String className, String name) implements Serializable {

        boolean step() {
            return name.endsWith(")");
        }

        @Override
        public String toString() {
            return className + " > " + name;
        }
    }

    /**
     * @param accepted one line per declaration that accepted the failed steps, with its release-notes line
     * @param problems one line per failure the declarations do not accept, or per stale declaration; empty when
     *                 the gate passes
     */
    public record Verdict(List<String> accepted, List<String> problems) {
    }

    private SchemaCompatOutcome() {
    }

    /**
     * @param module       the module that runs the scenario, as declarations name it
     * @param failures     every test of the run that failed
     * @param declarations the module's {@code schema-compatibility} entries of the declared breaks
     */
    public static Verdict judge(String module, List<FailedTest> failures, List<DeclaredBreaks.Entry> declarations) {
        List<FailedTest> steps = failures.stream().filter(FailedTest::step).toList();
        List<String> problems = new ArrayList<>();
        failures.stream().filter(failure -> !failure.step()).forEach(failure -> problems.add(failure
                + " failed outside the steps of phases 3 and 4 (the container, phase 1 or 2, or the scenario itself): "
                + "it is not a comparison, so no declaration accepts it"));
        // A failure outside the steps may have kept them from running, so it leaves a declaration's staleness
        // undecided: only the failure is reported.
        boolean comparedEveryStep = problems.isEmpty();
        DeclaredBreaks.Matching matching = DeclaredBreaks.match(steps.isEmpty() ? List.of() : List.of(ANY_STEP),
                declarations);
        List<String> accepted = new ArrayList<>();
        if (!matching.undeclared().isEmpty()) {
            steps.forEach(step -> problems.add(step + " failed: the current changelog and the baseline release are "
                    + "not compatible"));
            problems.add("if intended, in a major version, declare it as '" + GATE + " | " + module + " | " + ANY_STEP
                    + " | <what operators must do before upgrading>'");
        }
        matching.accepted().values().forEach(entry -> accepted.add(entry + ", release notes: " + entry.releaseNote()
                + ", failed steps: " + steps.stream().map(FailedTest::name).collect(Collectors.joining(", "))));
        if (comparedEveryStep) {
            matching.stale().forEach(entry -> problems.add("Declared break matches no failed step, remove it: "
                    + entry));
        }
        return new Verdict(accepted, problems);
    }
}
