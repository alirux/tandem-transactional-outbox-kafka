package com.codingful.tandem.benchmark;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

/**
 * Finds the highest sustainable offered rate (HLD-load-testing.md §3.1, §4 S1; LLD-benchmark §7).
 *
 * <p>The search is deliberately a <b>named algorithm rather than an invented one</b>: <b>exponential
 * search to bracket the ceiling, then bisection</b> — the standard method for locating the boundary of
 * a monotone predicate over an unbounded domain. The predicate is "rate {@code R} keeps the backlog
 * flat for a whole {@code sustainWindow}", assumed monotone in {@code R} (a system that sustains
 * {@code R} sustains anything below it); that assumption is what licenses bisection, and it is the one
 * modelling claim this class makes.
 *
 * <p><b>Convergence is the reason for the choice.</b> Doubling brackets a ceiling {@code k} octaves
 * above the seed in {@code k} holds, and each subsequent bisection halves the bracket — so from a
 * bracket of {@code [lo, 2·lo]} the relative uncertainty after {@code n} bisections is {@code 2^-n},
 * <i>stated in advance</i> and independent of how much budget remains. The search stops as soon as the
 * bracket is within {@code relativeTolerance}. An earlier hand-rolled variant (additive-increase /
 * multiplicative-decrease, no bracket) had no such property: it oscillated around the ceiling and
 * reported wherever the budget happened to stop it, which is how it came to report
 * {@code seed × 1.1²} — the same number on every host — as a measurement.
 *
 * <p><b>A result is only a maximum if the search bracketed it</b> ({@link RampResult#bracketed}).
 * Without an upper witness — a rate that demonstrably did <i>not</i> hold — the figure is a lower
 * bound set by the budget and the seed, not a property of the system under test, and callers must
 * report it as such rather than as a KPI. This is the check whose absence let the earlier defect look
 * like a measurement for as long as it did.
 *
 * <p>Two things that looked right on paper turned out not to converge on a real relay, both found by
 * actually running this against Docker containers rather than by inspection:
 *
 * <ul>
 *   <li><b>Hold the rate fixed while verifying it, don't keep raising it.</b> An earlier version kept
 *       increasing the rate on every flat observation <i>while</i> the sustain clock was running,
 *       which effectively tested whether the system could absorb a continuously-<i>increasing</i> rate
 *       for the whole window — a bar that is nearly impossible to clear.</li>
 *   <li><b>Compare against a fixed baseline with a tolerance band, not the immediately preceding
 *       sample with none.</b> The relay claims in batches (up to {@code batchSize} rows per poll
 *       cycle), so the pending count naturally saw-tooths by roughly that magnitude within a single
 *       poll cycle even at a genuinely sustainable steady state. A strict "no single sample may be
 *       higher than the last one" check trips on that normal wobble almost every window, so the
 *       sustain gate (also) never actually completed. {@code toleranceRows} — the claim-batch size is
 *       the natural unit for it — absorbs that wobble while still catching real, growing backlog.</li>
 * </ul>
 *
 * <p>Every completed hold is printed with a wall-clock timestamp and the current bracket, so a
 * resource sampler running alongside the benchmark can be read per rate step rather than averaged
 * across a whole run — the offered rate changes by design during a search, so a single mean over it
 * describes no particular operating point.
 */
public final class RampController {

    private static final DateTimeFormatter STEP_TIME = DateTimeFormatter.ofPattern("HH:mm:ss", Locale.ROOT);

    private final LagProbe lagProbe;
    private final Duration observationWindow;
    private final Duration sustainWindow;
    private final double relativeTolerance;
    private final long toleranceRows;

    /**
     * @param observationWindow how often the backlog is sampled within a hold
     * @param sustainWindow how long a rate must keep the backlog flat to count as sustained
     * @param relativeTolerance bracket width, relative to its upper end, at which the search stops
     *                          (0.05 = the answer is known to within 5%)
     * @param toleranceRows backlog growth absorbed as normal claim-batch wobble rather than treated as
     *                      a rate the system cannot keep up with
     */
    public RampController(LagProbe lagProbe, Duration observationWindow, Duration sustainWindow,
                           double relativeTolerance, long toleranceRows) {
        this.lagProbe = lagProbe;
        this.observationWindow = observationWindow;
        this.sustainWindow = sustainWindow;
        this.relativeTolerance = relativeTolerance;
        this.toleranceRows = toleranceRows;
    }

    /**
     * @param sustainedRatePerSecond the highest rate observed to hold — a maximum when
     *                               {@code bracketed}, otherwise only a lower bound
     * @param lowestFailingRatePerSecond the lowest rate observed <i>not</i> to hold, or
     *                                   {@link Double#POSITIVE_INFINITY} if none ever did
     */
    public record RampResult(double sustainedRatePerSecond, double lowestFailingRatePerSecond) {

        /**
         * Whether a rate that failed to hold was ever observed. Without that upper witness the search
         * located no ceiling, and {@link #sustainedRatePerSecond} is a lower bound set by the seed and
         * the budget rather than a property of the host.
         */
        public boolean bracketed() {
            return Double.isFinite(lowestFailingRatePerSecond);
        }

        /**
         * How tightly the ceiling is actually pinned, as a fraction of the bracket's upper end — the
         * *achieved* precision, which is the target tolerance only when the search converged before
         * the budget ran out. Reporting the target instead would overstate a search that was cut short.
         */
        public double relativeUncertainty() {
            return bracketed()
                    ? (lowestFailingRatePerSecond - sustainedRatePerSecond) / lowestFailingRatePerSecond
                    : Double.NaN;
        }
    }

    /**
     * The search interval: {@code lo} holds, {@code hi} does not, and {@code rate} is the candidate
     * being tested. {@code hi} is infinite until the first failing rate is seen — the honest encoding
     * of "the ceiling is known to be somewhere above {@code lo}, and nothing more".
     */
    record Search(double lo, double hi, double rate) {

        static Search from(double seedRate) {
            return new Search(0, Double.POSITIVE_INFINITY, seedRate);
        }

        boolean bracketed() {
            return Double.isFinite(hi);
        }

        boolean withinTolerance(double relativeTolerance) {
            return bracketed() && hi - lo <= relativeTolerance * hi;
        }
    }

    /** One completed hold, kept so the reported result can be audited against the run's own record. */
    record Observation(double rate, boolean held) {
    }

    /**
     * Checks the reported result against the trace that produced it, and throws if they disagree.
     *
     * <p>Both conditions are unreachable unless the search itself is wrong — after a rate fails it
     * becomes the bracket's upper end and every later candidate is below it, so a failing rate can
     * never sit at or under the reported maximum, and a rate that held can never sit above it. That
     * is exactly what makes the check worth running: it cannot fire on a slow host or a noisy one,
     * only on a defect. A previous version of this class reported the <i>latest</i> confirmed hold
     * rather than the highest, and so returned a figure the very same run had already beaten; the
     * output looked entirely ordinary, and nothing in the harness objected.
     */
    static void auditAgainstTrace(RampResult result, List<Observation> trace) {
        double reported = result.sustainedRatePerSecond();
        // The extremes, not the first offender: when several holds contradict the reported figure,
        // the highest one is what a reader needs in order to see how far off it is.
        double highestHeld = trace.stream().filter(Observation::held)
                .mapToDouble(Observation::rate).max().orElse(Double.NEGATIVE_INFINITY);
        double lowestFailed = trace.stream().filter(o -> !o.held())
                .mapToDouble(Observation::rate).min().orElse(Double.POSITIVE_INFINITY);

        if (highestHeld > reported) {
            throw new IllegalStateException(String.format(Locale.ROOT,
                    "search reported %.1f/s as the maximum, but %.1f/s held during the same run",
                    reported, highestHeld));
        }
        if (lowestFailed <= reported) {
            throw new IllegalStateException(String.format(Locale.ROOT,
                    "search reported %.1f/s as sustainable, but %.1f/s failed to hold during the same run",
                    reported, lowestFailed));
        }
    }

    /**
     * The whole search, as a pure function of one hold's outcome — extracted from the polling loop so
     * its convergence is pinned by tests that need neither a database nor a relay. The single
     * expression covers both phases: still unbracketed and holding, keep doubling; otherwise the
     * bracket has an end on both sides and the next candidate is its midpoint.
     */
    static Search afterHold(Search search, boolean held) {
        double lo = held ? search.rate() : search.lo();
        double hi = held ? search.hi() : search.rate();
        double next = Double.isFinite(hi) ? (lo + hi) / 2 : lo * 2;
        return new Search(lo, hi, Math.max(1, next));
    }

    /**
     * Starts {@code generator} at {@code initialRatePerSecond} and searches for up to {@code budget},
     * stopping early once the bracket is within {@code relativeTolerance}. Leaves the generator running
     * at the end — the caller stops it.
     */
    public RampResult findSustainableMax(LoadGenerator generator, double initialRatePerSecond, Duration budget) {
        Search search = Search.from(initialRatePerSecond);
        generator.start(search.rate());

        Instant deadline = Instant.now().plus(budget);
        Instant holdSince = Instant.now();
        long holdBaselinePending = lagProbe.overall().pending();
        List<Observation> trace = new ArrayList<>();

        while (Instant.now().isBefore(deadline) && !search.withinTolerance(relativeTolerance)) {
            sleep(observationWindow);
            long pending = lagProbe.overall().pending();

            boolean backlogGrew = pending > holdBaselinePending + toleranceRows;
            boolean holdComplete = Duration.between(holdSince, Instant.now()).compareTo(sustainWindow) >= 0;
            if (!backlogGrew && !holdComplete) {
                continue;   // still inside this hold — rate and baseline untouched
            }

            Search next = afterHold(search, !backlogGrew);
            printHold(!backlogGrew, search, next, pending);
            trace.add(new Observation(search.rate(), !backlogGrew));
            search = next;
            generator.setRate(search.rate());
            holdSince = Instant.now();
            holdBaselinePending = pending;
        }
        RampResult result = new RampResult(search.lo(), search.hi());
        auditAgainstTrace(result, trace);
        return result;
    }

    private static void printHold(boolean held, Search search, Search next, long pending) {
        System.out.printf(Locale.ROOT, "  ramp %s  %s rate:%.1f/s bracket:[%.1f, %s] next:%.1f/s pending:%d%n",
                LocalTime.now().format(STEP_TIME), held ? "held " : "grew ", search.rate(),
                next.lo(), next.bracketed() ? String.format(Locale.ROOT, "%.1f", next.hi()) : "unbounded",
                next.rate(), pending);
    }

    private static void sleep(Duration d) {
        try {
            Thread.sleep(d.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
