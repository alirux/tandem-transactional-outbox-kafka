package com.codingful.tandem.benchmark.scenario;

import com.codingful.tandem.benchmark.BenchmarkConfig;
import com.codingful.tandem.benchmark.CorrelationConsumer;
import com.codingful.tandem.benchmark.LagProbe;
import com.codingful.tandem.benchmark.LoadGenerator;
import com.codingful.tandem.benchmark.RampController;
import com.codingful.tandem.benchmark.RelayInstance;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.BooleanSupplier;

/** Correctness assertions and small timing helpers every scenario shares (HLD-load-testing.md §4). */
final class ScenarioSupport {

    private ScenarioSupport() {
    }

    record CorrectnessReport(long orderingViolations, Set<String> missingKeys, long duplicateCount) {
        boolean passed() {
            return orderingViolations == 0 && missingKeys.isEmpty();
        }
    }

    /**
     * How a set of {@code LEASE} instances have divided the buckets between them at one instant
     * (S9): how many buckets are owned in total, whether any two instances claim the same one, and the
     * per-instance shares. {@code covered} counts distinct buckets, so an overlap cannot inflate it.
     */
    record Coverage(int covered, boolean disjoint, List<Integer> perInstance) {

        /** Every bucket owned, by exactly one instance — what must keep being true, renewal after renewal. */
        boolean complete(int bucketCount) {
            return disjoint && covered == bucketCount;
        }
    }

    /** Samples {@code instances}' current bucket ownership (S9 checks it once per window). */
    static Coverage coverage(List<RelayInstance> instances) {
        Set<Integer> distinct = new HashSet<>();
        List<Integer> perInstance = new ArrayList<>();
        boolean disjoint = true;
        for (RelayInstance instance : instances) {
            Set<Integer> owned = instance.bucketSource().ownedBuckets();
            perInstance.add(owned.size());
            for (Integer bucket : owned) {
                disjoint &= distinct.add(bucket);
            }
        }
        return new Coverage(distinct.size(), disjoint, List.copyOf(perInstance));
    }

    /**
     * Where a scenario's offered rate came from. A scenario that holds "half of the ceiling" is only
     * doing that if a ceiling was actually found: when the ramp never observed a rate the host could
     * not hold, its figure is a lower bound set by the seed and the budget, and half of it is half of
     * an arbitrary number. The two cases read identically in a result line unless they are named, and
     * the run is archived either way.
     */
    enum RateBasis {

        /** The rate was given on the command line; no search ran. */
        EXPLICIT,
        /** The ramp bracketed a ceiling — it saw a rate that held and one that did not. */
        BRACKETED_CEILING,
        /** The ramp never found a failing rate, so its figure is a lower bound, not a ceiling. */
        LOWER_BOUND;

        static RateBasis of(RampController.RampResult result) {
            return result.bracketed() ? BRACKETED_CEILING : LOWER_BOUND;
        }

        /** How the offered rate should be described in a result line, given the fraction held of it. */
        String describe(double fraction) {
            int percent = (int) Math.round(fraction * 100);
            return switch (this) {
                case EXPLICIT -> "rate given explicitly";
                case BRACKETED_CEILING -> percent + "% of a bracketed ceiling";
                case LOWER_BOUND -> percent + "% of a LOWER BOUND — the ramp never found a rate this host "
                        + "could not hold, so this is not " + percent + "% of capacity";
            };
        }
    }

    /** Grace for the co-located consumer to receive what the outbox has already published (see {@link #verify}). */
    private static final Duration CONSUMER_CATCH_UP = Duration.ofSeconds(15);

    /** Zero ordering violations, and every inserted {@code (aggregateId, seq)} eventually arrived (duplicates allowed). */
    static CorrectnessReport verify(LoadGenerator generator, CorrelationConsumer consumer) throws InterruptedException {
        // The caller waits for the OUTBOX to drain (all rows published), but the correlation consumer polls
        // Kafka on its own thread and can lag that drain by a poll cycle — so diffing immediately would read a
        // just-published, not-yet-received event as loss (a flaky missing key under CI timing). Give the
        // consumer a bounded grace to catch up to what was published before diffing. This never hides real
        // loss: a genuinely dropped event never arrives, the wait times out, and verify still reports it.
        Set<String> inserted = new HashSet<>(generator.insertedKeys());
        Instant deadline = Instant.now().plus(CONSUMER_CATCH_UP);
        while (!consumer.receivedKeys().containsAll(inserted) && Instant.now().isBefore(deadline)) {
            Thread.sleep(200);
        }
        Set<String> missing = new HashSet<>(inserted);
        missing.removeAll(consumer.receivedKeys());
        return new CorrectnessReport(consumer.orderingViolations(), missing, consumer.duplicateCount());
    }

    /**
     * Polls the lag probe until {@code namespace}'s own PENDING/IN_FLIGHT backlog drains to zero, or
     * throws on timeout. Deliberately excludes {@code FAILED} rows ({@link LagProbe#inProgressForNamespace}):
     * a permanently failed row is terminal and will never drain on its own, so it must not stall this
     * wait — it surfaces instead as a missing key in {@link #verify}. Scoped to {@code namespace}
     * (a scenario's own aggregate-id prefix, from {@link com.codingful.tandem.benchmark.AggregateSelector})
     * so another scenario's permanently-stuck poisoned backlog (S6, sharing one {@code tandem_outbox}
     * table across a test class) can never make this wait hang forever.
     */
    static void waitForDrain(LagProbe lagProbe, String namespace, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (lagProbe.inProgressForNamespace(namespace).pending() == 0) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("backlog did not drain within " + timeout);
    }

    /** Like {@link #waitForDrain}, but ignoring one aggregate's own rows within {@code namespace} (S6: the poisoned one never drains). */
    static void waitForOthersToDrain(LagProbe lagProbe, String namespace, String excludedAggregateId, Duration timeout) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (lagProbe.pendingExcludingAggregate(namespace, excludedAggregateId) == 0) {
                return;
            }
            Thread.sleep(500);
        }
        throw new IllegalStateException("other aggregates did not drain within " + timeout);
    }

    /**
     * How often the ramp samples the backlog: a few seconds, floored at 1s and never longer than a
     * tenth of {@code duration} (so a smoke run still samples several times). Capped rather than scaled
     * because this is a sampling cadence, not a measurement window — a 10-minute run gains nothing from
     * checking the backlog once a minute, and {@link #sustainWindowFor} needs several samples inside it.
     */
    static Duration observationWindowFor(BenchmarkConfig cfg) {
        Duration tenth = cfg.duration().dividedBy(10);
        Duration capped = minDuration(tenth, Duration.ofSeconds(5));
        return capped.compareTo(Duration.ofSeconds(1)) < 0 ? Duration.ofSeconds(1) : capped;
    }

    /**
     * How long one candidate rate must hold flat to count as sustained: a <b>quarter</b> of
     * {@code duration}, so that a search budget of a small multiple of {@code duration} affords several
     * ramp steps. It must stay a fraction of the budget rather than equal it: with
     * {@code sustainWindow == budget / 2} the search can only ever confirm two steps, and S1 then
     * reports {@code seed × (1 + step)²} — the same number on every run and on every host, set by that
     * arithmetic rather than by the system under test. Floored at 2× the observation window so each
     * hold is judged on more than a single sample.
     */
    static Duration sustainWindowFor(BenchmarkConfig cfg) {
        return maxDuration(cfg.duration().dividedBy(4), observationWindowFor(cfg).multipliedBy(2));
    }

    static Duration maxDuration(Duration a, Duration b) {
        return a.compareTo(b) >= 0 ? a : b;
    }

    static Duration minDuration(Duration a, Duration b) {
        return a.compareTo(b) <= 0 ? a : b;
    }

    /**
     * Polls {@code condition} until it's true, or throws on timeout — the same pattern
     * {@code EmbeddedLeaseIT}/{@code BucketLeaseManagerIT} use in {@code tandem-jdbc} (S8: awaiting a
     * bucket-ownership partition to stabilize, before and after a simulated instance kill).
     */
    static void awaitUpTo(Duration timeout, BooleanSupplier condition) throws InterruptedException {
        Instant deadline = Instant.now().plus(timeout);
        while (Instant.now().isBefore(deadline)) {
            if (condition.getAsBoolean()) {
                return;
            }
            Thread.sleep(200);
        }
        throw new IllegalStateException("condition not met within " + timeout);
    }
}
