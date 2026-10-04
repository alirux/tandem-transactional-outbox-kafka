package com.codingful.tandem.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

/**
 * Picks which synthetic aggregate the next generated event belongs to (LLD-benchmark §4.2). The full
 * id space ({@link #universe()}) is known upfront so {@link LoadGenerator} can seed every
 * {@code bench_aggregate} row before driving load.
 *
 * <p>{@code namespace} scopes the generated ids (e.g. to a scenario id like {@code "S1"}) so that two
 * selectors never collide on the same aggregate id when they share one {@code tandem_outbox} table —
 * as {@link com.codingful.tandem.benchmark.SmokeLoadTest} does across scenarios in one run. Without
 * this, S6's poisoned aggregate (always {@code universe().get(0)}) would deterministically collide
 * with another scenario's own use of the same default id space, permanently blocking it too (the
 * poison gate is structural and has no expiry — LLD-jdbc §3.4.2).
 */
public interface AggregateSelector {

    /** The next aggregate id to write to. */
    String nextAggregateId();

    /** Every aggregate id this selector can produce — the seed set for {@code bench_aggregate}. */
    List<String> universe();

    /** Spreads load evenly across {@code cardinality} aggregates, namespaced under {@code namespace} (S1, S2, S4, S5). */
    static AggregateSelector uniform(String namespace, int cardinality) {
        List<String> ids = idRange(namespace, cardinality);
        return new AggregateSelector() {
            @Override
            public String nextAggregateId() {
                return ids.get(ThreadLocalRandom.current().nextInt(ids.size()));
            }

            @Override
            public List<String> universe() {
                return ids;
            }
        };
    }

    /**
     * A single hot aggregate ({@link #universe()}{@code .get(0)}) receives {@code hotFraction} of the
     * traffic; the rest spreads uniformly over the remaining {@code cardinality - 1} aggregates,
     * namespaced under {@code namespace} (S3, S6's poison target).
     */
    static AggregateSelector skewed(String namespace, int cardinality, double hotFraction) {
        if (cardinality < 2) {
            throw new IllegalArgumentException("cardinality must be at least 2 for a skewed distribution");
        }
        if (hotFraction <= 0 || hotFraction >= 1) {
            throw new IllegalArgumentException("hotFraction must be in (0, 1)");
        }
        List<String> ids = idRange(namespace, cardinality);
        return new AggregateSelector() {
            @Override
            public String nextAggregateId() {
                ThreadLocalRandom random = ThreadLocalRandom.current();
                if (random.nextDouble() < hotFraction) {
                    return ids.get(0);
                }
                return ids.get(1 + random.nextInt(ids.size() - 1));
            }

            @Override
            public List<String> universe() {
                return ids;
            }
        };
    }

    /**
     * A population of short-lived aggregates: {@code concurrentAggregates} are active at any moment,
     * each receives about {@code eventsPerAggregate} events (jittered by {@code quotaJitter}) spread
     * over its life, and then retires for good and is replaced by an id never used before (S11).
     *
     * <p>This is the distribution a real write side produces, and the difference from
     * {@link #uniform} is not cosmetic: with a fixed cardinality every aggregate's chain grows without
     * bound, so in a backlog of {@code N} pending rows the head-of-chain gate leaves only
     * {@code cardinality} of them claimable and the claim's cost per delivered row rises with
     * {@code N}. Here chain length is bounded by the quota instead, so the share of claimable rows
     * stays near {@code 1 / eventsPerAggregate} whatever the backlog. A measurement of backlog
     * recovery is dominated by which of the two it ran under.
     *
     * <p>The id space is finite because {@link LoadGenerator} seeds every {@code bench_aggregate} row
     * before driving load, so {@link #universe()} must be known upfront. Size it with
     * {@link #universeSizeFor}: running out throws rather than recycling an id, because recycling
     * would quietly restore the unbounded chains this selector exists to avoid.
     *
     * @param namespace            scopes the generated ids, as for the other selectors
     * @param eventsPerAggregate   mean number of events an aggregate receives before retiring
     * @param quotaJitter          fraction the quota varies by, uniformly in
     *                             {@code [1 - jitter, 1 + jitter]}; {@code 0} for a fixed quota
     * @param concurrentAggregates how many aggregates are active at once, which bounds how many
     *                             chains the relay can drain in parallel: keep it well above the
     *                             worker count or the workers starve on the gate rather than on work
     * @param universeSize         size of the precomputed id space
     * @throws IllegalArgumentException if any argument is out of range, or {@code universeSize} is
     *                                  smaller than {@code concurrentAggregates}
     */
    static AggregateSelector lifecycle(String namespace, int eventsPerAggregate, double quotaJitter,
            int concurrentAggregates, int universeSize) {
        if (eventsPerAggregate < 1) {
            throw new IllegalArgumentException("eventsPerAggregate must be at least 1");
        }
        if (quotaJitter < 0 || quotaJitter >= 1) {
            throw new IllegalArgumentException("quotaJitter must be in [0, 1)");
        }
        if (concurrentAggregates < 1) {
            throw new IllegalArgumentException("concurrentAggregates must be at least 1");
        }
        if (universeSize < concurrentAggregates) {
            throw new IllegalArgumentException(
                    "universeSize (" + universeSize + ") must be at least concurrentAggregates ("
                            + concurrentAggregates + ")");
        }
        List<String> ids = idRange(namespace, universeSize);
        return new AggregateSelector() {

            private final int[] slotId = new int[concurrentAggregates];
            private final int[] slotRemaining = new int[concurrentAggregates];
            private int nextUnused;

            {
                for (int slot = 0; slot < concurrentAggregates; slot++) {
                    slotId[slot] = nextUnused++;
                    slotRemaining[slot] = quota();
                }
            }

            private int quota() {
                if (quotaJitter == 0) {
                    return eventsPerAggregate;
                }
                double factor = 1 + ThreadLocalRandom.current().nextDouble(-quotaJitter, quotaJitter);
                return Math.max(1, (int) Math.round(eventsPerAggregate * factor));
            }

            @Override
            public synchronized String nextAggregateId() {
                int slot = ThreadLocalRandom.current().nextInt(concurrentAggregates);
                String id = ids.get(slotId[slot]);
                if (--slotRemaining[slot] <= 0) {
                    if (nextUnused >= ids.size()) {
                        throw new IllegalStateException(
                                "aggregate id space exhausted after " + ids.size()
                                        + " ids; size it with AggregateSelector.universeSizeFor(...)");
                    }
                    slotId[slot] = nextUnused++;
                    slotRemaining[slot] = quota();
                }
                return id;
            }

            @Override
            public List<String> universe() {
                return ids;
            }
        };
    }

    /**
     * How large an id space {@link #lifecycle} needs to serve {@code expectedEvents}, taking the
     * shortest quota the jitter allows and leaving one full active set of headroom.
     *
     * @param expectedEvents       upper bound on how many events the run will generate
     * @param eventsPerAggregate   the same mean quota passed to {@link #lifecycle}
     * @param quotaJitter          the same jitter passed to {@link #lifecycle}
     * @param concurrentAggregates the same active-set size passed to {@link #lifecycle}
     */
    static int universeSizeFor(long expectedEvents, int eventsPerAggregate, double quotaJitter,
            int concurrentAggregates) {
        double shortestQuota = Math.max(1, eventsPerAggregate * (1 - quotaJitter));
        long needed = (long) Math.ceil(expectedEvents / shortestQuota);
        return (int) Math.min(Integer.MAX_VALUE, needed + concurrentAggregates);
    }

    private static List<String> idRange(String namespace, int cardinality) {
        List<String> ids = new ArrayList<>(cardinality);
        for (int i = 0; i < cardinality; i++) {
            ids.add(namespace + "-bench-agg-" + i);
        }
        return List.copyOf(ids);
    }
}
