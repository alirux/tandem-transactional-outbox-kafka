package com.codingful.tandem.benchmark;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Per-aggregate delivery bookkeeping in memory bounded by the <b>aggregate cardinality</b>, not by the
 * number of events (LLD-benchmark §8): one high-watermark per aggregate, whatever the run's length.
 *
 * <p>That bound is what makes an hours-long run possible at all. The exact reconciliation the shorter
 * scenarios use — a {@code Set} of every {@code (aggregateId, seq)} on both the write and the read side
 * — costs a couple of hundred bytes per event on each side, so a run measured in hours spends its
 * memory on the harness rather than on the system under test, and the resulting GC pressure reads as a
 * throughput decline over time: precisely the signal an endurance run exists to detect.
 *
 * <p>Exactness is not lost, only re-derived. The generator's {@code seq} per aggregate is the
 * {@code bench_aggregate} version counter, bumped inside the same transaction as the insert, so it
 * runs 1, 2, 3, … with no holes (a rolled-back attempt takes its version increment back with it). The
 * sequence a reader should see for an aggregate is therefore fully described by its last value, and
 * any hole in the delivered stream is a lost event.
 */
public final class SequenceLedger {

    /**
     * How many offending {@code (aggregateId, seq)} pairs are kept for diagnostics. Capped because a
     * violation storm would otherwise grow an unbounded copy-on-write list, whose per-append copy makes
     * the harness quadratic exactly when the system under test is already misbehaving.
     */
    private static final int MAX_VIOLATION_SAMPLES = 20;

    private final List<String> violationSamples = new CopyOnWriteArrayList<>();
    private final Map<String, Long> highWatermarks = new ConcurrentHashMap<>();
    private final AtomicLong orderingViolations = new AtomicLong();
    private final AtomicLong duplicates = new AtomicLong();
    private final AtomicLong gaps = new AtomicLong();

    /** Records one delivered {@code (aggregateId, seq)}, updating the ordering/duplicate/gap counters. */
    public void record(String aggregateId, long seq) {
        highWatermarks.compute(aggregateId, (id, previous) -> {
            if (previous == null) {
                // Sequences start at 1, so an aggregate first seen at 5 has already lost four events —
                // counted here rather than nowhere: with a plain merge, a first delivery installs its
                // watermark without ever comparing it to anything, and every event before it becomes
                // invisible to the reconciliation.
                gaps.addAndGet(seq - 1);
                return seq;
            }
            long delivered = seq;
            if (delivered < previous) {
                orderingViolations.incrementAndGet();
                if (violationSamples.size() < MAX_VIOLATION_SAMPLES) {
                    violationSamples.add(aggregateId + ": seq " + delivered + " arrived after " + previous);
                }
                return previous;   // keep the high-watermark; don't regress on a violation
            }
            if (delivered == previous) {
                duplicates.incrementAndGet();   // at-least-once redelivery, not a violation
                return previous;
            }
            // Ordering is guaranteed per aggregate, so a seq that skips ahead has skipped events that
            // will never arrive later — a hole here is loss, counted as it happens rather than by
            // diffing two sets at the end.
            gaps.addAndGet(delivered - previous - 1);
            return delivered;
        });
    }

    /** Sample of up to {@value #MAX_VIOLATION_SAMPLES} offending {@code (aggregateId, seq)} pairs, for diagnostics. */
    public List<String> violationSamples() {
        return List.copyOf(violationSamples);
    }

    public long orderingViolations() {
        return orderingViolations.get();
    }

    public long duplicates() {
        return duplicates.get();
    }

    /** Events skipped mid-stream: a delivered {@code seq} that jumped over one or more of its predecessors. */
    public long gaps() {
        return gaps.get();
    }

    /**
     * Events written but never delivered, given each aggregate's last committed {@code seq}
     * ({@link LoadGenerator#lastInsertedSeqs()}): the {@link #gaps()} counted mid-stream, plus the tail
     * each aggregate is still missing. An aggregate written to but never delivered at all counts in
     * full — its whole sequence is the tail.
     *
     * @param lastInsertedSeqs last committed {@code seq} per aggregate id
     * @return the number of committed events that never arrived
     */
    public long lostEvents(Map<String, Long> lastInsertedSeqs) {
        long tail = 0;
        for (Map.Entry<String, Long> inserted : lastInsertedSeqs.entrySet()) {
            long delivered = highWatermarks.getOrDefault(inserted.getKey(), 0L);
            tail += Math.max(0, inserted.getValue() - delivered);
        }
        return gaps.get() + tail;
    }

    /**
     * True once every aggregate's delivered watermark has caught up with what was committed for it.
     *
     * @param lastInsertedSeqs last committed {@code seq} per aggregate id
     * @return whether delivery has caught up with the write side
     */
    public boolean caughtUpWith(Map<String, Long> lastInsertedSeqs) {
        for (Map.Entry<String, Long> inserted : lastInsertedSeqs.entrySet()) {
            if (highWatermarks.getOrDefault(inserted.getKey(), 0L) < inserted.getValue()) {
                return false;
            }
        }
        return true;
    }

    /** How many distinct aggregates have received at least one event. */
    public int aggregatesSeen() {
        return highWatermarks.size();
    }
}
