package com.codingful.tandem.benchmark;

import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * A consumer co-located with the harness that does double duty (HLD-load-testing.md §2.2): the relay
 * records nothing, so this is the one place COMMIT→ack latency and per-aggregate correctness
 * (ordering, duplicates) are observed. Owns its {@link EventReceiver} and holds no client type of its
 * own, so the verdict is decided by the same code whichever broker the run used; the caller
 * {@link #start()}s and {@link #stop()}s/closes it.
 *
 * <p>Correctness is tracked two ways, and which one a scenario picks is a memory decision. By default
 * every {@code (aggregateId, seq)} is kept in {@link #receivedKeys()} for an exact set-diff against the
 * write side. A scenario running for hours instead of minutes cannot afford that — see
 * {@link SequenceLedger} — and constructs this consumer with key tracking off, leaving the ledger,
 * whose memory is bounded by the aggregate cardinality, as the sole record. Ordering violations are
 * counted by the ledger in both modes.
 */
public final class CorrelationConsumer implements AutoCloseable {

    private final EventReceiver receiver;
    private final LatencyRecorder latencyRecorder;
    private final CommitTimestamps commitTimestamps;   // nullable — PROXY-only mode

    private final boolean trackReceivedKeys;
    private final SequenceLedger ledger = new SequenceLedger();
    private final Set<String> receivedKeys = ConcurrentHashMap.newKeySet();
    private final AtomicLong receivedCount = new AtomicLong();
    private final AtomicLong keyDuplicates = new AtomicLong();

    private volatile boolean running;
    private Thread pollThread;

    public CorrelationConsumer(EventReceiver receiver, LatencyRecorder latencyRecorder,
                                CommitTimestamps commitTimestamps) {
        this(receiver, latencyRecorder, commitTimestamps, true);
    }

    /**
     * @param trackReceivedKeys whether to keep every {@code (aggregateId, seq)} for an exact set-diff.
     *                          Off for a run long enough that the set itself would dominate the
     *                          harness's memory ({@link SequenceLedger}); {@link #receivedKeys()} is
     *                          then empty and correctness must be read from {@link #ledger()}.
     */
    public CorrelationConsumer(EventReceiver receiver, LatencyRecorder latencyRecorder,
                                CommitTimestamps commitTimestamps, boolean trackReceivedKeys) {
        this.receiver = receiver;
        this.latencyRecorder = latencyRecorder;
        this.commitTimestamps = commitTimestamps;
        this.trackReceivedKeys = trackReceivedKeys;
    }

    public void start() {
        running = true;
        pollThread = new Thread(this::pollLoop, "bench-correlation-consumer");
        pollThread.setDaemon(true);
        pollThread.start();
    }

    public void stop() {
        running = false;
        receiver.wakeup();
        if (pollThread != null) {
            try {
                pollThread.join(TimeUnit.SECONDS.toMillis(10));
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
    }

    @Override
    public void close() {
        stop();
        receiver.close();
    }

    public long orderingViolations() {
        return ledger.orderingViolations();
    }

    public long duplicateCount() {
        return trackReceivedKeys ? keyDuplicates.get() : ledger.duplicates();
    }

    /** Sample of up to a few offending {@code (aggregateId, seq)} pairs, for diagnostics. */
    public List<String> violationSamples() {
        return ledger.violationSamples();
    }

    /** Every event received, duplicates included — the per-window delivered-throughput counter. */
    public long receivedCount() {
        return receivedCount.get();
    }

    /** The bounded per-aggregate record of what arrived; the only correctness record when key tracking is off. */
    public SequenceLedger ledger() {
        return ledger;
    }

    /** Every {@code (aggregateId, seq)} received at least once, as {@code aggregateId + '#' + seq}. */
    public Set<String> receivedKeys() {
        return receivedKeys;
    }

    private void pollLoop() {
        while (running) {
            for (ReceivedEvent event : receiver.poll(Duration.ofMillis(200))) {
                onEvent(event);
            }
        }
    }

    private void onEvent(ReceivedEvent event) {
        if (event.aggregateId() == null || event.seq() < 0) {
            return;   // malformed/unrelated message: nothing to correlate
        }
        receivedCount.incrementAndGet();
        if (trackReceivedKeys && !receivedKeys.add(event.aggregateId() + '#' + event.seq())) {
            keyDuplicates.incrementAndGet();
        }
        // A redelivered duplicate has seq == the aggregate's watermark — expected under at-least-once
        // delivery (S5) and explicitly not fatal; only a strictly *decreasing* seq is a genuine
        // ordering violation. Both are the ledger's call.
        ledger.record(event.aggregateId(), event.seq());
        recordLatency(event);
    }

    private void recordLatency(ReceivedEvent event) {
        long t0 = -1;
        if (commitTimestamps != null) {
            t0 = commitTimestamps.takeCommitNanos(event.aggregateId(), event.seq());
        }
        if (t0 < 0) {
            t0 = event.t0Nanos();
        }
        if (t0 < 0) {
            return;   // header missing/unparseable — skip rather than record a bogus latency
        }
        latencyRecorder.record(Duration.ofNanos(Math.max(0, event.receivedNanos() - t0)));
    }
}
