package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.CloudEventsHeaders;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.header.Header;

/**
 * A Kafka consumer co-located with the harness that does double duty (HLD-load-testing.md §2.2): the
 * relay records nothing, so this is the one place COMMIT→ack latency and per-aggregate correctness
 * (ordering, duplicates) are observed. Owns its {@link KafkaConsumer}; the caller {@link #start()}s
 * and {@link #stop()}s/closes it.
 *
 * <p>Correctness is tracked two ways, and which one a scenario picks is a memory decision. By default
 * every {@code (aggregateId, seq)} is kept in {@link #receivedKeys()} for an exact set-diff against the
 * write side. A scenario running for hours instead of minutes cannot afford that — see
 * {@link SequenceLedger} — and constructs this consumer with key tracking off, leaving the ledger,
 * whose memory is bounded by the aggregate cardinality, as the sole record. Ordering violations are
 * counted by the ledger in both modes.
 */
public final class CorrelationConsumer implements AutoCloseable {

    private final KafkaConsumer<String, byte[]> consumer;
    private final LatencyRecorder latencyRecorder;
    private final CommitTimestamps commitTimestamps;   // nullable — PROXY-only mode

    private final boolean trackReceivedKeys;
    private final SequenceLedger ledger = new SequenceLedger();
    private final Set<String> receivedKeys = ConcurrentHashMap.newKeySet();
    private final AtomicLong receivedCount = new AtomicLong();
    private final AtomicLong keyDuplicates = new AtomicLong();

    private volatile boolean running;
    private Thread pollThread;

    public CorrelationConsumer(KafkaConsumer<String, byte[]> consumer, LatencyRecorder latencyRecorder,
                                CommitTimestamps commitTimestamps) {
        this(consumer, latencyRecorder, commitTimestamps, true);
    }

    /**
     * @param trackReceivedKeys whether to keep every {@code (aggregateId, seq)} for an exact set-diff.
     *                          Off for a run long enough that the set itself would dominate the
     *                          harness's memory ({@link SequenceLedger}); {@link #receivedKeys()} is
     *                          then empty and correctness must be read from {@link #ledger()}.
     */
    public CorrelationConsumer(KafkaConsumer<String, byte[]> consumer, LatencyRecorder latencyRecorder,
                                CommitTimestamps commitTimestamps, boolean trackReceivedKeys) {
        this.consumer = consumer;
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
        consumer.wakeup();
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
        consumer.close();
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
            ConsumerRecords<String, byte[]> records;
            try {
                records = consumer.poll(Duration.ofMillis(200));
            } catch (WakeupException e) {
                continue;   // checked again against `running` at the top of the loop
            }
            long receiveNanos = System.nanoTime();
            for (ConsumerRecord<String, byte[]> record : records) {
                onRecord(record, receiveNanos);
            }
        }
    }

    private void onRecord(ConsumerRecord<String, byte[]> record, long receiveNanos) {
        String aggregateId = record.key();
        long seq = headerLong(record, CloudEventsHeaders.CE_SEQ);
        if (seq < 0) {
            return;   // malformed/unrelated record — nothing to correlate
        }
        receivedCount.incrementAndGet();
        if (trackReceivedKeys && !receivedKeys.add(aggregateId + '#' + seq)) {
            keyDuplicates.incrementAndGet();
        }
        // A redelivered duplicate has seq == the aggregate's watermark — expected under at-least-once
        // delivery (S5) and explicitly not fatal; only a strictly *decreasing* seq is a genuine
        // ordering violation. Both are the ledger's call.
        ledger.record(aggregateId, seq);
        recordLatency(aggregateId, seq, record, receiveNanos);
    }

    private void recordLatency(String aggregateId, long seq, ConsumerRecord<String, byte[]> record, long receiveNanos) {
        long t0 = -1;
        if (commitTimestamps != null) {
            t0 = commitTimestamps.takeCommitNanos(aggregateId, seq);
        }
        if (t0 < 0) {
            t0 = headerLong(record, BenchmarkHeaders.T0_NANOS);
        }
        if (t0 < 0) {
            return;   // header missing/unparseable — skip rather than record a bogus latency
        }
        latencyRecorder.record(Duration.ofNanos(Math.max(0, receiveNanos - t0)));
    }

    private static long headerLong(ConsumerRecord<String, byte[]> record, String headerName) {
        Header header = record.headers().lastHeader(headerName);
        if (header == null) {
            return -1;
        }
        try {
            return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8).trim());
        } catch (NumberFormatException e) {
            return -1;
        }
    }
}
