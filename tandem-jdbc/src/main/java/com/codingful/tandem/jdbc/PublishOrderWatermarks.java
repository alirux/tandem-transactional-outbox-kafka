package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.SeqSource;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * The last ordering value one worker published per aggregate, and the verdict on the next one — the
 * in-process half of the write-side ordering detector (HLD §8).
 *
 * <p><b>Why in process, and why at publish time.</b> When two writers to one aggregate are not
 * serialised, the row inserted first can commit second, so the relay publishes the later row first —
 * yet the table is left perfectly ordered. No later scan of {@code tandem_outbox} can find the
 * violation, because by then there is nothing wrong with the data; only the relay, as it publishes,
 * witnesses the sequence that actually went out.
 *
 * <p><b>Which value it holds depends on the row</b> (HLD-managed-seq §6.1). An application-assigned
 * {@code seq} is an ordering the application <i>declared</i>, independent of the order rows were
 * inserted, so it is the stronger thing to check a published order against. Every other row — one
 * Tandem numbered, one with no number, one whose provenance this build predates — is checked on
 * {@code id}, which is the only ordering such a row has. A {@link Key} change for an aggregate resets
 * its entry rather than comparing a {@code seq} against an {@code id}, so the row on which an
 * aggregate switches modes is not judged at all.
 *
 * <p><b>Bounded, and deliberately lossy.</b> One entry per aggregate for the life of the process
 * would leak on a high-cardinality aggregate space, so this is an LRU capped at {@link #CAPACITY}
 * entries per worker — a fixed constant, not a knob. Evicting an aggregate degrades detection exactly
 * as a relay restart already does, and the design accepts that: this is an alerting signal for a
 * precondition that should never be violated, not an audit log.
 *
 * <p><b>Not thread-safe, by contract.</b> One instance belongs to one {@link RelayWorker} and is
 * touched only from that worker's own loop, never from the dispatcher's completion thread — which is
 * also what lets the regression path issue a database lookup at all.
 */
final class PublishOrderWatermarks {

    /**
     * Entries kept per worker. Orders of magnitude above any realistic {@code batchSize}, and a few MB
     * of heap even across a wide worker pool.
     */
    static final int CAPACITY = 4096;

    /** Which ordering a stored watermark holds, so two of them are never compared with each other. */
    enum Key {
        /** The number the application supplied — the order it declared. */
        SEQ,
        /** The row's identity, for a row that declared no order of its own. */
        ID
    }

    /** What the next value for an aggregate says about the order it was published in. */
    enum Verdict {
        /** Ahead of everything seen for this aggregate — the normal case, a first sighting, or a key change. */
        ADVANCED,
        /** Exactly the last one published: an at-least-once redelivery, not an ordering problem. */
        DUPLICATE,
        /** Behind the last one published — either a write-side ordering violation or an operator replay. */
        REGRESSED
    }

    /** The ordering a row is judged on, and the value of it. */
    record Watermark(Key key, long value) {
    }

    private final int capacity;
    private final Map<AggregateId, Watermark> lastPublished;

    PublishOrderWatermarks() {
        this(CAPACITY);
    }

    PublishOrderWatermarks(int capacity) {
        this.capacity = capacity;
        this.lastPublished = new LinkedHashMap<>(16, 0.75f, true) {
            @Override
            protected boolean removeEldestEntry(Map.Entry<AggregateId, Watermark> eldest) {
                return size() > PublishOrderWatermarks.this.capacity;
            }
        };
    }

    /**
     * The ordering this row is judged on. Only an {@link SeqSource#APPLICATION} row carries a number
     * the application chose; a managed one is its insert order restated, and reading it would import
     * the sequence's own hazards as violations that never happened (HLD-managed-seq §6.1).
     */
    static Key keyFor(OutboxRecord record) {
        return record.seqSource() == SeqSource.APPLICATION ? Key.SEQ : Key.ID;
    }

    /** The value of {@link #keyFor(OutboxRecord)} for this row. */
    static long valueFor(OutboxRecord record) {
        return keyFor(record) == Key.SEQ ? record.seq() : record.id();
    }

    /**
     * Judge {@code record} against the last row published for its aggregate, raising the watermark
     * only when it advances. A {@code REGRESSED} verdict leaves the watermark <b>untouched</b> on
     * purpose: lowering it to a replayed row's value would make every later row look like an advance,
     * and the next genuine regression would go unseen (HLD §8). That is also what lets
     * {@link #lastPublished(AggregateId)} report, afterwards, the value the row was judged against.
     */
    Verdict record(OutboxRecord record) {
        AggregateId aggregateId = record.aggregateId();
        Key key = keyFor(record);
        long value = valueFor(record);
        Watermark last = lastPublished.get(aggregateId);
        if (last != null && last.key() == key) {
            if (value < last.value()) {
                return Verdict.REGRESSED;
            }
            if (value == last.value()) {
                return Verdict.DUPLICATE;
            }
        }
        lastPublished.put(aggregateId, new Watermark(key, value));
        return Verdict.ADVANCED;
    }

    /** The watermark this aggregate currently holds, or {@code null} if none is tracked. */
    Watermark lastPublished(AggregateId aggregateId) {
        return lastPublished.get(aggregateId);
    }

    /** Aggregates currently tracked — for tests asserting the LRU actually bounds itself. */
    int size() {
        return lastPublished.size();
    }
}
