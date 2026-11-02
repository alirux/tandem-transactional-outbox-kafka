package com.codingful.tandem.core;

import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The write-side value the client builds and inserts (LLD-core §1.3) — immutable, built via
 * {@link #builder()}.
 *
 * <ul>
 *   <li><b>Payload is {@code byte[]}</b>: the core never serializes, so it forces no JSON library on
 *       the client (§1.3). Higher tiers may offer an {@code Object}-accepting overload backed by a
 *       {@link com.codingful.tandem.core.port.PayloadSerializer}.</li>
 *   <li><b>{@code seq} has two modes and no default</b> (HLD-seq §4.4). Exactly one of
 *       {@link Builder#seq(long)} (the application's own number for the event) or
 *       {@link Builder#unsequenced()} (the row has none, and no {@code ce_seq} is published) must be
 *       stated; building without one fails. Supplying the number buys the strongest ordering
 *       detection and costs the hazards of keeping it in step with the domain (HLD-seq §3.2); when
 *       undecided, {@link Builder#unsequenced()} is the usual answer.</li>
 *   <li><b>{@code lockedWrite}</b> asks the write-side adapter to serialise concurrent writers to the
 *       same aggregate with a transaction-scoped advisory lock, orthogonal to {@code seq}
 *       (HLD-seq §4.2): either mode can be combined with or without it.</li>
 *   <li>{@code contentType} is the only typed convenience field that maps onto a header: the
 *       write-side serializes it into {@code headers["content-type"]} at insert (LLD-jdbc §2), the
 *       key the relay reads for the CloudEvents {@code datacontenttype} (LLD-kafka §3.2). There is no
 *       dedicated column — it reuses {@code headers} (Pareto).</li>
 * </ul>
 *
 * <p>There is deliberately <b>no {@code causationId}</b> here: causation is part of the opt-in
 * causal-ordering feature (off by default; HLD §9), so it is not carried on the basic-round write
 * value.
 */
public final class OutboxMessage {

    private static final int MAX_COLUMN_LENGTH = 255;

    private final AggregateId aggregateId;
    private final String aggregateType;
    private final String type;            // CloudEvents `type`; nullable (Q20)
    private final long seq;               // meaningful only when hasSeq
    private final boolean hasSeq;         // false = unsequenced() (HLD-seq §4.1)
    private final boolean lockedWrite;    // true = serialise concurrent writers via advisory lock (HLD-seq §4.2)
    private final byte[] payload;         // already serialized (Q3)
    private final String contentType;     // nullable; persisted into headers["content-type"]
    private final Map<String, String> headers;  // immutable copy; may be empty

    private OutboxMessage(Builder b) {
        this.aggregateId = Objects.requireNonNull(b.aggregateId, "aggregateId");
        this.aggregateType = requireWithinColumn(requireNonBlank(b.aggregateType, "aggregateType"), "aggregateType");
        this.type = b.type == null ? null : requireWithinColumn(b.type, "type");
        this.hasSeq = resolveHasSeq(b);
        this.seq = b.seq;
        this.lockedWrite = b.lockedWrite;
        this.payload = Objects.requireNonNull(b.payload, "payload").clone();
        this.contentType = b.contentType;
        this.headers = Collections.unmodifiableMap(new LinkedHashMap<>(b.headers));
    }

    public AggregateId aggregateId() {
        return aggregateId;
    }

    public String aggregateType() {
        return aggregateType;
    }

    /** CloudEvents {@code type}; may be {@code null} (Q20 — the relay falls back to {@code aggregateType}). */
    public String type() {
        return type;
    }

    /**
     * The app-assigned sequence number.
     *
     * @throws IllegalStateException if the message is {@link Builder#unsequenced()}: any value
     *                               returned would be a fiction; guard with {@link #hasSeq()}
     */
    public long seq() {
        if (!hasSeq) {
            throw new IllegalStateException("this message carries no seq, guard with hasSeq()");
        }
        return seq;
    }

    /** Whether {@link #seq()} returns a value rather than throwing. */
    public boolean hasSeq() {
        return hasSeq;
    }

    /**
     * Whether the write-side adapter serialises concurrent writers to this aggregate with a
     * transaction-scoped advisory lock (HLD-seq §4.2). Independent of {@link #hasSeq()}: either
     * mode can ask for the lock or not.
     */
    public boolean lockedWrite() {
        return lockedWrite;
    }

    /** The serialized payload bytes. Returns a defensive copy — the value is immutable. */
    public byte[] payload() {
        return payload.clone();
    }

    /** e.g. {@code "application/json"}; may be {@code null}. */
    public String contentType() {
        return contentType;
    }

    /** Immutable; may be empty. */
    public Map<String, String> headers() {
        return headers;
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof OutboxMessage other)) {
            return false;
        }
        return seq == other.seq
                && hasSeq == other.hasSeq
                && lockedWrite == other.lockedWrite
                && aggregateId.equals(other.aggregateId)
                && aggregateType.equals(other.aggregateType)
                && Objects.equals(type, other.type)
                && Arrays.equals(payload, other.payload)
                && Objects.equals(contentType, other.contentType)
                && headers.equals(other.headers);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(aggregateId, aggregateType, type, seq, hasSeq, lockedWrite, contentType, headers);
        result = 31 * result + Arrays.hashCode(payload);
        return result;
    }

    @Override
    public String toString() {
        return "OutboxMessage{aggregateId=" + aggregateId
                + ", aggregateType=" + aggregateType
                + ", type=" + type
                + ", seq=" + renderSeq()
                + ", lockedWrite=" + lockedWrite
                + ", contentType=" + contentType
                + ", payloadBytes=" + payload.length
                + ", headerNames=" + headers.keySet() + '}';
    }

    /** {@code toString} rendering shared with {@link OutboxRecord}: the number, or {@code none}. */
    String renderSeq() {
        return hasSeq ? Long.toString(seq) : "none";
    }

    /**
     * Exactly one of the two modes must be stated. The failure names both and what each is for:
     * this is the moment the decision is actually being made, and a message that only refuses would
     * send the caller looking for documentation they are already standing in (HLD-seq §4.4).
     */
    private static boolean resolveHasSeq(Builder b) {
        if (b.seqAssigned && b.unsequenced) {
            throw new IllegalStateException(
                    "seq(long) and unsequenced() are mutually exclusive: a message has a sequence number or none");
        }
        if (!b.seqAssigned && !b.unsequenced) {
            throw new IllegalStateException(
                    "no seq mode stated: call seq(long) if consumers read a number the aggregate assigns"
                            + " to each event, or unsequenced() if they deduplicate on the event id;"
                            + " unsequenced() is the usual answer");
        }
        return b.seqAssigned;
    }

    private static String requireNonBlank(String value, String field) {
        Objects.requireNonNull(value, field);
        if (value.isBlank()) {
            throw new IllegalArgumentException(field + " must not be blank");
        }
        return value;
    }

    /**
     * The bound of the {@code VARCHAR(255)} column, counted in code points like {@link AggregateId}:
     * checked here so that a value an in-memory outbox accepts is one the database accepts too.
     */
    private static String requireWithinColumn(String value, String field) {
        if (value.codePointCount(0, value.length()) > MAX_COLUMN_LENGTH) {
            throw new IllegalArgumentException(field + " exceeds " + MAX_COLUMN_LENGTH + " chars");
        }
        return value;
    }

    public static final class Builder {
        private AggregateId aggregateId;
        private String aggregateType;
        private String type;
        private long seq;
        private boolean seqAssigned;
        private boolean unsequenced;
        private boolean lockedWrite;
        private byte[] payload;
        private String contentType;
        private final Map<String, String> headers = new LinkedHashMap<>();

        private Builder() {
        }

        public Builder aggregateId(AggregateId aggregateId) {
            this.aggregateId = aggregateId;
            return this;
        }

        public Builder aggregateId(String aggregateId) {
            this.aggregateId = AggregateId.of(aggregateId);
            return this;
        }

        public Builder aggregateType(String aggregateType) {
            this.aggregateType = aggregateType;
            return this;
        }

        /** CloudEvents {@code type}; optional — the relay falls back to {@code aggregateType} if unset (Q20). */
        public Builder type(String type) {
            this.type = type;
            return this;
        }

        /**
         * The app-assigned sequence number of this event, increasing within its aggregate (HLD §4.2).
         * It is a number per event, not per state: it equals the aggregate's version only while
         * every version produces exactly one event. It is what the relay can check a published order
         * against, at the cost of keeping it in step with the domain (HLD-seq §3.2, §4.4).
         *
         * <p>Mutually exclusive with {@link #unsequenced()}.
         */
        public Builder seq(long seq) {
            this.seq = seq;
            this.seqAssigned = true;
            return this;
        }

        /**
         * Gives the row no sequence number at all: the write side stores {@code NULL} and the relay
         * publishes no {@code ce_seq}, leaving consumers to deduplicate on the event id, which is
         * unique by construction (HLD-seq §4.1).
         *
         * <p>The usual answer when undecided (§4.4). It asks nothing of the domain, and it is the one
         * starting point that stays open: adding a number later is additive for consumers, while
         * taking one away is not. What it gives up is the stronger ordering detection an
         * application-assigned number buys — an unsequenced row is still checked, against its insert
         * order rather than a declared one (§6.1).
         *
         * <p>Such a row falls outside {@code UNIQUE (aggregate_id, seq)}, and loses nothing by it:
         * that constraint catches a duplicate number, which cannot arise where no number is supplied.
         *
         * <p>Mutually exclusive with {@link #seq(long)}.
         */
        public Builder unsequenced() {
            this.unsequenced = true;
            return this;
        }

        /**
         * Serialises concurrent writers to this aggregate with a transaction-scoped advisory lock,
         * taken by the write-side adapter on the same connection right before its insert
         * (HLD-seq §4.2): a concurrent transaction writing the same aggregate blocks until this one
         * commits, making commit order match insert order. Independent of {@code seq}: combine
         * freely with either mode.
         *
         * <p>Costs one statement on the caller's hot path and turns concurrent writers to one
         * aggregate into waiters; not the default (HLD-seq §5).
         */
        public Builder lockedWrite() {
            this.lockedWrite = true;
            return this;
        }

        /**
         * The already-serialized event bytes; the core never serializes (§1.3). A storage adapter may
         * constrain the encoding: the bundled PostgreSQL adapter stores this in a {@code jsonb} column,
         * so it must be valid UTF-8 JSON there (see {@code JdbcOutboxRepository}).
         */
        public Builder payload(byte[] payload) {
            this.payload = payload;
            return this;
        }

        /** e.g. {@code "application/json"}; persisted into {@code headers["content-type"]} (overrides any existing entry). */
        public Builder contentType(String contentType) {
            this.contentType = contentType;
            return this;
        }

        /** Adds one header; call repeatedly for multiple entries. */
        public Builder header(String name, String value) {
            this.headers.put(
                    Objects.requireNonNull(name, "header name"),
                    Objects.requireNonNull(value, "header value"));
            return this;
        }

        /** Merges all entries of {@code headers} (same semantics as repeated {@link #header}). */
        public Builder headers(Map<String, String> headers) {
            headers.forEach(this::header);
            return this;
        }

        /**
         * @throws NullPointerException     if a required field ({@code aggregateId}, {@code aggregateType},
         *                                  {@code payload}) is unset
         * @throws IllegalArgumentException if {@code aggregateType} is blank, or {@code aggregateType} or
         *                                  {@code type} is longer than 255 code points
         * @throws IllegalStateException    if neither {@link #seq(long)} nor {@link #unsequenced()} was
         *                                  stated, or if both were
         */
        public OutboxMessage build() {
            return new OutboxMessage(this);
        }
    }
}
