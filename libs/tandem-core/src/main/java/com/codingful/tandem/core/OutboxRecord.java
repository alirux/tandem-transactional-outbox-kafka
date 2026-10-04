package com.codingful.tandem.core;

import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * A persisted outbox row with its delivery state (LLD-core §1.4), returned by the relay-side
 * {@link com.codingful.tandem.core.port.OutboxStore} and the Admin API. Immutable; built via
 * {@link #builder()}.
 *
 * <p>Convenience accessors ({@link #aggregateId()}, {@link #aggregateType()}, {@link #type()},
 * {@link #seq()}, {@link #payload()}, …) delegate to the wrapped {@link OutboxMessage} so the
 * dispatcher and router read the record directly (LLD-kafka §3).
 */
public final class OutboxRecord {

    private final long id;
    private final OutboxMessage message;
    private final SeqSource seqSource;    // from the row's seq_source column — see seqSource()
    private final OutboxStatus status;
    private final int attempts;
    private final String lockedBy;        // nullable
    private final Instant lockedUntil;    // nullable
    private final String lastError;       // nullable
    private final Instant nextAttemptAt;  // nullable
    private final Instant createdAt;
    private final Long lamport;           // nullable; reserved — nothing writes it today (§9)
    private final String discardReason;   // nullable; set only by the Admin API's DiscardService, never the relay

    private OutboxRecord(Builder b) {
        this.id = b.id;
        this.message = Objects.requireNonNull(b.message, "message");
        if (this.message.seqSource() == SeqSource.MANAGED) {
            throw new IllegalStateException(
                    "a record describes a persisted row, so its seq is already assigned — resolve the "
                            + "number the insert produced before building one");
        }
        this.seqSource = b.seqSource == null ? this.message.seqSource() : b.seqSource;
        if (this.seqSource != SeqSource.UNKNOWN && (this.seqSource == SeqSource.NONE) != !this.message.hasSeq()) {
            throw new IllegalStateException(
                    "seqSource disagrees with whether the row has a seq, seqSource:" + this.seqSource
                            + ", hasSeq:" + this.message.hasSeq());
        }
        this.status = Objects.requireNonNull(b.status, "status");
        this.attempts = b.attempts;
        this.lockedBy = b.lockedBy;
        this.lockedUntil = b.lockedUntil;
        this.lastError = b.lastError;
        this.nextAttemptAt = b.nextAttemptAt;
        this.createdAt = Objects.requireNonNull(b.createdAt, "createdAt");
        this.lamport = b.lamport;
        this.discardReason = b.discardReason;
    }

    public long id() {
        return id;
    }

    /**
     * The stored message.
     *
     * <p>Its {@link OutboxMessage#seqSource()} describes how the <i>message</i> was built, which for a
     * row read back from storage is not the same question as where the row's number came from — a
     * mapper rebuilding a {@code MANAGED} row supplies the resolved number, so the message reports
     * {@code APPLICATION}. For a persisted row {@link OutboxRecord#seqSource()} is the authority.
     */
    public OutboxMessage message() {
        return message;
    }

    /**
     * Where this row's {@code seq} came from, read from the {@code seq_source} column — the value the
     * relay's ordering detector keys on (HLD-managed-seq §6.1). May be {@link SeqSource#UNKNOWN} if
     * the row was written by a newer version than this one.
     */
    public SeqSource seqSource() {
        return seqSource;
    }

    public OutboxStatus status() {
        return status;
    }

    public int attempts() {
        return attempts;
    }

    public String lockedBy() {
        return lockedBy;
    }

    public Instant lockedUntil() {
        return lockedUntil;
    }

    public String lastError() {
        return lastError;
    }

    public Instant nextAttemptAt() {
        return nextAttemptAt;
    }

    public Instant createdAt() {
        return createdAt;
    }

    /**
     * Lamport timestamp.
     *
     * <p><b>Always {@code null}, and read by nothing.</b> Reserved for the cross-aggregate
     * causal-ordering feature (HLD §9), which is designed but not implemented: no {@code lamport}
     * column exists in the shipped DDL, no row mapper populates this, and the publish path carries
     * no code for it. Inventory: {@code docs/HLD-causal-ordering.md} §0.
     */
    public Long lamport() {
        return lamport;
    }

    /** Operator-supplied reason recorded when this row was discarded; {@code null} otherwise (HLD-admin-api §4). */
    public String discardReason() {
        return discardReason;
    }

    // --- convenience delegates to the wrapped message ---

    public AggregateId aggregateId() {
        return message.aggregateId();
    }

    public String aggregateType() {
        return message.aggregateType();
    }

    public String type() {
        return message.type();
    }

    /**
     * @throws IllegalStateException if the row has no sequence number ({@link SeqSource#NONE});
     *                               guard with {@link #hasSeq()}
     */
    public long seq() {
        return message.seq();
    }

    /** Whether {@link #seq()} returns a value rather than throwing. */
    public boolean hasSeq() {
        return message.hasSeq();
    }

    public byte[] payload() {
        return message.payload();
    }

    public String contentType() {
        return message.contentType();
    }

    public Map<String, String> headers() {
        return message.headers();
    }

    public static Builder builder() {
        return new Builder();
    }

    @Override
    public String toString() {
        return "OutboxRecord{id=" + id
                + ", aggregateType=" + aggregateType()
                + ", aggregateId=" + aggregateId()
                + ", seq=" + message.renderSeq()
                + ", seqSource=" + seqSource
                + ", status=" + status
                + ", attempts=" + attempts + '}';
    }

    /** A copy of this record with the given status (and nothing else changed). */
    public Builder toBuilder() {
        return new Builder()
                .id(id)
                .message(message)
                .seqSource(seqSource)
                .status(status)
                .attempts(attempts)
                .lockedBy(lockedBy)
                .lockedUntil(lockedUntil)
                .lastError(lastError)
                .nextAttemptAt(nextAttemptAt)
                .createdAt(createdAt)
                .lamport(lamport)
                .discardReason(discardReason);
    }

    public static final class Builder {
        private long id;
        private OutboxMessage message;
        private SeqSource seqSource;
        private OutboxStatus status = OutboxStatus.PENDING;
        private int attempts;
        private String lockedBy;
        private Instant lockedUntil;
        private String lastError;
        private Instant nextAttemptAt;
        private Instant createdAt;
        private Long lamport;
        private String discardReason;

        private Builder() {
        }

        public Builder id(long id) {
            this.id = id;
            return this;
        }

        public Builder message(OutboxMessage message) {
            this.message = message;
            return this;
        }

        /**
         * The row's {@code seq_source}. Defaults to the message's own mode, which is right for a
         * record built from a message that was never persisted; a mapper reading a stored row must
         * set it explicitly, since a resolved {@code MANAGED} number is indistinguishable from an
         * application-assigned one once it is in the column (HLD-managed-seq §6.1).
         */
        public Builder seqSource(SeqSource seqSource) {
            this.seqSource = seqSource;
            return this;
        }

        public Builder status(OutboxStatus status) {
            this.status = status;
            return this;
        }

        public Builder attempts(int attempts) {
            this.attempts = attempts;
            return this;
        }

        public Builder lockedBy(String lockedBy) {
            this.lockedBy = lockedBy;
            return this;
        }

        public Builder lockedUntil(Instant lockedUntil) {
            this.lockedUntil = lockedUntil;
            return this;
        }

        public Builder lastError(String lastError) {
            this.lastError = lastError;
            return this;
        }

        public Builder nextAttemptAt(Instant nextAttemptAt) {
            this.nextAttemptAt = nextAttemptAt;
            return this;
        }

        public Builder createdAt(Instant createdAt) {
            this.createdAt = createdAt;
            return this;
        }

        /** Reserved for causal ordering (HLD §9) — no product code calls this; see {@link OutboxRecord#lamport()}. */
        public Builder lamport(Long lamport) {
            this.lamport = lamport;
            return this;
        }

        public Builder discardReason(String discardReason) {
            this.discardReason = discardReason;
            return this;
        }

        /**
         * @throws NullPointerException  if {@code message}, {@code status} or {@code createdAt} is unset
         * @throws IllegalStateException if the message still leaves {@code seq} to Tandem
         *                               ({@link OutboxMessage.Builder#managedSeq()}), or if
         *                               {@code seqSource} disagrees with whether the message carries a
         *                               number — the in-memory mirror of the row's own {@code CHECK}
         */
        public OutboxRecord build() {
            return new OutboxRecord(this);
        }
    }
}
