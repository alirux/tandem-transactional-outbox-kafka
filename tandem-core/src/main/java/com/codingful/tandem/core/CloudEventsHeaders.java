package com.codingful.tandem.core;

/**
 * Names of the CloudEvents extension attributes Tandem sets and their <b>binary-mode</b> header forms
 * (LLD-kafka §3.1/§3.3, LLD-rabbitmq §5). In binary mode an extension {@code foo} becomes the header
 * {@code ce_foo} over Kafka and {@code cloudEvents_foo} over AMQP. Declared in the dependency-free
 * core because the consumer-side adapters read these exact names off the wire (HLD §1.4) — they must
 * never drift.
 *
 * <p>Tandem's own extensions are named here, plus the binary forms of the standard {@code id}
 * attribute, which a consumer deduplicates on. The other standard attributes (source/type/subject/time)
 * are written by the CloudEvents SDK on Kafka and composed from {@link #AMQP_BINARY_PREFIX} plus the
 * attribute's own spec name on AMQP, where the SDK ships no 0.9.1 binding.
 */
public final class CloudEventsHeaders {

    private CloudEventsHeaders() {
    }

    /** Kafka binary-mode header prefix: an extension {@code foo} is written as {@code ce_foo}. */
    public static final String BINARY_PREFIX = "ce_";

    /**
     * AMQP binary-mode header prefix: an attribute {@code foo} is written as {@code cloudEvents_foo}
     * (LLD-rabbitmq §5). The CloudEvents AMQP binding prefixes every attribute, standard ones
     * included, because AMQP application properties are a flat namespace shared with the
     * application's own headers.
     */
    public static final String AMQP_BINARY_PREFIX = "cloudEvents_";

    /** The standard {@code id} attribute (the outbox row id) as a Kafka binary-mode header: {@code ce_id}. */
    public static final String CE_ID = BINARY_PREFIX + "id";

    /** The standard {@code id} attribute (the outbox row id) as an AMQP binary-mode header: {@code cloudEvents_id}. */
    public static final String AMQP_ID = AMQP_BINARY_PREFIX + "id";

    /**
     * Per-aggregate sequence. Extension {@code seq} → header {@code ce_seq}. Present only when the row
     * carries a number; a row written unsequenced publishes none (HLD-managed-seq §4.5).
     */
    public static final String EXT_SEQ = "seq";
    public static final String CE_SEQ = BINARY_PREFIX + EXT_SEQ;
    public static final String AMQP_SEQ = AMQP_BINARY_PREFIX + EXT_SEQ;

    /** Partition key (always equals the aggregate id). Extension {@code partitionkey} → header {@code ce_partitionkey}. */
    public static final String EXT_PARTITION_KEY = "partitionkey";
    public static final String CE_PARTITION_KEY = BINARY_PREFIX + EXT_PARTITION_KEY;
    public static final String AMQP_PARTITION_KEY = AMQP_BINARY_PREFIX + EXT_PARTITION_KEY;

    /**
     * Lamport / logical clock. Extension {@code logicalclock} → header {@code ce_logicalclock}.
     * ("lamport" is the design term; "logicalclock" is the on-the-wire name — LLD-kafka §3.3.)
     *
     * <p><b>Reserved — a name only.</b> Nothing reads or writes this constant: the cross-aggregate
     * causal-ordering feature that would emit the extension (HLD §9) is designed but not
     * implemented, and the publish path deliberately carries no code for it. Declared here so the
     * on-the-wire name cannot drift if the feature is ever built. Inventory:
     * {@code docs/HLD-causal-ordering.md} §0.
     */
    public static final String EXT_LOGICAL_CLOCK = "logicalclock";
    public static final String CE_LOGICAL_CLOCK = BINARY_PREFIX + EXT_LOGICAL_CLOCK;

    /**
     * Causation id: the event that caused this one. Extension {@code causationid} → header
     * {@code ce_causationid}. It differs from the row-header name {@link TandemHeaders#CAUSATION_ID}
     * because CloudEvents extension names admit only lowercase letters and digits.
     *
     * <p><b>Reserved, a name only.</b> Nothing reads or writes this constant: like
     * {@link #EXT_LOGICAL_CLOCK} it belongs to the cross-aggregate causal-ordering feature (HLD §9),
     * designed but not implemented. Declared here so the on-the-wire name cannot drift if the feature
     * is ever built. Inventory: {@code docs/HLD-causal-ordering.md} §0.
     */
    public static final String EXT_CAUSATION_ID = "causationid";
    public static final String CE_CAUSATION_ID = BINARY_PREFIX + EXT_CAUSATION_ID;
}
