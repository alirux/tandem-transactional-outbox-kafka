package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.OutboxMessage;

/**
 * The sink a {@link TransactionalOutboxTemplate} unit of work records outbox rows into
 * (LLD-spring-producer §3). Implementations collect the calls and the template inserts them, in
 * collection order, within the same transaction it opened.
 *
 * <p>A collector instance is valid only for the duration of the {@code execute} call that supplied it
 * and is not thread-safe.
 *
 * <p><b>Provided:</b> Tandem implements this interface and the application calls it. A minor version
 * may add abstract methods, so an application implementing it follows Tandem's minor versions
 * (HLD-api-stability §4.1).
 */
public interface OutboxCollector {

    /**
     * Record a pre-built message — the full-control, dependency-free path (no serializer involved).
     *
     * @param message the row to insert; must not be null
     */
    void add(OutboxMessage message);

    /**
     * Record a row from an object payload, serialized by the configured {@code PayloadSerializer}.
     *
     * @param aggregateType the aggregate type (topic routing, CloudEvents {@code type} fallback)
     * @param aggregateId   the aggregate the row belongs to
     * @param seq           the event's sequence number, increasing within the aggregate (HLD §4.2);
     *                      the aggregate's {@code version} only while each version produces one event
     * @param payload       the payload object to serialize
     * @throws com.codingful.tandem.core.exception.PayloadSerializationException if no
     *                      {@code PayloadSerializer} is configured, or serialization fails
     */
    void record(String aggregateType, AggregateId aggregateId, long seq, Object payload);

    /**
     * As {@link #record(String, AggregateId, long, Object)}, with a String aggregate id.
     *
     * @param aggregateType the aggregate type
     * @param aggregateId   the aggregate id
     * @param seq           the event's sequence number, increasing within the aggregate
     * @param payload       the payload object to serialize
     */
    void record(String aggregateType, String aggregateId, long seq, Object payload);

    /**
     * Record a row with <b>no sequence number</b> ({@code unsequenced()}, HLD-seq §4.1): no
     * {@code ce_seq} reaches consumers, who deduplicate on the event id instead. The usual choice
     * when nothing downstream reads a number the aggregate assigns to its events (§4.4).
     *
     * @param aggregateType the aggregate type (topic routing, CloudEvents {@code type} fallback)
     * @param aggregateId   the aggregate the row belongs to
     * @param payload       the payload object to serialize
     * @throws com.codingful.tandem.core.exception.PayloadSerializationException if no
     *                      {@code PayloadSerializer} is configured, or serialization fails
     */
    void record(String aggregateType, AggregateId aggregateId, Object payload);

    /**
     * As {@link #record(String, AggregateId, Object)}, with a String aggregate id.
     *
     * @param aggregateType the aggregate type
     * @param aggregateId   the aggregate id
     * @param payload       the payload object to serialize
     */
    void record(String aggregateType, String aggregateId, Object payload);
}
