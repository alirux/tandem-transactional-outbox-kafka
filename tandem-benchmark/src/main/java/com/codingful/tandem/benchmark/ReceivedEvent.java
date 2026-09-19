package com.codingful.tandem.benchmark;

/**
 * One event as the co-located consumer took delivery of it (LLD-benchmark §5.2): the broker-neutral
 * subset {@link CorrelationConsumer} correlates on, so that the one place deciding correctness holds
 * no client type.
 *
 * @param aggregateId   the aggregate the event belongs to: Kafka's record key, AMQP's
 *                      {@code cloudEvents_partitionkey} header
 * @param seq           the per-aggregate sequence carried by the {@code seq} CloudEvents extension
 * @param t0Nanos       the harness's insert-time {@code nanoTime} header, or {@code -1} when the
 *                      header was missing or unparseable
 * @param receivedNanos {@code System.nanoTime()} at delivery, stamped by the receiver rather than by
 *                      the correlator: on a pushed transport the two are not the same instant
 */
public record ReceivedEvent(String aggregateId, long seq, long t0Nanos, long receivedNanos) {
}
