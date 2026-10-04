package com.codingful.tandem.benchmark;

/**
 * Which broker a run publishes to (LLD-benchmark §3.1), chosen by {@code --broker=}.
 *
 * <p>The scenarios are written against {@link BrokerHarness} and {@link EventReceiver}, never against
 * a client library, so this enum is the whole of their knowledge of the transport.
 */
public enum Broker {

    /** Apache Kafka. The default, and what every published number was measured on. */
    KAFKA,

    /**
     * RabbitMQ over AMQP 0.9.1. For correctness runs, not for numbers: the topology the harness
     * declares is one queue read by one consumer, which keeps per-aggregate ordering observable but
     * makes the delivered-throughput figures a property of that topology rather than of the broker
     * (LLD-benchmark §3.1).
     */
    RABBIT
}
