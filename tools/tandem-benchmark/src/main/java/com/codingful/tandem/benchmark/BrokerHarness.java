package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import java.util.OptionalLong;

/**
 * The broker half of {@link BenchmarkEnvironment} (LLD-benchmark §3.1): the container, the topology
 * the run needs, the dispatchers the relays publish through, and the receivers the harness
 * correlates on.
 *
 * <p>It exists because the relay engine is transport-neutral and the scenarios are correctness
 * gates, so running them against a second broker is a matter of supplying the two ends rather than
 * of rewriting what they assert. The harness owns every client it hands out and closes them all on
 * {@link #close()}; callers never close a dispatcher themselves.
 */
public interface BrokerHarness extends AutoCloseable {

    /**
     * @param broker which implementation to build
     * @param config the run's sizing, the delivery/confirm deadline above all, which both
     *               implementations must report back through {@link #deliveryTimeoutMillis()}
     * @return a harness, not yet started
     */
    static BrokerHarness forBroker(Broker broker, BenchmarkConfig config) {
        return switch (broker) {
            case KAFKA -> new KafkaBrokerHarness(config);
            case RABBIT -> new RabbitBrokerHarness(config);
        };
    }

    /** Starts the broker container and declares whatever topology the run publishes into. */
    void start();

    /**
     * A new dispatcher with its own connection to the broker, as a separate relay process would have.
     *
     * @param spanRecorder the recorder to wire into it; {@link TandemSpanRecorder#NOOP} for every
     *                     scenario but the tracing demo
     * @return the dispatcher, owned and closed by this harness
     */
    OutboxDispatcher newDispatcher(TandemSpanRecorder spanRecorder);

    /**
     * The deadline this broker's dispatchers really enforce on a publish, for the relay's
     * {@code rowLease > deliveryTimeout} startup invariant (LLD-jdbc §3.5). Kafka's
     * {@code delivery.timeout.ms} and AMQP's publisher-confirm timeout are the same quantity here.
     */
    long deliveryTimeoutMillis();

    /**
     * A receiver reading everything published by this run.
     *
     * @param group names the subscription: a Kafka consumer group, an AMQP consumer tag
     */
    EventReceiver newReceiver(String group);

    /**
     * Drops whatever the previous scenario left undelivered, so each one starts from the state the
     * first one found. The broker-side counterpart of
     * {@link BenchmarkEnvironment#resetBetweenScenarios()}.
     */
    void resetBetweenScenarios();

    /**
     * How many publishes this harness's dispatchers are still waiting on the broker to settle, or
     * {@link OptionalLong#empty()} when the client does not expose it.
     *
     * <p>Empty is not zero, and the distinction is the point: an adapter that tracks pending publishes
     * in a map of its own has somewhere to leak, so a run that ends with this at zero has shown
     * something a run that cannot see it has not (S12).
     */
    OptionalLong inFlightPublishes();

    /**
     * Makes the broker stop answering, as a network partition or a hung broker would, until
     * {@link #restoreBroker()}. The connection is not closed politely: whether the client notices at
     * all, and how long it takes, is part of what S12 measures.
     *
     * @throws UnsupportedOperationException if this harness cannot interrupt its broker
     */
    void interruptBroker();

    /** Undoes {@link #interruptBroker()}. */
    void restoreBroker();

    @Override
    void close();
}
