package com.codingful.tandem.benchmark;

import java.time.Duration;
import java.util.List;

/**
 * The harness's read side over whichever broker the run uses (LLD-benchmark §3.1).
 *
 * <p>Poll-shaped on purpose, although AMQP pushes and Kafka polls: draining a buffer is expressible
 * over both, while a callback is not expressible over a consumer that must be polled to make
 * progress. Each implementation stamps {@link ReceivedEvent#receivedNanos()} where its own transport
 * actually hands the event over.
 */
public interface EventReceiver extends AutoCloseable {

    /**
     * @param timeout how long to wait when nothing has arrived yet
     * @return what the broker delivered since the last call, in delivery order; empty when the
     *         timeout expired or {@link #wakeup()} cut the wait short
     */
    List<ReceivedEvent> poll(Duration timeout);

    /** Cuts short a {@link #poll} in progress so the polling thread can see a stop request. */
    void wakeup();

    @Override
    void close();
}
