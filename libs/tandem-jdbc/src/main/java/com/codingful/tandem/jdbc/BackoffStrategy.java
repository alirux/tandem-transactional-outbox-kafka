package com.codingful.tandem.jdbc;

import java.time.Duration;

/**
 * Computes the delay before a retriable failure becomes eligible again (LLD-jdbc §3.6, Q13).
 * Pluggable; the default is {@link #fullJitter()}.
 *
 * <p><b>SPI:</b> the application implements this interface and Tandem calls it. A method added in a
 * minor version is always {@code default}, so an existing implementation keeps working
 * (HLD-api-stability §4.1).
 */
@FunctionalInterface
public interface BackoffStrategy {

    /**
     * Delay before the next attempt, given how many attempts have already been made
     * ({@code attempts} is the row's current attempt count, 0 for the first failure).
     */
    Duration delayFor(int attempts);

    /** Default: exponential with full jitter — {@code random(0, min(cap, base * 2^attempts))}. */
    static BackoffStrategy fullJitter() {
        return new FullJitterBackoff(Duration.ofSeconds(1), Duration.ofMinutes(5));
    }
}
