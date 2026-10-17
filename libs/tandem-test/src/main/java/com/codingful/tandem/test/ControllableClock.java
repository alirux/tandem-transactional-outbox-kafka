package com.codingful.tandem.test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A {@link Clock} whose instant is set explicitly, so backoff, lease-expiry and retention behaviour
 * can be driven deterministically in unit tests (LLD-test §1). A real collaborator — not a mock.
 */
public final class ControllableClock extends Clock {

    private final AtomicReference<Instant> instant;
    private final ZoneId zone;

    /** @param start the clock's initial instant, in UTC */
    public ControllableClock(Instant start) {
        this(new AtomicReference<>(Objects.requireNonNull(start, "start")), ZoneOffset.UTC);
    }

    private ControllableClock(AtomicReference<Instant> instant, ZoneId zone) {
        this.instant = instant;
        this.zone = Objects.requireNonNull(zone, "zone");
    }

    /** A clock starting at {@code 2024-01-01T00:00:00Z}. */
    public static ControllableClock atEpochDay() {
        return new ControllableClock(Instant.parse("2024-01-01T00:00:00Z"));
    }

    /** Move time forward by {@code amount}. */
    public void advance(Duration amount) {
        instant.updateAndGet(current -> current.plus(amount));
    }

    /** Set the current instant explicitly. */
    public void set(Instant now) {
        instant.set(Objects.requireNonNull(now, "now"));
    }

    @Override
    public Instant instant() {
        return instant.get();
    }

    @Override
    public ZoneId getZone() {
        return zone;
    }

    @Override
    public Clock withZone(ZoneId zone) {
        // The same instant source, as Clock#withZone requires: a clock derived for a zone keeps
        // following advance() and set() on either of the two.
        return new ControllableClock(instant, zone);
    }
}
