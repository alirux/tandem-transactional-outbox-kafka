package com.codingful.tandem.test.reference;

import java.time.Duration;
import java.util.List;
import java.util.Objects;

/**
 * Renders a default as the defaults reference files write it (LLD-api-stability §8.3), so every module
 * spells a duration, an enum constant or a missing value the same way. A test fixture of this
 * repository, never published.
 */
public final class ReferenceValues {

    /** A key or setting without a default. */
    public static final String NONE = "<none>";

    /** A key or setting startup fails without. */
    public static final String REQUIRED = "<required>";

    private static final List<Unit> UNITS = List.of(
            new Unit("d", Duration.ofDays(1).toNanos()),
            new Unit("h", Duration.ofHours(1).toNanos()),
            new Unit("m", Duration.ofMinutes(1).toNanos()),
            new Unit("s", Duration.ofSeconds(1).toNanos()),
            new Unit("ms", Duration.ofMillis(1).toNanos()),
            new Unit("us", 1_000));

    private ReferenceValues() {
    }

    /**
     * @param value the default in force; {@code null} when there is none
     * @return a duration in Spring's short form with the largest exact unit ({@code 14d}, {@code 100ms}),
     *         an enum constant by its name, {@value #NONE} for {@code null}, any other value as its string
     */
    public static String of(Object value) {
        if (value == null) {
            return NONE;
        }
        if (value instanceof Duration duration) {
            return duration(duration);
        }
        if (value instanceof Enum<?> constant) {
            return constant.name();
        }
        return String.valueOf(value);
    }

    /**
     * @param rule how the default is computed at run time, checked by the test before it is rendered
     * @return the rule, marked as a derived default
     */
    public static String derived(String rule) {
        return "<derived: " + Objects.requireNonNull(rule, "rule") + ">";
    }

    /**
     * @param value a setting Tandem applies even when the application sets it
     * @return the value, marked as always applied
     */
    public static String always(Object value) {
        return "<always: " + of(value) + ">";
    }

    private static String duration(Duration duration) {
        long nanos = duration.toNanos();
        for (Unit unit : UNITS) {
            if (nanos % unit.nanos() == 0) {
                return nanos / unit.nanos() + unit.suffix();
            }
        }
        return nanos + "ns";
    }

    private record Unit(String suffix, long nanos) {
    }
}
