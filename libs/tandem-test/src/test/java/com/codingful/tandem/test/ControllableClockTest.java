package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;
import org.junit.jupiter.api.Test;

class ControllableClockTest {

    private static final ZoneId ROME = ZoneId.of("Europe/Rome");

    /**
     * Code under test often derives a zoned clock to compute local dates; it must keep following the
     * test's control of time, as a {@link Clock#withZone} of any clock keeps its instant source.
     */
    @Test
    void GIVEN_a_clock_derived_for_another_zone_WHEN_the_test_advances_time_THEN_the_derived_clock_follows() {
        ControllableClock clock = ControllableClock.atEpochDay();
        Clock rome = clock.withZone(ROME);

        clock.advance(Duration.ofHours(3));
        clock.set(clock.instant().plusSeconds(1));

        assertThat(rome.instant()).isEqualTo(clock.instant());
        assertThat(rome.getZone()).isEqualTo(ROME);
    }

    @Test
    void GIVEN_a_derived_clock_WHEN_the_test_sets_time_THEN_the_original_reads_it_too() {
        ControllableClock clock = ControllableClock.atEpochDay();
        Instant later = Instant.parse("2024-06-01T12:00:00Z");

        ((ControllableClock) clock.withZone(ROME)).set(later);

        assertThat(clock.instant()).isEqualTo(later);
    }
}
