package com.codingful.tandem.test.reference;

import static org.assertj.core.api.Assertions.assertThat;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import org.junit.jupiter.api.Test;

class ReferenceValuesTest {

    @Test
    void GIVEN_durations_WHEN_rendered_THEN_each_takes_the_largest_unit_that_divides_it_exactly() {
        assertThat(ReferenceValues.of(Duration.ofDays(14))).isEqualTo("14d");
        assertThat(ReferenceValues.of(Duration.ofHours(36))).isEqualTo("36h");
        assertThat(ReferenceValues.of(Duration.ofSeconds(60))).isEqualTo("1m");
        assertThat(ReferenceValues.of(Duration.ofSeconds(90))).isEqualTo("90s");
        assertThat(ReferenceValues.of(Duration.ofMillis(1500))).isEqualTo("1500ms");
        assertThat(ReferenceValues.of(Duration.of(250, ChronoUnit.MICROS))).isEqualTo("250us");
        assertThat(ReferenceValues.of(Duration.ofNanos(7))).isEqualTo("7ns");
    }

    @Test
    void GIVEN_an_enum_constant_WHEN_rendered_THEN_it_is_written_by_its_name_not_its_string() {
        assertThat(ReferenceValues.of(Mode.LEASE)).isEqualTo("LEASE");
    }

    @Test
    void GIVEN_no_value_WHEN_rendered_THEN_the_key_is_marked_as_having_no_default() {
        assertThat(ReferenceValues.of(null)).isEqualTo(ReferenceValues.NONE);
    }

    @Test
    void GIVEN_a_plain_value_WHEN_rendered_THEN_it_is_written_as_its_string() {
        assertThat(ReferenceValues.of(2.0)).isEqualTo("2.0");
        assertThat(ReferenceValues.of("application/json")).isEqualTo("application/json");
    }

    @Test
    void GIVEN_a_derived_or_always_applied_default_WHEN_rendered_THEN_it_is_marked_as_such() {
        assertThat(ReferenceValues.derived("availableProcessors() * 2")).isEqualTo("<derived: availableProcessors() * 2>");
        assertThat(ReferenceValues.always(Duration.ofSeconds(30))).isEqualTo("<always: 30s>");
    }

    private enum Mode {
        LEASE;

        @Override
        public String toString() {
            return "lease mode";
        }
    }
}
