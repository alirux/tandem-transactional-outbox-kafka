package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.junit.jupiter.api.Test;

class AggregateIdTest {

    /** One code point the column counts once, held by Java as two {@code char}s. */
    private static final String OUTSIDE_THE_BMP = Character.toString(0x1F600);

    @Test
    void GIVEN_a_normal_value_WHEN_constructed_THEN_it_is_exposed_unchanged() {
        assertThat(AggregateId.of("order-1").value()).isEqualTo("order-1");
        assertThat(AggregateId.of("order-1")).hasToString("order-1");
    }

    @Test
    void GIVEN_a_null_value_WHEN_constructed_THEN_it_is_rejected() {
        assertThatThrownBy(() -> AggregateId.of(null)).isInstanceOf(NullPointerException.class);
    }

    @Test
    void GIVEN_a_blank_value_WHEN_constructed_THEN_it_is_rejected() {
        assertThatThrownBy(() -> AggregateId.of("   ")).isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_a_value_at_the_length_bound_WHEN_constructed_THEN_255_is_accepted_and_256_is_rejected() {
        assertThat(AggregateId.of("a".repeat(255)).value()).hasSize(255);
        assertThatThrownBy(() -> AggregateId.of("a".repeat(256)))
                .isInstanceOf(IllegalArgumentException.class);
    }

    @Test
    void GIVEN_characters_outside_the_basic_plane_WHEN_constructed_THEN_the_bound_counts_characters_as_the_column_does() {
        // VARCHAR(255) bounds characters, not the two UTF-16 units Java spends on each of these.
        assertThat(AggregateId.of(OUTSIDE_THE_BMP.repeat(255)).value()).isEqualTo(OUTSIDE_THE_BMP.repeat(255));
        assertThatThrownBy(() -> AggregateId.of(OUTSIDE_THE_BMP.repeat(256)))
                .isInstanceOf(IllegalArgumentException.class);
    }
}
