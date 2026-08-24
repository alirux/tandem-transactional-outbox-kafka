package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

class SeqSourceTest {

    @Test
    void GIVEN_a_stored_provenance_WHEN_read_back_THEN_it_maps_to_the_mode_that_wrote_it() {
        assertThat(SeqSource.fromCode(SeqSource.APPLICATION.code())).isEqualTo(SeqSource.APPLICATION);
        assertThat(SeqSource.fromCode(SeqSource.MANAGED.code())).isEqualTo(SeqSource.MANAGED);
        assertThat(SeqSource.fromCode(SeqSource.NONE.code())).isEqualTo(SeqSource.NONE);
    }

    @Test
    void GIVEN_a_row_written_by_a_newer_version_WHEN_read_by_this_one_THEN_it_reports_unknown_instead_of_failing() {
        // The forward-compatibility contract: client, relay and admin may run at different versions
        // against one database, so meeting a provenance this build predates is ordinary. Throwing
        // would take the relay down over a row it could still deliver perfectly well.
        assertThat(SeqSource.fromCode(3)).isEqualTo(SeqSource.UNKNOWN);
        assertThat(SeqSource.fromCode(99)).isEqualTo(SeqSource.UNKNOWN);
    }

    @Test
    void GIVEN_the_unknown_reading_WHEN_it_is_about_to_be_written_back_THEN_its_code_cannot_be_mistaken_for_a_real_one() {
        assertThat(SeqSource.UNKNOWN.code()).isNegative();
        assertThat(SeqSource.fromCode(SeqSource.UNKNOWN.code())).isEqualTo(SeqSource.UNKNOWN);
    }

    @ParameterizedTest
    @EnumSource(SeqSource.class)
    void GIVEN_any_provenance_WHEN_its_code_is_read_back_THEN_it_round_trips_to_itself(SeqSource source) {
        assertThat(SeqSource.fromCode(source.code())).isEqualTo(source);
    }
}
