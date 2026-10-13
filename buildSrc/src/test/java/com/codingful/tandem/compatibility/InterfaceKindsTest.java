package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.Test;

class InterfaceKindsTest {

    private static final String PORT = "com.acme.Port";
    private static final String LISTENER = "com.acme.Source$Listener";

    @Test
    void GIVEN_interfaces_without_a_single_stated_kind_WHEN_settled_THEN_the_check_fails_naming_each_one() {
        InterfaceKinds.Verdict verdict = InterfaceKinds.settle(List.of(
                new PublicInterface(PORT, StatedKind.NONE),
                new PublicInterface(LISTENER, StatedKind.CONFLICTING),
                new PublicInterface("com.acme.Fine", StatedKind.SPI)));

        assertThat(verdict.problems()).hasSize(2);
        assertThat(verdict.problems().get(0)).startsWith(PORT).contains("states no kind");
        assertThat(verdict.problems().get(1)).startsWith(LISTENER).contains("more than one kind");
    }

    @Test
    void GIVEN_interfaces_of_both_kinds_WHEN_settled_THEN_the_provided_ones_are_those_stating_provided() {
        InterfaceKinds.Verdict verdict = InterfaceKinds.settle(List.of(
                new PublicInterface(PORT, StatedKind.SPI),
                new PublicInterface(LISTENER, StatedKind.PROVIDED),
                new PublicInterface("com.acme.Other", StatedKind.PROVIDED)));

        assertThat(verdict.problems()).isEmpty();
        assertThat(verdict.provided()).containsExactly("com.acme.Other", LISTENER);
    }
}
