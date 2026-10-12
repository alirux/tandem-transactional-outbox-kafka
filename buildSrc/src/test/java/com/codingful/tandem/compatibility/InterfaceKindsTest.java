package com.codingful.tandem.compatibility;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;

class InterfaceKindsTest {

    private static final String MODULE = "tandem-core";
    private static final String PORT = "com.acme.Port";
    private static final String LISTENER = "com.acme.Source$Listener";
    private static final String RELEASE_NOTE = "Port is implemented by Tandem only.";

    private static DeclaredBreaks.Entry declared(String type) {
        return new DeclaredBreaks.Entry(InterfaceKinds.GATE, MODULE, type, RELEASE_NOTE, 3);
    }

    private static InterfaceKinds.Verdict settle(StatedKind current, Map<String, StatedKind> baseline,
            DeclaredBreaks.Entry... declarations) {
        return InterfaceKinds.settle(MODULE, List.of(new PublicInterface(PORT, current)), baseline, List.of(declarations));
    }

    @Test
    void GIVEN_interfaces_without_a_single_stated_kind_WHEN_settled_THEN_the_check_fails_naming_each_one() {
        InterfaceKinds.Verdict verdict = InterfaceKinds.settle(MODULE, List.of(
                new PublicInterface(PORT, StatedKind.NONE),
                new PublicInterface(LISTENER, StatedKind.CONFLICTING),
                new PublicInterface("com.acme.Fine", StatedKind.SPI)), null, List.of());

        assertThat(verdict.problems()).hasSize(2);
        assertThat(verdict.problems().get(0)).startsWith(PORT).contains("states no kind");
        assertThat(verdict.problems().get(1)).startsWith(LISTENER).contains("more than one kind");
    }

    @Test
    void GIVEN_no_baseline_WHEN_settled_THEN_the_provided_interfaces_are_those_stating_provided() {
        InterfaceKinds.Verdict verdict = InterfaceKinds.settle(MODULE, List.of(
                new PublicInterface(PORT, StatedKind.SPI),
                new PublicInterface(LISTENER, StatedKind.PROVIDED)), null, List.of());

        assertThat(verdict.problems()).isEmpty();
        assertThat(verdict.provided()).containsExactly(LISTENER);
    }

    @Test
    void GIVEN_an_interface_whose_kind_did_not_change_WHEN_settled_THEN_it_keeps_its_kind() {
        assertThat(settle(StatedKind.SPI, Map.of(PORT, StatedKind.SPI)).problems()).isEmpty();
        assertThat(settle(StatedKind.SPI, Map.of(PORT, StatedKind.SPI)).provided()).isEmpty();
        assertThat(settle(StatedKind.PROVIDED, Map.of(PORT, StatedKind.PROVIDED)).problems()).isEmpty();
        assertThat(settle(StatedKind.PROVIDED, Map.of(PORT, StatedKind.PROVIDED)).provided()).containsExactly(PORT);
    }

    @Test
    void GIVEN_a_provided_interface_of_the_baseline_WHEN_it_becomes_SPI_THEN_it_passes_as_SPI() {
        InterfaceKinds.Verdict verdict = settle(StatedKind.SPI, Map.of(PORT, StatedKind.PROVIDED));

        assertThat(verdict.problems()).isEmpty();
        assertThat(verdict.provided()).isEmpty();
    }

    @Test
    void GIVEN_an_SPI_interface_of_the_baseline_WHEN_it_becomes_provided_undeclared_THEN_the_check_fails_with_the_declaration_to_write() {
        InterfaceKinds.Verdict verdict = settle(StatedKind.PROVIDED, Map.of(PORT, StatedKind.SPI));

        assertThat(verdict.problems()).singleElement().asString()
                .startsWith(PORT)
                .contains(InterfaceKinds.GATE + " | " + MODULE + " | " + PORT);
    }

    @Test
    void GIVEN_an_SPI_interface_of_the_baseline_WHEN_it_becomes_provided_and_is_declared_THEN_it_passes_as_provided() {
        DeclaredBreaks.Entry declaration = declared(PORT);

        InterfaceKinds.Verdict verdict = settle(StatedKind.PROVIDED, Map.of(PORT, StatedKind.SPI), declaration);

        assertThat(verdict.problems()).isEmpty();
        assertThat(verdict.provided()).containsExactly(PORT);
        assertThat(verdict.accepted()).containsExactly(declaration);
    }

    @Test
    void GIVEN_a_baseline_older_than_the_rule_or_without_the_interface_WHEN_settled_THEN_the_current_kind_holds() {
        Map<String, StatedKind> noParagraph = Map.of(PORT, StatedKind.NONE);
        Map<String, StatedKind> absent = Map.of(LISTENER, StatedKind.SPI);

        for (Map<String, StatedKind> baseline : List.of(noParagraph, absent)) {
            assertThat(settle(StatedKind.PROVIDED, baseline).problems()).isEmpty();
            assertThat(settle(StatedKind.PROVIDED, baseline).provided()).containsExactly(PORT);
            assertThat(settle(StatedKind.SPI, baseline).provided()).isEmpty();
        }
    }

    @Test
    void GIVEN_a_declaration_for_an_interface_that_was_not_reclassified_WHEN_settled_THEN_it_fails_as_stale() {
        InterfaceKinds.Verdict unchanged = settle(StatedKind.SPI, Map.of(PORT, StatedKind.SPI), declared(PORT));
        InterfaceKinds.Verdict unknown = settle(StatedKind.PROVIDED, Map.of(PORT, StatedKind.SPI), declared(PORT),
                declared(LISTENER));

        assertThat(unchanged.problems()).singleElement().asString().contains("matches no").contains(PORT);
        assertThat(unknown.problems()).singleElement().asString().contains("matches no").contains(LISTENER);
    }
}
