package com.codingful.tandem.compatibility;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Settles the kind of each public interface for the gates (LLD-api-stability §4.3): every interface must
 * state one, and the baseline's kind prevails, so an interface the baseline released as SPI turns
 * provided only through a declared break.
 */
public final class InterfaceKinds {

    /** The gate kind of the declared breaks this check reads. */
    public static final String GATE = "interface-kind";

    /**
     * The outcome for one module.
     *
     * @param problems  what fails the check, one line each; empty when it passes
     * @param provided  the binary names of the interfaces whose kind for the gates is provided
     * @param accepted  the declarations that accepted a reclassification, for the build log
     */
    public record Verdict(List<String> problems, SortedSet<String> provided,
            Collection<DeclaredBreaks.Entry> accepted) {
    }

    private InterfaceKinds() {
    }

    /**
     * @param module       the module checked, as declarations name it
     * @param current      the module's interfaces in the working tree
     * @param baseline     the kinds the baseline's javadoc states, by binary name; {@code null} when the module
     *                     is not compared with a baseline
     * @param declarations the module's {@code interface-kind} entries of the declared breaks
     */
    public static Verdict settle(String module, List<PublicInterface> current, Map<String, StatedKind> baseline,
            List<DeclaredBreaks.Entry> declarations) {
        List<String> problems = new ArrayList<>();
        for (PublicInterface type : current) {
            if (type.kind() == StatedKind.NONE) {
                problems.add(type.binaryName() + " states no kind: its javadoc needs a " + StatedKind.SPI_PARAGRAPH
                        + " or a " + StatedKind.PROVIDED_PARAGRAPH + " paragraph (HLD-api-stability §4.1)");
            } else if (type.kind() == StatedKind.CONFLICTING) {
                problems.add(type.binaryName() + " states more than one kind: its javadoc must carry exactly one "
                        + StatedKind.SPI_PARAGRAPH + " or " + StatedKind.PROVIDED_PARAGRAPH + " paragraph");
            }
        }
        if (!problems.isEmpty()) {
            return new Verdict(problems, new TreeSet<>(), List.of());
        }
        // The one change the baseline's kind turns into a break: an interface applications implement against
        // the baseline, which may now gain abstract methods. A baseline that states no kind (one older than the
        // rule) or does not have the interface leaves the current kind in force.
        List<String> reclassified = current.stream()
                .filter(type -> type.kind() == StatedKind.PROVIDED)
                .filter(type -> baseline != null && baseline.get(type.binaryName()) == StatedKind.SPI)
                .map(PublicInterface::binaryName)
                .toList();
        DeclaredBreaks.Matching matching = DeclaredBreaks.match(reclassified, declarations);
        for (String type : matching.undeclared()) {
            problems.add(type + " was SPI in the baseline and is now provided, which breaks every application "
                    + "implementing it; if intended, declare it as '" + DeclaredBreaks.declaration(GATE, module, type)
                    + "'");
        }
        for (DeclaredBreaks.Entry stale : matching.stale()) {
            problems.add("Declared break matches no SPI interface turned provided, remove it: " + stale);
        }
        // In a passing run every interface keeps its current kind: the one row of the table where the baseline's
        // kind differs from it (SPI turned provided) passes only once declared, and then counts as provided.
        SortedSet<String> provided = new TreeSet<>();
        current.stream().filter(type -> type.kind() == StatedKind.PROVIDED)
                .forEach(type -> provided.add(type.binaryName()));
        return new Verdict(problems, provided, matching.accepted().values());
    }
}
