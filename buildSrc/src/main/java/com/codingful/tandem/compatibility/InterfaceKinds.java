package com.codingful.tandem.compatibility;

import java.util.ArrayList;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;

/**
 * Settles the kind of each public interface for the Java API gate (LLD-api-stability §4.3): every interface
 * must state one in its javadoc, and the provided ones may gain abstract methods.
 */
public final class InterfaceKinds {

    /**
     * The outcome for one module.
     *
     * @param problems what fails the check, one line each; empty when it passes
     * @param provided the binary names of the interfaces that state they are provided
     */
    public record Verdict(List<String> problems, SortedSet<String> provided) {
    }

    private InterfaceKinds() {
    }

    /** @param current the module's interfaces in the working tree */
    public static Verdict settle(List<PublicInterface> current) {
        List<String> problems = new ArrayList<>();
        SortedSet<String> provided = new TreeSet<>();
        for (PublicInterface type : current) {
            switch (type.kind()) {
                case NONE -> problems.add(type.binaryName() + " states no kind: its javadoc needs a "
                        + StatedKind.SPI_PARAGRAPH + " or a " + StatedKind.PROVIDED_PARAGRAPH
                        + " paragraph (HLD-api-stability §4.1)");
                case CONFLICTING -> problems.add(type.binaryName() + " states more than one kind: its javadoc must "
                        + "carry exactly one " + StatedKind.SPI_PARAGRAPH + " or " + StatedKind.PROVIDED_PARAGRAPH
                        + " paragraph");
                case PROVIDED -> provided.add(type.binaryName());
                case SPI -> {
                }
            }
        }
        return new Verdict(problems, provided);
    }
}
