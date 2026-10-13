package com.codingful.tandem.compatibility;

/**
 * What an interface's own javadoc states about its kind (HLD-api-stability §4.1): exactly one of the
 * paragraphs {@code <b>SPI:</b>} and {@code <b>Provided:</b>} states a kind; none, or more than one, does
 * not.
 */
public enum StatedKind {
    SPI,
    PROVIDED,
    NONE,
    CONFLICTING;

    static final String SPI_PARAGRAPH = "<b>SPI:</b>";
    static final String PROVIDED_PARAGRAPH = "<b>Provided:</b>";

    /** The kind a javadoc comment states; {@code null} stands for a declaration without javadoc. */
    static StatedKind of(String javadoc) {
        if (javadoc == null) {
            return NONE;
        }
        int spi = occurrences(javadoc, SPI_PARAGRAPH);
        int provided = occurrences(javadoc, PROVIDED_PARAGRAPH);
        if (spi + provided == 0) {
            return NONE;
        }
        if (spi + provided > 1) {
            return CONFLICTING;
        }
        return spi == 1 ? SPI : PROVIDED;
    }

    private static int occurrences(String text, String paragraph) {
        int count = 0;
        for (int at = text.indexOf(paragraph); at >= 0; at = text.indexOf(paragraph, at + paragraph.length())) {
            count++;
        }
        return count;
    }
}
