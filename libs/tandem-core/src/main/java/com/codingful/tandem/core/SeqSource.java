package com.codingful.tandem.core;

/**
 * Where a row's {@code seq} came from (HLD-managed-seq §4.5, §6.1) — the {@code seq_source} column,
 * and the write-side mode that produced it.
 *
 * <p><b>Why the row records this at all.</b> A persisted {@code seq} cannot say where it came from:
 * an application-assigned number and one drawn from {@code tandem_seq} are both a {@code BIGINT}.
 * The relay's ordering detector needs the distinction, because only an {@link #APPLICATION} value is
 * an ordering the application <i>declared</i> — independent of the order rows were inserted, and
 * therefore the only one worth checking a published order against. A {@link #MANAGED} value is the
 * insert order already, and a {@link #NONE} row declares nothing; for both the detector keys on the
 * row's {@code id} instead (HLD-managed-seq §6.1).
 */
public enum SeqSource {

    /** The application supplied the number, usually the aggregate's version. */
    APPLICATION(0),

    /** Tandem drew the number from the {@code tandem_seq} sequence at insert. */
    MANAGED(1),

    /** The row has no sequence number: {@code seq} is {@code NULL} and no {@code ce_seq} is published. */
    NONE(2),

    /**
     * A stored value this build does not recognise — produced only by {@link #fromCode(int)}, never
     * written. Its {@link #code()} is {@code -1} precisely so it cannot round-trip into the column.
     */
    UNKNOWN(-1);

    private final int code;

    SeqSource(int code) {
        this.code = code;
    }

    public int code() {
        return code;
    }

    /**
     * Maps a stored {@code seq_source} back to the enum, <b>tolerating a value this build predates</b>
     * by returning {@link #UNKNOWN} rather than throwing.
     *
     * <p>Deliberately unlike {@link OutboxStatus#fromCode(int)}, which throws: an unknown status has
     * no safe reading, while an unknown {@code seq_source} does — the detector treats
     * {@link #UNKNOWN} exactly as it treats {@link #MANAGED} and {@link #NONE}, keying on {@code id},
     * which under-reports and can never invent a violation. The column carries no {@code CHECK}
     * bounding it to the known values for the same reason: one would make adding a fourth source a
     * breaking schema change (HLD §1.4). Client, relay and admin may run at different versions
     * against one database, so meeting a newer value is ordinary, not exceptional.
     *
     * @param code the stored {@code seq_source} value
     * @return the matching constant, or {@link #UNKNOWN} if this build does not recognise {@code code}
     */
    public static SeqSource fromCode(int code) {
        return switch (code) {
            case 0 -> APPLICATION;
            case 1 -> MANAGED;
            case 2 -> NONE;
            default -> UNKNOWN;
        };
    }
}
