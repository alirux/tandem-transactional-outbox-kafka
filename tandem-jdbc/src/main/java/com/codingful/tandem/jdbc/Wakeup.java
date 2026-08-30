package com.codingful.tandem.jdbc;

/**
 * How the write-side tells the relay that a bucket has just been written to, so the row is found by a
 * signal instead of by the next poll (dispatch-latency §3.4). The <b>same value belongs on both
 * sides</b>: the write-side emits it, the relay listens for it, and each is useless without the other.
 *
 * <p>It only ever shortens discovery. Polling stays active in every mode, so a signal that is never
 * emitted, never delivered, or dropped by a connection pooler costs latency and nothing else — the
 * relay still finds the row within {@link RelayConfig#pollInterval()}, exactly as it does with
 * {@link #NONE}.
 */
public enum Wakeup {

    /** The default: nothing is emitted and nothing is listened for; discovery is the poll loop alone. */
    NONE,

    /**
     * PostgreSQL {@code LISTEN}/{@code NOTIFY}. The write-side emits one
     * {@code SELECT pg_notify('tandem_wakeup', <bucket>)} per transaction, on the caller's own
     * connection and inside the caller's transaction, so a signal can never announce a row that a
     * rollback removed; the relay holds one dedicated connection listening on that channel
     * ({@link PgNotifyWakeup}).
     *
     * <p>The emitted statement runs in the caller's transaction, which is what makes the signal
     * transactional and is also its one cost to the writer: an error there (a full cluster-wide
     * notification queue) fails that transaction like any other statement in it.
     *
     * <p><b>PostgreSQL only</b>, and it needs a session the pooler leaves alone: a connection pooler in
     * transaction-pooling mode silently breaks {@code LISTEN}, which degrades to {@link #NONE}
     * behaviour rather than failing.
     */
    PG_NOTIFY;

    /**
     * The PostgreSQL notification channel {@link #PG_NOTIFY} uses, named here rather than on
     * {@link PgNotifyWakeup} so the write-side never names a class that touches the driver. Part of the
     * published contract between a writer and a relay, so it never changes.
     */
    public static final String CHANNEL = "tandem_wakeup";
}
