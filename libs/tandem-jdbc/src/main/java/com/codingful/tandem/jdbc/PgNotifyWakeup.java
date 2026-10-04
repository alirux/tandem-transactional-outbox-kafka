package com.codingful.tandem.jdbc;

import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.Objects;
import javax.sql.DataSource;
import org.postgresql.PGConnection;
import org.postgresql.PGNotification;

/**
 * The PostgreSQL {@code LISTEN}/{@code NOTIFY} wakeup source (dispatch-latency §3.4): one dedicated
 * connection, held for the relay's lifetime, listening on {@value Wakeup#CHANNEL} for the bucket numbers the
 * write-side emits inside its own transactions ({@link Wakeup#PG_NOTIFY}).
 *
 * <p>The transactional semantics are the reason this mechanism is sound rather than merely fast: the
 * database delivers a notification when its transaction commits and discards it when it rolls back, so
 * a signal can never announce a row that does not exist. PostgreSQL also collapses notifications with
 * the same channel and payload within one transaction, so a transaction writing fifty events for one
 * aggregate produces one signal, with no coalescing of our own.
 *
 * <p><b>Operational notes an operator has to know</b>, because each one turns this into a no-op rather
 * than an error (the relay keeps polling, so delivery is unaffected):
 *
 * <ul>
 *   <li>A <b>connection pooler in transaction-pooling mode</b> (PgBouncer's default, and the common
 *       one) breaks {@code LISTEN}: the session that subscribed is handed to someone else. This is the
 *       usual way for wakeups to stop working silently.</li>
 *   <li>This source holds <b>one connection permanently</b>, outside the pool's working set. Size the
 *       relay's pool with that one connection in mind.</li>
 *   <li>The notification queue is cluster-wide and bounded. A listener that stops draining it can
 *       ultimately make commits fail — which is why the loop below never does work of its own on the
 *       listening thread beyond handing the bucket number over.</li>
 *   <li>The driver must be PostgreSQL's. Against any other, the source logs once and stops, leaving
 *       the relay polling.</li>
 * </ul>
 *
 * <p>Needs {@code org.postgresql:postgresql} on the classpath. It is a {@code compileOnly} dependency
 * of {@code tandem-jdbc} — never redistributed, so the client write-side footprint is unchanged
 * (HLD §1.3) — and this class is the only one that touches it, loaded only when
 * {@link Wakeup#PG_NOTIFY} is configured.
 */
public final class PgNotifyWakeup implements WakeupSource {

    private static final Logger LOG = System.getLogger(PgNotifyWakeup.class.getName());

    /**
     * How long a receive blocks before the loop looks at {@code running} again. It bounds shutdown,
     * nothing else: notifications arrive when they arrive, and a timeout is not a poll.
     */
    private static final int RECEIVE_TIMEOUT_MILLIS = 1_000;

    private static final BackoffStrategy RECONNECT_BACKOFF =
            new FullJitterBackoff(Duration.ofSeconds(1), Duration.ofSeconds(30));

    private final DataSource dataSource;

    /** Read by the listening thread on every iteration; written by {@link #start}/{@link #stop}. */
    private volatile boolean running;

    /**
     * The connection currently being listened on, so {@link #stop()} can close it from another thread
     * and break the blocking receive. {@code null} whenever no connection is established.
     */
    private volatile Connection connection;

    private Thread thread;

    /**
     * @param dataSource where the listening connection is taken from; one connection is held for as
     *                   long as the source runs, so it must not be a transaction-scoped proxy
     * @throws NullPointerException if {@code dataSource} is {@code null}
     */
    public PgNotifyWakeup(DataSource dataSource) {
        this.dataSource = Objects.requireNonNull(dataSource, "dataSource");
    }

    @Override
    public synchronized void start(Listener listener) {
        Objects.requireNonNull(listener, "listener");
        if (running) {
            return;
        }
        running = true;
        thread = new Thread(() -> listen(listener), "tandem-relay-wakeup");
        thread.setDaemon(true);
        thread.start();
    }

    @Override
    public synchronized void stop() {
        if (!running) {
            return;
        }
        running = false;
        // Closing from here is what ends the blocking receive: the listening thread is inside a socket
        // read, where an interrupt alone would not reach it. The interrupt is kept only as a fallback,
        // and deliberately not sent first: interrupting a thread mid-read closes its channel underneath
        // it, so the connection's own close then fails and every shutdown logs an I/O error it caused
        // itself.
        closeQuietly(connection);
        if (!joined(Duration.ofSeconds(2))) {
            thread.interrupt();
            joined(Duration.ofSeconds(3));
        }
        thread = null;
    }

    /** @return whether the listening thread ended within {@code timeout} */
    private boolean joined(Duration timeout) {
        try {
            thread.join(timeout.toMillis());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        return !thread.isAlive();
    }

    /**
     * Subscribe, then hand every notification over until stopped, reconnecting on failure. Every exit
     * from the inner loop other than a stop is a lost subscription, so the next one starts with
     * {@link Listener#wakeAll()}: nothing emitted while disconnected was delivered, and only the poll
     * ceiling would otherwise cover that gap.
     */
    private void listen(Listener listener) {
        int failures = 0;
        while (running) {
            try (Connection conn = dataSource.getConnection()) {
                connection = conn;
                subscribe(conn);
                failures = 0;
                LOG.log(Level.INFO, "Listening for relay wakeups channel:" + Wakeup.CHANNEL);
                listener.wakeAll();
                receiveUntilStopped(conn.unwrap(PGConnection.class), listener);
            } catch (SQLException | RuntimeException e) {
                if (!running) {
                    break;
                }
                Duration delay = RECONNECT_BACKOFF.delayFor(failures++);
                LOG.log(Level.WARNING, "Relay wakeup subscription failed; polling until it is back"
                        + " channel:" + Wakeup.CHANNEL + ", retryInMs:" + delay.toMillis(), e);
                sleep(delay.toMillis());
            } catch (LinkageError missingDriver) {
                // The PostgreSQL driver is absent or incompatible. Retrying cannot fix it, and the relay
                // is fully correct without wakeups, so this stops for good rather than looping (C5).
                LOG.log(Level.ERROR, "Relay wakeups need the PostgreSQL driver; polling only"
                        + " channel:" + Wakeup.CHANNEL, missingDriver);
                return;
            } finally {
                connection = null;
            }
        }
    }

    /**
     * Autocommit is not incidental: notifications are handed to the client between transactions, so a
     * session left inside one would receive nothing until it ended.
     */
    private static void subscribe(Connection conn) throws SQLException {
        conn.setAutoCommit(true);
        try (Statement statement = conn.createStatement()) {
            // The channel is a fixed identifier, not a value: LISTEN takes no bind parameter.
            statement.execute("LISTEN " + Wakeup.CHANNEL);
        }
    }

    private void receiveUntilStopped(PGConnection conn, Listener listener) throws SQLException {
        while (running) {
            PGNotification[] notifications = conn.getNotifications(RECEIVE_TIMEOUT_MILLIS);
            if (notifications == null) {
                continue;   // the receive timed out; nothing was written, and running is re-checked
            }
            for (PGNotification notification : notifications) {
                deliver(listener, notification.getParameter());
            }
        }
    }

    /**
     * A payload this relay cannot read is skipped, not fatal: the channel is a published contract that
     * may grow a richer payload later, and a listener that died on an unrecognised one would turn a
     * forward-compatible change into an outage (HLD §1.4). Skipping costs the poll interval, once.
     */
    private static void deliver(Listener listener, String payload) {
        if (payload != null) {
            try {
                listener.wake(Integer.parseInt(payload.trim()));
                return;
            } catch (NumberFormatException unreadable) {
                // falls through to the same skip as a missing payload
            }
        }
        if (LOG.isLoggable(Level.DEBUG)) {
            LOG.log(Level.DEBUG, "Ignoring an unreadable wakeup payload channel:" + Wakeup.CHANNEL);
        }
    }

    private static void closeQuietly(Connection conn) {
        if (conn == null) {
            return;
        }
        try {
            conn.close();
        } catch (SQLException e) {
            LOG.log(Level.DEBUG, "Closing the wakeup connection failed channel:" + Wakeup.CHANNEL, e);
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
