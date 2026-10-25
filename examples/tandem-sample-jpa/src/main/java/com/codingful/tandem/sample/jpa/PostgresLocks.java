package com.codingful.tandem.sample.jpa;

import java.time.Duration;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * [DEMO-ONLY] Observes a lock wait inside PostgreSQL, so the demo and its tests prove a writer is
 * blocked rather than assume it from a thread that has simply not been scheduled yet.
 */
final class PostgresLocks {
    private PostgresLocks() {}

    /** The backend id of the session running the current transaction. */
    static int backendPid(JdbcTemplate jdbc) {
        return jdbc.queryForObject("SELECT pg_backend_pid()", Integer.class);
    }

    /** Whether the session {@code pid} waits on another session's lock before {@code timeout} elapses. */
    static boolean awaitBlocked(JdbcTemplate jdbc, int pid, Duration timeout) throws InterruptedException {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (true) {
            if (Boolean.TRUE.equals(jdbc.queryForObject(
                    "SELECT cardinality(pg_blocking_pids(?)) > 0", Boolean.class, pid))) {
                return true;
            }
            if (System.nanoTime() >= deadline) {
                return false;
            }
            Thread.sleep(20);
        }
    }
}
