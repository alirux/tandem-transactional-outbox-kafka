package com.codingful.tandem.jdbc;

import com.codingful.tandem.test.TandemTestContainer;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Supplier;
import javax.sql.DataSource;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Base for the JDBC integration tests: a single shared PostgreSQL container (Testcontainers) with the
 * committed baseline DDL applied, reset before each test. Tagged {@code integration} so it runs under
 * {@code integrationTest} (needs Docker), not the Docker-free {@code test} task.
 *
 * <p>The container comes from {@link TandemTestContainer#newPostgresContainer()}, so this suite and
 * the end-to-end one move together when a run is pointed at another engine or major.
 */
@Tag("integration")
abstract class AbstractPostgresIT {

    // Singleton container pattern: started once for the whole test run, reused across IT classes,
    // torn down by the Testcontainers reaper at JVM exit.
    private static final PostgreSQLContainer<?> POSTGRES;

    static final DataSource DATA_SOURCE;

    static {
        POSTGRES = TandemTestContainer.newPostgresContainer();
        POSTGRES.start();
        DATA_SOURCE = new SimpleDataSource(
                POSTGRES.getJdbcUrl(), POSTGRES.getUsername(), POSTGRES.getPassword());
        applyBaselineSchema(DATA_SOURCE);
    }

    @BeforeEach
    void resetTables() {
        execute("TRUNCATE tandem_outbox RESTART IDENTITY",
                "UPDATE tandem_bucket_lease SET owner = NULL, lease_until = NULL, paused = false",
                "TRUNCATE tandem_relay_member",
                // Cleared so each test's first component construction re-seeds the bucket-count guard
                // (LLD-bucket-count-guard); every IT uses bucketCount 256, so re-seeding is consistent.
                "TRUNCATE tandem_meta");
    }

    /**
     * Connections whose {@code now()} and {@code clock_timestamp()} resolve an hour behind
     * {@code pg_catalog}'s, the skew of a database whose clock runs behind the host reading from it. A
     * row's {@code created_at} is written on that clock by setting it from {@code clock_an_hour_behind.now()}.
     */
    static DataSource dataSourceWithClockAnHourBehind() {
        execute("CREATE SCHEMA IF NOT EXISTS clock_an_hour_behind",
                "CREATE OR REPLACE FUNCTION clock_an_hour_behind.now() RETURNS timestamptz LANGUAGE sql"
                        + " AS 'SELECT pg_catalog.now() - interval ''1 hour'''",
                "CREATE OR REPLACE FUNCTION clock_an_hour_behind.clock_timestamp() RETURNS timestamptz LANGUAGE sql"
                        + " AS 'SELECT pg_catalog.clock_timestamp() - interval ''1 hour'''");
        return dataSourceWith("currentSchema=clock_an_hour_behind,pg_catalog,public");
    }

    /**
     * Runs {@code read} on another thread while {@code tandem_outbox} is locked, and releases the lock
     * {@code wait} after the read is seen blocked on it: a backlog read held up for that long, as a busy
     * database holds one. Returns what the read returned.
     */
    static <T> T readHeldUpOnTheOutboxFor(Duration wait, Supplier<T> read) {
        try (Connection blocker = DATA_SOURCE.getConnection()) {
            blocker.setAutoCommit(false);
            try (Statement lock = blocker.createStatement()) {
                lock.execute("LOCK TABLE tandem_outbox IN ACCESS EXCLUSIVE MODE");
            }
            CompletableFuture<T> reading = CompletableFuture.supplyAsync(read);
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
            while (!blockedOnALock()) {
                if (System.nanoTime() > deadline) {
                    throw new IllegalStateException("the read never waited on the outbox lock");
                }
                Thread.sleep(10);
            }
            Thread.sleep(wait.toMillis());
            blocker.commit();
            return reading.get(10, TimeUnit.SECONDS);
        } catch (SQLException | ExecutionException | TimeoutException e) {
            throw new IllegalStateException("held-up read failed", e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted", e);
        }
    }

    private static boolean blockedOnALock() throws SQLException {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT count(*) FROM pg_stat_activity"
                     + " WHERE wait_event_type = 'Lock' AND datname = current_database()")) {
            rs.next();
            return rs.getInt(1) > 0;
        }
    }

    /** The oldest {@code created_at} in the outbox, as the database stored it. */
    static Instant oldestCreatedAt() {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery("SELECT min(created_at) FROM tandem_outbox")) {
            rs.next();
            return rs.getObject(1, OffsetDateTime.class).toInstant();
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed", e);
        }
    }

    /** Another DataSource on the same database, its connections opened with extra JDBC URL parameters. */
    static DataSource dataSourceWith(String urlParameters) {
        String url = POSTGRES.getJdbcUrl();
        return new SimpleDataSource(url + (url.contains("?") ? "&" : "?") + urlParameters,
                POSTGRES.getUsername(), POSTGRES.getPassword());
    }

    static void execute(String... statements) {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String sql : statements) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("test SQL failed", e);
        }
    }

    private static void applyBaselineSchema(DataSource dataSource) {
        String ddl = readBaseline();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(ddl);   // the PostgreSQL driver runs the multi-statement script in one call
        } catch (SQLException e) {
            throw new IllegalStateException("applying baseline schema failed", e);
        }
    }

    /** Locate {@code schema/postgres/tandem-baseline.sql} by walking up from the working directory. */
    private static String readBaseline() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("schema/postgres/tandem-baseline.sql");
            if (Files.exists(candidate)) {
                try {
                    return Files.readString(candidate, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("could not locate schema/postgres/tandem-baseline.sql");
    }
}
