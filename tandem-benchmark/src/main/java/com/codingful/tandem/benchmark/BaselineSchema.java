package com.codingful.tandem.benchmark;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import javax.sql.DataSource;

/**
 * Applies the committed {@code tandem_*} DDL to a benchmark database, reading the same file
 * {@code TandemTestContainer} applies from the repository rather than a copy of it, so a schema
 * change reaches a benchmark run without anyone remembering to mirror it.
 *
 * <p>The harness needs it separately from that helper because it starts its Postgres on its own: the
 * broker is chosen per run ({@link BrokerHarness}) and a container helper that always starts Kafka
 * would start one for a run that never publishes to it.
 */
final class BaselineSchema {

    private BaselineSchema() {
    }

    static void applyTo(DataSource dataSource) {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(read());   // the PostgreSQL driver runs the multi-statement script in one call
        } catch (SQLException e) {
            throw new IllegalStateException("applying baseline schema failed", e);
        }
    }

    /** Walks up from the working directory to {@code schema/postgres/tandem-baseline.sql}. */
    static String read() {
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
