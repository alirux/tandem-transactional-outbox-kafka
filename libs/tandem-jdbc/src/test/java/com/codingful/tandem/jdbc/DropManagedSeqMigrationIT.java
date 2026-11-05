package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Pins the upgrade path, which the rest of the suite never walks: every other test starts from the
 * generated flat baseline, so it only ever sees a database created from scratch. Here the schema is
 * built the way an adopter's already is (the changesets the previous release shipped), then filled
 * with rows in each mode that release offered, and only then migrated. Data present at migration time
 * is the whole point: what happens to a row Tandem numbered is decided by the migration alone.
 */
class DropManagedSeqMigrationIT extends AbstractPostgresIT {

    private static final String SCHEMA = "as_shipped_by_the_previous_release";

    /** The changesets that had already shipped, and the one that migrates them. */
    private static final String[] ALREADY_SHIPPED = {
            "v1-baseline.sql", "v2-replays.sql", "v3-managed-seq.sql", "v4-optional-seq.sql", "v5-claim-under-backlog.sql"};
    private static final String MIGRATION = "v6-drop-managed-seq.sql";

    // The values the previous release wrote to seq_source; the column is gone once migrated.
    private static final int APPLICATION = 0;
    private static final int MANAGED = 1;
    private static final int NONE = 2;

    @BeforeAll
    static void buildTheOldSchemaFillItAndMigrate() throws SQLException {
        runInIsolatedSchema("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE", "CREATE SCHEMA " + SCHEMA);
        for (String shipped : ALREADY_SHIPPED) {
            runInIsolatedSchema(changesetSql(shipped));
        }
        runInIsolatedSchema(
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, seq_source, payload)"
                        + " VALUES ('numbered', 'Order', 0, 7, " + APPLICATION + ", '{}'::jsonb),"
                        + " ('unnumbered', 'Order', 0, NULL, " + NONE + ", '{}'::jsonb)",
                // A managed row leaves the column out, so the sequence numbers it.
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq_source, payload)"
                        + " VALUES ('tandem-numbered', 'Order', 0, " + MANAGED + ", '{}'::jsonb)");
        runInIsolatedSchema(changesetSql(MIGRATION));
    }

    @Test
    void GIVEN_a_row_numbered_by_the_application_WHEN_the_migration_runs_THEN_it_keeps_its_number() {
        assertThat(querySeqs("SELECT seq FROM tandem_outbox WHERE aggregate_id = 'numbered'")).containsExactly(7L);
    }

    @Test
    void GIVEN_a_row_numbered_by_tandem_WHEN_the_migration_runs_THEN_it_is_left_with_no_number() {
        assertThat(querySeqs("SELECT seq FROM tandem_outbox WHERE aggregate_id = 'tandem-numbered'"))
                .containsExactly((Long) null);
    }

    @Test
    void GIVEN_a_row_with_no_number_WHEN_the_migration_runs_THEN_it_still_has_none() {
        assertThat(querySeqs("SELECT seq FROM tandem_outbox WHERE aggregate_id = 'unnumbered'"))
                .containsExactly((Long) null);
    }

    @Test
    void GIVEN_a_migrated_database_WHEN_a_write_leaves_the_number_out_THEN_the_row_has_none_rather_than_one_drawn_for_it() {
        runInIsolatedSchema(
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, payload)"
                        + " VALUES ('left-out', 'Order', 0, '{}'::jsonb)");

        assertThat(querySeqs("SELECT seq FROM tandem_outbox WHERE aggregate_id = 'left-out'"))
                .containsExactly((Long) null);
    }

    /**
     * Reads the shipped changeset and drops only Liquibase's own directive lines; every remaining
     * line, comments included, is SQL. Reading the real file rather than a copy is deliberate: a
     * fixture would let the migration under test and the migration that ships drift apart.
     */
    private static String changesetSql(String fileName) {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("schema/postgres/changelog").resolve(fileName);
            if (Files.exists(candidate)) {
                try {
                    return Files.readAllLines(candidate, StandardCharsets.UTF_8).stream()
                            .filter(line -> !line.startsWith("--liquibase") && !line.startsWith("--changeset"))
                            .reduce("", (a, b) -> a + b + "\n");
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("could not locate schema/postgres/changelog/" + fileName);
    }

    private static void runInIsolatedSchema(String... statements) {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET search_path TO " + SCHEMA);
            for (String sql : statements) {
                stmt.execute(sql);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }

    /** The {@code seq} of each row returned, {@code null} where the row has none. */
    private static List<Long> querySeqs(String sql) {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET search_path TO " + SCHEMA);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                List<Long> out = new ArrayList<>();
                while (rs.next()) {
                    long seq = rs.getLong(1);
                    out.add(rs.wasNull() ? null : seq);
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
