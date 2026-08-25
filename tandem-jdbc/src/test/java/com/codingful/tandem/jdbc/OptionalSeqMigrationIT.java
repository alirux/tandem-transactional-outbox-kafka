package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.SeqSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * Pins the upgrade path, which the rest of the suite never walks: every other test starts from the
 * generated flat baseline, so it only ever sees a database created from scratch. Here the schema is
 * built the way an adopter's already is — the changesets the previous release shipped — then filled
 * with rows, and only then migrated. Data present at migration time is the whole point: the backfill,
 * the {@code NOT NULL} that follows it and the new {@code CHECK} all pass trivially on an empty table
 * and are the three things that can fail on a populated one.
 */
class OptionalSeqMigrationIT extends AbstractPostgresIT {

    private static final String SCHEMA = "as_shipped_by_the_previous_release";

    /** The changesets that had already shipped, and the one that migrates them. */
    private static final String[] ALREADY_SHIPPED = {"v1-baseline.sql", "v2-replays.sql", "v3-managed-seq.sql"};
    private static final String MIGRATION = "v4-optional-seq.sql";

    @BeforeAll
    static void buildTheOldSchemaFillItAndMigrate() throws SQLException {
        runInIsolatedSchema("DROP SCHEMA IF EXISTS " + SCHEMA + " CASCADE", "CREATE SCHEMA " + SCHEMA);
        for (String shipped : ALREADY_SHIPPED) {
            runInIsolatedSchema(changesetSql(shipped));
        }
        // Two rows an application would have written before upgrading. `seq` was mandatory then, and
        // nothing recorded where the number came from - which is exactly what the migration must cope
        // with.
        runInIsolatedSchema(
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, payload)"
                        + " VALUES ('order-1', 'Order', 0, 1, '{}'::jsonb), ('order-1', 'Order', 0, 2, '{}'::jsonb)");
        runInIsolatedSchema(changesetSql(MIGRATION));
    }

    @Test
    void GIVEN_rows_written_before_the_upgrade_WHEN_the_migration_runs_THEN_they_are_recorded_as_carrying_the_application_s_own_number() {
        assertThat(queryInts("SELECT seq_source FROM tandem_outbox WHERE aggregate_id = 'order-1' ORDER BY id"))
                .containsExactly(SeqSource.APPLICATION.code(), SeqSource.APPLICATION.code());
    }

    @Test
    void GIVEN_a_migrated_database_WHEN_a_message_is_written_without_a_number_THEN_it_is_stored_with_none() {
        runInIsolatedSchema(
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, seq_source, payload)"
                        + " VALUES ('order-2', 'Order', 0, NULL, " + SeqSource.NONE.code() + ", '{}'::jsonb)");

        assertThat(queryInts("SELECT count(*) FROM tandem_outbox WHERE aggregate_id = 'order-2' AND seq IS NULL"))
                .containsExactly(1);
    }

    @Test
    void GIVEN_a_migrated_database_WHEN_a_message_claims_no_number_but_carries_one_THEN_the_database_refuses_it() {
        assertThatThrownBy(() -> runInIsolatedSchema(
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, seq_source, payload)"
                        + " VALUES ('order-3', 'Order', 0, 7, " + SeqSource.NONE.code() + ", '{}'::jsonb)"))
                .hasMessageContaining("tandem_outbox_seq_source_agrees");
    }

    @Test
    void GIVEN_a_migrated_database_WHEN_a_message_states_no_provenance_THEN_the_database_refuses_it() {
        assertThatThrownBy(() -> runInIsolatedSchema(
                "INSERT INTO tandem_outbox (aggregate_id, aggregate_type, bucket, seq, payload)"
                        + " VALUES ('order-4', 'Order', 0, 9, '{}'::jsonb)"))
                .hasMessageContaining("seq_source");
    }

    /**
     * Reads the shipped changeset and drops only Liquibase's own directive lines; every remaining
     * line, comments included, is SQL. Reading the real file rather than a copy is deliberate - a
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

    private static java.util.List<Integer> queryInts(String sql) {
        try (Connection conn = DATA_SOURCE.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("SET search_path TO " + SCHEMA);
            try (ResultSet rs = stmt.executeQuery(sql)) {
                java.util.List<Integer> out = new java.util.ArrayList<>();
                while (rs.next()) {
                    out.add(rs.getInt(1));
                }
                return out;
            }
        } catch (SQLException e) {
            throw new IllegalStateException(e.getMessage(), e);
        }
    }
}
