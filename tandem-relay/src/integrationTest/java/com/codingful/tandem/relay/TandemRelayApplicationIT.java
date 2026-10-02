package com.codingful.tandem.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.test.TandemTestContainer;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The application as it ships (LLD-relay §8.2): the built jar started as a process and configured
 * through environment variables only, against a real PostgreSQL and a real Kafka. Nothing of the
 * application is on this test's classpath; it is observed from outside, over HTTP, JDBC and Kafka,
 * which is how an operator and a consumer see it.
 *
 * <p>Every relay here runs under {@code LEASE}: several of these processes are alive at once against
 * one database, and that is the coordination mode for more than one instance.
 */
@Tag("integration")
class TandemRelayApplicationIT {

    // What an operator's manifests are written against: part of the application's contract (§7.3).
    private static final String ADMIN_SUMMARY_PATH = "/tandem/admin/v1/outbox/summary";
    private static final String ADMIN_MESSAGE_PATH = "/tandem/admin/v1/outbox/messages/";
    private static final String RELAY_CONTRIBUTOR = "tandemRelay";
    private static final String TANDEM_METER = "tandem_outbox_workers_active";

    private static final String RELAY_ENABLED_KEY = "tandem.relay.enabled";
    private static final String ADMIN_ENABLED_KEY = "tandem.admin.enabled";
    private static final String KAFKA_SOURCE_KEY = "tandem.kafka.source";

    private static final String AGGREGATE_TYPE = "Order";
    private static final String TOPIC = "order-topic";   // the default route of that aggregate type
    private static final String INSTANCE_ID = "relay-it-both-roles";
    private static final int BUCKET_COUNT = 256;
    private static final Duration GRACE_PERIOD = Duration.ofSeconds(60);
    private static final ObjectMapper JSON = new ObjectMapper();

    private static TandemTestContainer infrastructure;
    private static RelayProcess bothRoles;

    @BeforeAll
    static void startInfrastructureAndTheApplication() {
        infrastructure = new TandemTestContainer().start();
        infrastructure.createTopic(TOPIC, 1);
        Map<String, String> environment = relayEnvironment();
        environment.put("TANDEM_ADMIN_ENABLED", "true");
        environment.put("TANDEM_RELAY_INSTANCE_ID", INSTANCE_ID);
        bothRoles = RelayProcess.start(environment).awaitReady();
    }

    @AfterAll
    static void stopEverything() {
        if (bothRoles != null) {
            bothRoles.close();
        }
        if (infrastructure != null) {
            infrastructure.close();
        }
    }

    private static Map<String, String> databaseEnvironment() {
        Map<String, String> environment = new HashMap<>();
        environment.put("SPRING_DATASOURCE_URL", infrastructure.postgresJdbcUrl());
        environment.put("SPRING_DATASOURCE_USERNAME", infrastructure.postgresUsername());
        environment.put("SPRING_DATASOURCE_PASSWORD", infrastructure.postgresPassword());
        return environment;
    }

    private static Map<String, String> relayEnvironment() {
        Map<String, String> environment = databaseEnvironment();
        environment.put("TANDEM_KAFKA_SOURCE", "/tandem/relay-it");
        environment.put("TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS", infrastructure.bootstrapServers());
        environment.put("TANDEM_RELAY_COORDINATION", "LEASE");
        return environment;
    }

    private static OutboxMessage message(String aggregateId, String payload) {
        return OutboxMessage.builder()
                .aggregateType(AGGREGATE_TYPE).aggregateId(aggregateId).seq(1L)
                .payload(payload.getBytes(StandardCharsets.UTF_8))
                .contentType("application/json")
                .build();
    }

    private static JsonNode json(HttpResponse<String> response) throws IOException {
        return JSON.readTree(response.body());
    }

    // ---- both roles in one process ---------------------------------------------------------------

    @Test
    void GIVEN_the_broker_address_given_only_as_an_environment_variable_WHEN_a_row_is_written_THEN_the_relay_delivers_it()
            throws IOException {
        String aggregateId = "delivered-order";
        OutboxMessage written = message(aggregateId, "{\"id\":\"" + aggregateId + "\"}");
        infrastructure.newRepository(BUCKET_COUNT).insert(written);

        try (KafkaConsumer<String, byte[]> consumer = infrastructure.newConsumer("relay-it-delivery", TOPIC)) {
            ConsumerRecord<String, byte[]> delivered = firstRecordOf(consumer, aggregateId, Duration.ofSeconds(60));

            assertThat(delivered).as("a record for %s on %s", aggregateId, TOPIC).isNotNull();
            // Compared as documents: PostgreSQL stores the payload as jsonb and renders it in its own
            // spacing, so the bytes on the wire are the same JSON, not the same bytes.
            assertThat(JSON.readTree(delivered.value())).isEqualTo(JSON.readTree(written.payload()));
        }
    }

    @Test
    void GIVEN_a_running_relay_WHEN_the_readiness_probe_is_read_THEN_it_is_up_and_identifies_the_instance()
            throws IOException {
        HttpResponse<String> readiness = bothRoles.management(RelayProcess.READINESS_PATH);

        assertThat(readiness.statusCode()).isEqualTo(200);
        JsonNode relay = json(readiness).path("components").path(RELAY_CONTRIBUTOR);
        assertThat(relay.path("status").asText()).isEqualTo("UP");
        assertThat(relay.path("details").path("instanceId").asText()).isEqualTo(INSTANCE_ID);
    }

    @Test
    void GIVEN_a_running_relay_WHEN_the_metrics_are_scraped_THEN_tandems_own_meters_are_there() {
        HttpResponse<String> scrape = bothRoles.management("/actuator/prometheus");

        assertThat(scrape.statusCode()).isEqualTo(200);
        assertThat(scrape.body()).contains(TANDEM_METER);
    }

    @Test
    void GIVEN_the_admin_api_enabled_WHEN_a_stored_message_is_read_THEN_its_payload_is_rendered_as_the_json_it_holds()
            throws IOException, SQLException {
        String aggregateId = "inspected-order";
        String payload = "{\"id\":\"" + aggregateId + "\",\"lines\":[1,2]}";
        infrastructure.newRepository(BUCKET_COUNT).insert(message(aggregateId, payload));

        HttpResponse<String> summary = bothRoles.application(ADMIN_SUMMARY_PATH);
        HttpResponse<String> detail = bothRoles.application(ADMIN_MESSAGE_PATH + rowIdOf(aggregateId));

        assertThat(summary.statusCode()).isEqualTo(200);
        assertThat(detail.statusCode()).isEqualTo(200);
        assertThat(json(detail).path("payload")).isEqualTo(JSON.readTree(payload));
    }

    @Test
    void GIVEN_the_two_ports_WHEN_each_is_asked_for_what_the_other_serves_THEN_neither_answers() {
        assertThat(bothRoles.application(RelayProcess.READINESS_PATH).statusCode()).isEqualTo(404);
        assertThat(bothRoles.management(ADMIN_SUMMARY_PATH).statusCode()).isEqualTo(404);
    }

    @Test
    void GIVEN_a_running_application_WHEN_its_info_is_read_THEN_it_names_the_library_release_it_contains()
            throws IOException {
        JsonNode build = json(bothRoles.management("/actuator/info")).path("build");

        assertThat(build.path("version").asText()).isNotBlank();
        assertThat(build.path("tandem").path("library").asText()).isEqualTo(System.getProperty("tandem.relay.pin"));
    }

    // ---- one role, or a configuration that must be refused ---------------------------------------

    @Test
    void GIVEN_only_the_relay_role_WHEN_the_admin_api_is_called_THEN_no_route_answers() {
        try (RelayProcess relayOnly = RelayProcess.start(relayEnvironment()).awaitReady()) {
            assertThat(relayOnly.application(ADMIN_SUMMARY_PATH).statusCode()).isEqualTo(404);
        }
    }

    @Test
    void GIVEN_only_the_admin_api_role_and_no_kafka_setting_WHEN_the_application_starts_THEN_it_serves_the_api_and_reports_no_relay()
            throws IOException {
        Map<String, String> environment = databaseEnvironment();
        environment.put("TANDEM_RELAY_ENABLED", "false");
        environment.put("TANDEM_ADMIN_ENABLED", "true");

        try (RelayProcess adminOnly = RelayProcess.start(environment).awaitReady()) {
            assertThat(adminOnly.application(ADMIN_SUMMARY_PATH).statusCode()).isEqualTo(200);
            JsonNode readiness = json(adminOnly.management(RelayProcess.READINESS_PATH));
            assertThat(readiness.path("status").asText()).isEqualTo("UP");
            assertThat(readiness.path("components").path(RELAY_CONTRIBUTOR).path("status").asText())
                    .isEqualTo("UNKNOWN");
        }
    }

    @Test
    void GIVEN_a_running_relay_WHEN_it_is_asked_to_stop_THEN_it_exits_by_itself_within_the_grace_period() {
        try (RelayProcess relay = RelayProcess.start(relayEnvironment()).awaitReady()) {
            relay.requestStop();

            // 143 is how a JVM reports having ended on SIGTERM; a kill for overrunning would be 137.
            assertThat(relay.awaitExit(GRACE_PERIOD)).isEqualTo(143);
        }
    }

    @Test
    void GIVEN_no_cloudevents_source_WHEN_the_application_starts_THEN_it_exits_naming_the_missing_setting() {
        Map<String, String> environment = relayEnvironment();
        environment.remove("TANDEM_KAFKA_SOURCE");

        try (RelayProcess refused = RelayProcess.start(environment)) {
            assertThat(refused.awaitExit(GRACE_PERIOD)).isNotZero();
            assertThat(refused.output()).contains(KAFKA_SOURCE_KEY);
        }
    }

    @Test
    void GIVEN_neither_role_enabled_WHEN_the_application_starts_THEN_it_exits_naming_both_settings() {
        Map<String, String> environment = databaseEnvironment();
        environment.put("TANDEM_RELAY_ENABLED", "false");

        try (RelayProcess refused = RelayProcess.start(environment)) {
            assertThat(refused.awaitExit(GRACE_PERIOD)).isNotZero();
            assertThat(refused.output()).contains(RELAY_ENABLED_KEY).contains(ADMIN_ENABLED_KEY);
        }
    }

    // ---- helpers ---------------------------------------------------------------------------------

    private static ConsumerRecord<String, byte[]> firstRecordOf(KafkaConsumer<String, byte[]> consumer,
            String aggregateId, Duration timeout) {
        long deadline = System.nanoTime() + timeout.toNanos();
        while (System.nanoTime() < deadline) {
            for (ConsumerRecord<String, byte[]> record : consumer.poll(Duration.ofMillis(200))) {
                if (aggregateId.equals(record.key())) {
                    return record;
                }
            }
        }
        return null;
    }

    private static long rowIdOf(String aggregateId) throws SQLException {
        try (Connection connection = infrastructure.dataSource().getConnection();
                PreparedStatement statement =
                        connection.prepareStatement("SELECT id FROM tandem_outbox WHERE aggregate_id = ?")) {
            statement.setString(1, aggregateId);
            try (ResultSet rows = statement.executeQuery()) {
                assertThat(rows.next()).as("an outbox row for %s", aggregateId).isTrue();
                return rows.getLong("id");
            }
        }
    }
}
