package com.codingful.tandem.sample.jpa;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.test.TandemTestContainer;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.env.Environment;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.annotation.DirtiesContext;

/** Runs the actual demo runner and autoconfigured relay, then independently reads the topic. */
@Tag("integration")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.AFTER_CLASS)
class JpaSampleSmokeIT {
    @Autowired
    private TandemTestContainer infrastructure;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private Environment environment;

    @Test
    void GIVEN_the_completed_demo_WHEN_the_operator_inspects_the_failed_event_THEN_the_admin_api_exposes_it() throws Exception {
        long id = jdbc.queryForObject("SELECT id FROM tandem_outbox WHERE aggregate_id = ?", Long.class, SampleRunner.ROLLBACK);
        var request = HttpRequest.newBuilder(URI.create(
                "http://localhost:" + environment.getRequiredProperty("local.server.port")
                        + "/tandem/admin/v1/outbox/messages/" + id)).GET().build();
        var response = HttpClient.newHttpClient().send(request, HttpResponse.BodyHandlers.ofString());
        assertThat(response.statusCode()).isEqualTo(200);
        assertThat(response.body()).contains(SampleRunner.ROLLBACK, "FAILED");
    }

    @Test
    @Timeout(75)
    void GIVEN_the_jpa_demo_WHEN_concurrent_commands_commit_THEN_kafka_preserves_each_stream_and_excludes_rolled_back_events() {
        Map<String, List<Long>> delivered = new LinkedHashMap<>();
        var seen = new HashSet<String>();
        try (var consumer = infrastructure.newConsumer("jpa-smoke-" + UUID.randomUUID(), DemoContainerInitializer.TOPIC)) {
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            while (seen.size() < 10 && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    String id = new String(record.headers().lastHeader(CloudEventsHeaders.CE_ID).value(),
                            StandardCharsets.UTF_8);
                    if (!seen.add(id)) continue;
                    long seq = Long.parseLong(new String(record.headers().lastHeader(CloudEventsHeaders.CE_SEQ).value(),
                            StandardCharsets.UTF_8));
                    delivered.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(seq);
                }
            }
        }
        assertThat(delivered).containsOnlyKeys(SampleRunner.SINGLE, SampleRunner.LEFT, SampleRunner.RIGHT,
                SampleRunner.VERSIONED, SampleRunner.ROLLBACK);
        assertThat(delivered.get(SampleRunner.SINGLE)).containsExactly(1L, 2L, 3L, 4L);
        assertThat(delivered.get(SampleRunner.LEFT)).containsExactly(1L, 2L);
        assertThat(delivered.get(SampleRunner.RIGHT)).containsExactly(1L, 2L);
        assertThat(delivered.get(SampleRunner.VERSIONED)).containsExactly(1L);
        assertThat(delivered.get(SampleRunner.ROLLBACK)).containsExactly(1L);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sample_order_note WHERE order_id = ?",
                Long.class, SampleRunner.ROLLBACK)).isEqualTo(1L);
        assertThat(jdbc.queryForObject("SELECT event_sequence FROM sample_order WHERE id = ?",
                Long.class, SampleRunner.ROLLBACK)).isEqualTo(1L);
    }
}
