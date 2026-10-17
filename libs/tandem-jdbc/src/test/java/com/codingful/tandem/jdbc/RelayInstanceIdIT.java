package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.test.RecordingDispatcher;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import org.junit.jupiter.api.Test;

/**
 * The instance id a relay accepts is one it can run under: every name {@link RelayConfig} takes, the
 * longest included (a Kubernetes pod name reaches 63 characters), must still let its workers claim rows.
 */
class RelayInstanceIdIT extends AbstractPostgresIT {

    private static final int BUCKETS = 256;
    private static final String LONGEST_INSTANCE_ID = "r".repeat(64);

    @Test
    void GIVEN_the_longest_instance_id_the_relay_accepts_WHEN_it_runs_THEN_it_delivers_the_outbox() throws InterruptedException {
        new JdbcOutboxRepository(DATA_SOURCE, BUCKETS).insert(OutboxMessage.builder()
                .aggregateId("order-1").aggregateType("Order").unsequenced()
                .payload("{}".getBytes(StandardCharsets.UTF_8)).build());
        RecordingDispatcher dispatcher = new RecordingDispatcher();
        RelayConfig cfg = RelayConfig.builder().bucketCount(BUCKETS).instanceId(LONGEST_INSTANCE_ID)
                .workersPerInstance(1).pollInterval(Duration.ofMillis(20)).build();
        WorkerPool relay = new WorkerPool(new JdbcOutboxStore(DATA_SOURCE, 10), dispatcher, cfg);

        relay.start();
        try {
            long deadline = System.nanoTime() + Duration.ofSeconds(10).toNanos();
            while (dispatcher.dispatchCount() == 0 && System.nanoTime() < deadline) {
                Thread.sleep(20);
            }
        } finally {
            relay.stop();
        }

        assertThat(dispatcher.dispatchCount()).as("events delivered").isEqualTo(1);
    }
}
