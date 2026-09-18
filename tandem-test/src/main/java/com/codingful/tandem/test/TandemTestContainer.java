package com.codingful.tandem.test;

import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaRelay;
import com.codingful.tandem.kafka.KafkaRelayConfig;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import javax.sql.DataSource;
import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Integration-test harness (LLD-test §4): starts a real <b>PostgreSQL</b> and a real <b>Kafka</b>
 * (KRaft) container, applies the committed baseline DDL, and exposes the {@link DataSource} +
 * bootstrap servers plus convenience factories for the {@link JdbcOutboxRepository}, the
 * {@link WorkerPool} relay and a Kafka consumer — so an end-to-end test can insert, run the relay,
 * and assert the CloudEvent landed on the topic in per-aggregate order.
 *
 * <p>Closing it ({@code try-with-resources}) stops the relays, consumers and both containers.
 */
@SuppressWarnings({"deprecation", "resource"})  // deprecation: legacy KafkaContainer+withKraft; resource: close() manages both containers
public final class TandemTestContainer implements AutoCloseable {

    /** The PostgreSQL image every integration test runs against unless one is named explicitly. */
    public static final String DEFAULT_POSTGRES_IMAGE = "postgres:16-alpine";

    /**
     * System property naming the PostgreSQL image to start instead of {@link #DEFAULT_POSTGRES_IMAGE};
     * {@value} as an environment variable is {@code TANDEM_TEST_POSTGRES_IMAGE}.
     */
    public static final String POSTGRES_IMAGE_PROPERTY = "tandem.test.postgres.image";

    private static final String POSTGRES_IMAGE_ENV = "TANDEM_TEST_POSTGRES_IMAGE";

    private final PostgreSQLContainer<?> postgres = newPostgresContainer();
    private final KafkaContainer kafka =
            new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.6.1")).withKraft();

    private final List<KafkaRelay> relays = new ArrayList<>();
    private final List<KafkaConsumer<?, ?>> consumers = new ArrayList<>();
    private DataSource dataSource;

    /**
     * The PostgreSQL image the harness starts: the {@value #POSTGRES_IMAGE_PROPERTY} system property,
     * else the {@code TANDEM_TEST_POSTGRES_IMAGE} environment variable, else
     * {@link #DEFAULT_POSTGRES_IMAGE}. The override is what lets one run of the same suite verify a
     * different PostgreSQL major, which is how the supported-version claim is kept honest
     * (guide/compatibility.md); it also lets an adopter point the harness at the image their own
     * production runs.
     *
     * @return the image coordinate to start, never {@code null} or blank
     */
    public static String postgresImage() {
        String configured = System.getProperty(POSTGRES_IMAGE_PROPERTY);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv(POSTGRES_IMAGE_ENV);
        }
        return configured == null || configured.isBlank() ? DEFAULT_POSTGRES_IMAGE : configured;
    }

    /**
     * A PostgreSQL container on {@link #postgresImage()}, ready to start. Shared with
     * {@code tandem-jdbc}'s integration base class so both suites land on the same engine.
     *
     * @return a configured, not-yet-started container
     */
    public static PostgreSQLContainer<?> newPostgresContainer() {
        // asCompatibleSubstituteFor: Testcontainers refuses an image not named `postgres` unless told
        // it speaks the same protocol, which is what lets a run be pointed at a vendor's own
        // distribution rather than only at another major.
        return new PostgreSQLContainer<>(
                DockerImageName.parse(postgresImage()).asCompatibleSubstituteFor("postgres"));
    }

    /** Start both containers and apply the baseline schema. */
    public TandemTestContainer start() {
        postgres.start();
        kafka.start();
        this.dataSource = new DriverManagerDataSource(
                postgres.getJdbcUrl(), postgres.getUsername(), postgres.getPassword());
        applyBaselineSchema();
        return this;
    }

    /** The running Postgres container's {@link DataSource}; valid only after {@link #start()}. */
    public DataSource dataSource() {
        return dataSource;
    }

    /** Bootstrap servers of the running Kafka container, reachable from the host. */
    public String bootstrapServers() {
        return kafka.getBootstrapServers();
    }

    /** JDBC URL of the running Postgres container, reachable from the host (e.g. for an external SQL client). */
    public String postgresJdbcUrl() {
        return postgres.getJdbcUrl();
    }

    /** Username for the running Postgres container. */
    public String postgresUsername() {
        return postgres.getUsername();
    }

    /** Password for the running Postgres container. */
    public String postgresPassword() {
        return postgres.getPassword();
    }

    /** A {@link JdbcOutboxRepository} wired to this container's Postgres — the write-side entry point. */
    public JdbcOutboxRepository newRepository(int bucketCount) {
        // Write-side bucket-count guard (LLD-bucket-count-guard §7): an explicit assembly step against
        // this container's plain DataSource. The repository constructor deliberately does no I/O (its
        // DataSource may be transaction-scoped), so the assembly runs the guard, mirroring how
        // tandem-spring-producer will run it against the raw DataSource bean at startup.
        BucketCountGuard.check(dataSource, bucketCount);
        return new JdbcOutboxRepository(dataSource, bucketCount);
    }

    /** A {@link JdbcOutboxStore} wired to this container's Postgres — relay-side persistence. */
    public JdbcOutboxStore newStore(int maxAttempts) {
        return new JdbcOutboxStore(dataSource, maxAttempts);
    }

    /**
     * A relay wired end-to-end: a {@link JdbcOutboxStore} polling Postgres and a {@link KafkaRelay}
     * publishing to this broker. The dispatcher is tracked and closed when this container closes;
     * the caller {@code start()}s and {@code stop()}s the returned pool.
     */
    public WorkerPool newRelay(RelayConfig cfg, TopicRouter router, KafkaRelayConfig kafkaCfg) {
        // Relay-side bucket-count guard (LLD-bucket-count-guard §7): an explicit assembly step, since
        // WorkerPool is port-only (no DataSource). Fails fast if this relay's bucketCount differs from
        // the value the write-side established. Mirrors how tandem-spring-relay will wire it in autoconfig.
        BucketCountGuard.check(dataSource, cfg.bucketCount());
        OutboxStore store = newStore(cfg.maxAttempts());
        KafkaRelay relay = new KafkaRelay(producerConfig(), router, kafkaCfg);
        relays.add(relay);
        return new WorkerPool(store, relay, cfg);
    }

    /** Create a topic with the given partition count (so per-aggregate keys distribute across partitions). */
    public void createTopic(String topic, int partitions) {
        Map<String, Object> config = Map.of(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        try (Admin admin = Admin.create(config)) {
            admin.createTopics(List.of(new NewTopic(topic, partitions, (short) 1))).all().get();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("interrupted creating topic " + topic, e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("failed to create topic " + topic, e);
        }
    }

    /** A consumer reading from the beginning, subscribed to {@code topics}. Closed when this container closes. */
    public KafkaConsumer<String, byte[]> newConsumer(String groupId, String... topics) {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        config.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        KafkaConsumer<String, byte[]> consumer =
                new KafkaConsumer<>(config, new StringDeserializer(), new ByteArrayDeserializer());
        consumer.subscribe(List.of(topics));
        consumers.add(consumer);
        return consumer;
    }

    /** Minimal Kafka producer config (just {@code bootstrap.servers}) pointing at this container's broker. */
    public Map<String, Object> producerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers());
        return config;
    }

    /** Closes every tracked consumer and relay, then stops both containers. */
    @Override
    public void close() {
        for (KafkaConsumer<?, ?> consumer : consumers) {
            consumer.close();
        }
        for (KafkaRelay relay : relays) {
            relay.close();
        }
        kafka.stop();
        postgres.stop();
    }

    private void applyBaselineSchema() {
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
