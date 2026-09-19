package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.jdbc.BackoffStrategy;
import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.RelayControlSource;
import com.codingful.tandem.jdbc.WakeupSource;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.test.TandemTestContainer;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Clock;
import javax.sql.DataSource;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.testcontainers.containers.PostgreSQLContainer;

/**
 * Dedicated container + wiring environment for the benchmark (LLD-benchmark §3) — <b>not</b>
 * {@code TandemTestContainer.newRelay}: that helper's {@code DataSource} opens a fresh, unpooled
 * connection per call, which cannot bound the driver's real concurrency (LLD-benchmark §4.2). This
 * class wires a Hikari-pooled {@link DataSource} and assembles the relay directly, with a
 * {@link FaultInjector} seam for S6.
 *
 * <p>It owns the database; the broker is a {@link BrokerHarness} chosen per run (§3.1), which is why
 * the containers are started separately rather than through {@code TandemTestContainer}: that helper
 * always starts Kafka, including for a run that publishes to RabbitMQ.
 */
public final class BenchmarkEnvironment implements AutoCloseable {

    private final BenchmarkConfig config;
    private final PostgreSQLContainer<?> postgres = TandemTestContainer.newPostgresContainer();
    private final BrokerHarness broker;
    private final FaultInjector faultInjector = new FaultInjector();

    private HikariDataSource dataSource;
    private JdbcOutboxStore store;
    private WorkerPool relayPool;
    private BenchmarkMetrics metrics;
    private LagProbe lagProbe;

    public BenchmarkEnvironment(BenchmarkConfig config) {
        this.config = config;
        this.broker = BrokerHarness.forBroker(config.broker(), config);
    }

    public BenchmarkEnvironment start() {
        postgres.start();
        dataSource = pooledDataSource();
        BaselineSchema.applyTo(dataSource);
        applyBenchSchema();
        broker.start();

        // Bucket-count guard (LLD-bucket-count-guard §7): the explicit assembly-level startup check,
        // run once against the plain pooled DataSource. The write-side (LoadGenerator's repository) and
        // every relay in this environment share config.bucketCount(), so seeding it here establishes the
        // value the whole run agrees on — mirroring how a real assembly runs the guard at startup.
        BucketCountGuard.check(dataSource, config.bucketCount());

        store = new JdbcOutboxStore(dataSource, config.maxAttempts());
        metrics = new BenchmarkMetrics();
        lagProbe = new LagProbe(dataSource);

        OutboxDispatcher dispatcher =
                new FaultInjectingDispatcher(broker.newDispatcher(TandemSpanRecorder.NOOP), faultInjector);

        // relayConfigBuilder() defaults coordination to RelayConfig's own default (SINGLE), matching
        // every scenario except S8 (which builds its own LEASE-coordinated instances via
        // newRelayInstance instead of this primary pool).
        RelayConfig relayCfg = relayConfigBuilder().build();
        relayPool = new WorkerPool(store, dispatcher, relayCfg, metrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), BucketSource.forCoordination(relayCfg, dataSource),
                RelayControlSource.NOOP, newWakeupSource());
        return this;
    }

    /**
     * A {@link RelayConfig.Builder} pre-populated from this environment's {@link BenchmarkConfig}
     * (bucket count, workers, poll interval, batch size, row lease, max attempts, the real delivery
     * timeout) — the shared sizing every relay instance in this environment uses. S8 layers
     * {@code .coordination(LEASE).instanceId(...)} on top to build its own additional instances.
     */
    public RelayConfig.Builder relayConfigBuilder() {
        return relayConfigBuilder(config, broker.deliveryTimeoutMillis());
    }

    /**
     * The mapping itself, taking the producer's resolved {@code deliveryTimeoutMs} rather than reading
     * it off a started environment: it is a pure translation of the harness's knobs into the relay's,
     * and every knob that fails to make the crossing does so silently — the run is simply sized as if
     * the flag had never been passed.
     */
    static RelayConfig.Builder relayConfigBuilder(BenchmarkConfig config, long deliveryTimeoutMs) {
        return RelayConfig.builder()
                .bucketCount(config.bucketCount())
                .workersPerInstance(config.workers())
                .pollInterval(config.pollInterval())
                .pollIntervalFloor(config.pollIntervalFloor())
                .batchSize(config.batchSize())
                .rowLease(config.rowLease())
                .maxAttempts(config.maxAttempts())
                .retention(config.retention())
                .cleanupInterval(config.cleanupInterval())
                .cleanupBatchSize(config.cleanupBatchSize())
                .deliveryTimeoutMs(deliveryTimeoutMs);
    }

    /**
     * Builds an additional, independent relay instance (its own connection to the broker, its own
     * {@link BucketSource} per {@code relayCfg.coordination()}), sharing this environment's
     * {@code DataSource}/{@code JdbcOutboxStore} (as real instances share one DB). For scenarios
     * simulating more than one relay instance against one outbox (S8: {@code LEASE} coordination,
     * HLD §3.2 axis 2 — the "N embedded replicas" case a naive {@code SINGLE} deployment gets wrong).
     * The caller {@code start()}s/{@code stop()}s the returned pool; this environment closes the
     * producer on {@link #close()}.
     */
    public RelayInstance newRelayInstance(RelayConfig relayCfg) {
        return newRelayInstance(relayCfg, metrics);
    }

    /**
     * A source for this environment's configured {@link com.codingful.tandem.jdbc.Wakeup} mode, on this
     * environment's {@code DataSource}. A new one per relay instance, deliberately: each holds its own
     * listening connection, exactly as separate relay processes would.
     */
    public WakeupSource newWakeupSource() {
        return WakeupSource.forWakeup(config.wakeup(), dataSource);
    }

    /**
     * As {@link #newRelayInstance(RelayConfig)}, but reporting to {@code instanceMetrics} instead of the
     * environment's shared {@link BenchmarkMetrics}. Needed whenever the instances must be told apart in
     * the metrics themselves: several of the port's signals are per-instance ({@code workers.active}),
     * so one shared sink collapses them to whichever instance reported last (§6.3).
     */
    public RelayInstance newRelayInstance(RelayConfig relayCfg, TandemMetrics instanceMetrics) {
        return newRelayInstance(relayCfg, instanceMetrics, TandemSpanRecorder.NOOP);
    }

    /**
     * As {@link #newRelayInstance(RelayConfig, TandemMetrics)}, but also wiring {@code spanRecorder}
     * into the instance's producer — {@link TracingDashboardDemo}'s one relay instance is the only
     * caller that passes a real one (§6.4); every other scenario gets the free default.
     */
    public RelayInstance newRelayInstance(RelayConfig relayCfg, TandemMetrics instanceMetrics,
            TandemSpanRecorder spanRecorder) {
        return newRelayInstance(relayCfg, instanceMetrics, spanRecorder, newWakeupSource());
    }

    /**
     * As {@link #newRelayInstance(RelayConfig, TandemMetrics, TandemSpanRecorder)}, but with an explicit
     * {@code wakeupSource} instead of this environment's configured one — for S10, whose whole subject is
     * running one arm with a wakeup and one without inside a single run.
     */
    public RelayInstance newRelayInstance(RelayConfig relayCfg, TandemMetrics instanceMetrics,
            TandemSpanRecorder spanRecorder, WakeupSource wakeupSource) {
        OutboxDispatcher producer = broker.newDispatcher(spanRecorder);
        OutboxDispatcher dispatcher = new FaultInjectingDispatcher(producer, faultInjector);
        // Wrapped here, not on the shared `store` field: only an instance built through this method can
        // ever have its claims stalled (S9), so env.relayPool() and every S1-S8 scenario built from
        // env.store() directly are untouched — the wrapper is a no-op pass-through until a scenario
        // calls FaultInjector.stallWorkerClaims for this instance's id.
        OutboxStore faultInjectingStore = new FaultInjectingOutboxStore(store, faultInjector);
        BucketSource bucketSource = BucketSource.forCoordination(relayCfg, dataSource);
        WorkerPool pool = new WorkerPool(faultInjectingStore, dispatcher, relayCfg, instanceMetrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), bucketSource, RelayControlSource.NOOP, wakeupSource);
        return new RelayInstance(pool, bucketSource, producer);
    }

    public BenchmarkConfig config() {
        return config;
    }

    /** The pooled write/relay {@link DataSource}. */
    public DataSource dataSource() {
        return dataSource;
    }

    public JdbcOutboxStore store() {
        return store;
    }

    /** Caller {@code start()}s/{@code stop()}s it per scenario. */
    public WorkerPool relayPool() {
        return relayPool;
    }

    public BenchmarkMetrics metrics() {
        return metrics;
    }

    public LagProbe lagProbe() {
        return lagProbe;
    }

    public FaultInjector faultInjector() {
        return faultInjector;
    }

    /** The broker half of this environment, for the scenarios that act on the broker itself (S12). */
    public BrokerHarness broker() {
        return broker;
    }

    /**
     * Empties every table the scenarios write to, so each one starts from the state the first one
     * found (LLD-benchmark §8).
     *
     * <p>Scenarios share one environment, and sharing it turned out to mean sharing its
     * <i>contents</i>: a scenario that ends with a backlog — S4 saturates on purpose, and on a small
     * host cannot always work it off inside its window — leaves those rows for whatever runs next,
     * which then measures its own load plus the leftovers. That made results order-dependent, and it
     * stayed hidden while an exception from one scenario aborted the whole batch; isolating the
     * exceptions is what exposed it. Isolating the exception is not the same as isolating the state.
     *
     * <p>The broker is reset the same way and for the same reason, as far as it can be: what one
     * scenario left undelivered is not the next one's traffic ({@link BrokerHarness#resetBetweenScenarios}).
     *
     * <p>Called between scenarios, with every relay pool already stopped. {@code bench_aggregate} is
     * included because its version counters would otherwise keep climbing across scenarios that
     * address the same synthetic aggregates.
     *
     * <p><b>{@code tandem_bucket_lease} is cleared, never truncated.</b> Its rows are not runtime
     * state: the baseline DDL seeds exactly one per virtual bucket, and a {@code LEASE} relay refuses
     * to start unless it finds {@code bucketCount} of them — so emptying the table makes the
     * multi-instance scenario fail at startup with a configuration error rather than run. Only the
     * ownership columns are reset, which is what "no one owns anything yet" actually means here.
     */
    public void resetBetweenScenarios() {
        broker.resetBetweenScenarios();
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE TABLE tandem_outbox, bench_aggregate");
            stmt.execute("UPDATE tandem_bucket_lease "
                    + "SET owner = NULL, lease_until = NULL, paused = false, updated_at = now()");
        } catch (SQLException e) {
            throw new IllegalStateException("resetting the environment between scenarios failed", e);
        }
    }

    /** A receiver over everything this run publishes, whatever the broker. Caller closes it. */
    public EventReceiver newReceiver(String group) {
        return broker.newReceiver(group);
    }

    /**
     * The raw Kafka consumer, for the tracing demo alone: its consumer-side span bridge reads
     * {@code traceparent} off Kafka's own record, which is a Kafka artefact and not something the
     * harness's neutral {@link ReceivedEvent} carries. Caller closes it.
     *
     * @throws IllegalStateException if this run is not against Kafka
     */
    public KafkaConsumer<String, byte[]> newKafkaConsumer(String group) {
        if (!(broker instanceof KafkaBrokerHarness kafka)) {
            throw new IllegalStateException("The tracing demo reads Kafka records directly; "
                    + "it needs --broker=kafka, got broker:" + config.broker());
        }
        return kafka.newKafkaConsumer(group);
    }

    private HikariDataSource pooledDataSource() {
        HikariConfig hikari = new HikariConfig();
        hikari.setJdbcUrl(postgres.getJdbcUrl());
        hikari.setUsername(postgres.getUsername());
        hikari.setPassword(postgres.getPassword());
        hikari.setMaximumPoolSize(config.maxConnections());
        hikari.setPoolName("tandem-benchmark");
        return new HikariDataSource(hikari);
    }

    private void applyBenchSchema() {
        try (InputStream ddlStream = getClass().getResourceAsStream("/bench-schema.sql")) {
            if (ddlStream == null) {
                throw new IllegalStateException("bench-schema.sql not found on classpath");
            }
            String ddl = new String(ddlStream.readAllBytes(), StandardCharsets.UTF_8);
            try (Connection conn = dataSource.getConnection();
                 Statement stmt = conn.createStatement()) {
                stmt.execute(ddl);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("applying bench schema failed", e);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        if (relayPool != null) {
            relayPool.stop();
        }
        broker.close();
        if (dataSource != null) {
            dataSource.close();
        }
        postgres.stop();
    }
}
