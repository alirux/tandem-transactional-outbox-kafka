package com.codingful.tandem.spring.relay;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.jdbc.BackoffStrategy;
import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.JdbcRelayControlSource;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.RelayControlSource;
import com.codingful.tandem.jdbc.WakeupSource;
import com.codingful.tandem.jdbc.WorkerPool;
import io.micrometer.tracing.propagation.Propagator;
import java.time.Clock;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.autoconfigure.condition.ConditionalOnSingleCandidate;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Relay autoconfiguration (LLD-spring-config §4.4): contributes the relay engine — topic router, Kafka
 * dispatcher, outbox store, bucket source and {@link WorkerPool} — and a {@link RelayLifecycle} that
 * starts and stops it with the application. Ordered after Spring Boot's own
 * {@code DataSourceAutoConfiguration} and gated on a single {@code DataSource} candidate; the whole
 * configuration is conditional on {@code tandem.relay.enabled} (default true), the supported way to load
 * the module without running a relay. Every bean is {@link ConditionalOnMissingBean}, so an application
 * can replace any piece — most usefully a custom {@link TopicRouter}, or a
 * {@link MessageEncoder} that publishes an envelope other than CloudEvents.
 *
 * <p>The ordering is declared by <b>name</b> for both generations: Boot 4 moved
 * {@code DataSourceAutoConfiguration} into {@code spring-boot-jdbc} and every tracing autoconfiguration
 * out of {@code spring-boot-actuator-autoconfigure}, so a class literal would name a type absent there
 * and the ordering would be silently lost (LLD-spring-config §1.1). The tracing entries matter because
 * {@link #tandemSpanRecorder} is conditional on the {@code Propagator} bean they contribute: evaluated
 * too early, the condition sees none and the relay silently emits no publish span.
 */
@AutoConfiguration(afterName = {
        // Spring Boot 3.x
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.tracing.MicrometerTracingAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.tracing.BraveAutoConfiguration",
        "org.springframework.boot.actuate.autoconfigure.tracing.OpenTelemetryAutoConfiguration",
        // Spring Boot 4.x — relocated into spring-boot-jdbc and
        // spring-boot-micrometer-tracing{,-brave,-opentelemetry}
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration",
        "org.springframework.boot.micrometer.tracing.autoconfigure.MicrometerTracingAutoConfiguration",
        "org.springframework.boot.micrometer.tracing.brave.autoconfigure.BraveAutoConfiguration",
        "org.springframework.boot.micrometer.tracing.opentelemetry.autoconfigure.OpenTelemetryTracingAutoConfiguration"})
@ConditionalOnSingleCandidate(DataSource.class)
@ConditionalOnProperty(prefix = "tandem.relay", name = "enabled", matchIfMissing = true)
@EnableConfigurationProperties({TandemOutboxProperties.class, TandemRelayProperties.class,
        TandemTracingProperties.class})
public class TandemRelayAutoConfiguration {

    @Bean
    @ConditionalOnMissingBean
    RelayConfig tandemRelayConfig(TandemOutboxProperties outbox, TandemRelayProperties relay) {
        return buildRelayConfig(outbox, relay);
    }

    /**
     * Map the bound properties onto a {@link RelayConfig}. Only properties the user set override
     * {@code RelayConfig}'s own defaults, so the config object stays the single source of truth
     * (LLD-spring-config §2.2/§4.2). Package-private so it is unit-testable without a context.
     */
    static RelayConfig buildRelayConfig(TandemOutboxProperties outbox, TandemRelayProperties relay) {
        RelayConfig.Builder builder = RelayConfig.builder().bucketCount(outbox.bucketCount());
        // Only override RelayConfig's own defaults for properties the user actually set, so the config
        // object stays the single source of truth for defaults (LLD-spring-config §2.2/§4.2).
        if (relay.coordination() != null) {
            builder.coordination(relay.coordination());
        }
        if (relay.instanceId() != null) {
            builder.instanceId(relay.instanceId());
        }
        if (relay.bucketLease() != null) {
            builder.bucketLease(relay.bucketLease());
        }
        if (relay.workersPerInstance() != null) {
            builder.workersPerInstance(relay.workersPerInstance());
        }
        if (relay.pollInterval() != null) {
            builder.pollInterval(relay.pollInterval());
        }
        if (relay.pollIntervalFloor() != null) {
            builder.pollIntervalFloor(relay.pollIntervalFloor());
        }
        if (relay.pollBackoffFactor() != null) {
            builder.pollBackoffFactor(relay.pollBackoffFactor());
        }
        if (relay.batchSize() != null) {
            builder.batchSize(relay.batchSize());
        }
        if (relay.rowLease() != null) {
            builder.rowLease(relay.rowLease());
        }
        if (relay.maxAttempts() != null) {
            builder.maxAttempts(relay.maxAttempts());
        }
        if (relay.retention() != null) {
            builder.retention(relay.retention());
        }
        if (relay.cleanupBatchSize() != null) {
            builder.cleanupBatchSize(relay.cleanupBatchSize());
        }
        if (relay.reclaimInterval() != null) {
            builder.reclaimInterval(relay.reclaimInterval());
        }
        if (relay.cleanupInterval() != null) {
            builder.cleanupInterval(relay.cleanupInterval());
        }
        if (relay.metricsInterval() != null) {
            builder.metricsInterval(relay.metricsInterval());
        }
        if (relay.logEveryRows() != null) {
            builder.logEveryRows(relay.logEveryRows());
        }
        if (relay.orderViolationDetection() != null) {
            builder.orderViolationDetection(relay.orderViolationDetection());
        }
        return builder.build();
    }

    /**
     * Instrumented mode (HLD-tracing.md §6): the {@code tandem.relay.publish} span emitter, contributed
     * only when the application runs Micrometer Tracing <b>and</b> asks for it explicitly — a tracing
     * library on the classpath never turns span emission on by itself (§9). Absent, the publish adapter
     * falls back to {@link TandemSpanRecorder#NOOP} and only propagation mode is in force.
     *
     * <p>The conditions sit on the bean method, not a nested {@code @Configuration} that Spring Boot 4
     * would not process (LLD-spring-config §1.1). Spring reads them from ASM metadata, so naming the
     * Micrometer type there is safe when the library is absent — but it still reflects over every method
     * of this class, so the type must not appear in a method's <b>erased signature</b>. Hence
     * {@link ObjectProvider}, whose generic argument erasure removes: a bare {@code Propagator} parameter
     * would fail every application without Micrometer Tracing with a {@code NoClassDefFoundError}, before
     * any condition could back the bean off.
     */
    @Bean
    @ConditionalOnClass(Propagator.class)
    @ConditionalOnBean(Propagator.class)
    @ConditionalOnMissingBean
    @ConditionalOnProperty(prefix = "tandem.tracing", name = "publish-span", havingValue = "true")
    TandemSpanRecorder tandemSpanRecorder(ObjectProvider<Propagator> propagator) {
        return new MicrometerTandemSpanRecorder(propagator.getObject());
    }

    /**
     * Fails with a message naming the fix when nothing contributed a publish adapter: neither
     * {@link TandemKafkaAutoConfiguration} (which needs {@code tandem-kafka} on the classpath) nor the
     * application itself. Without it the relay would fail as an unsatisfied {@link WorkerPool}
     * dependency, which names an interface rather than the dependency to declare.
     *
     * <p>It backs off as soon as either contributes one: the Kafka autoconfiguration is ordered
     * {@code before} this class, which is where Spring's cross-class {@link ConditionalOnMissingBean}
     * guarantee applies.
     */
    @Bean
    @ConditionalOnMissingBean
    OutboxDispatcher tandemMissingDispatcher() {
        throw new TandemConfigurationException("The relay has no OutboxDispatcher, so it has nothing to"
                + " publish with. Add `com.codingful:tandem-kafka` to publish to Kafka with Tandem's"
                + " CloudEvents binding, or contribute an OutboxDispatcher bean of your own (for example"
                + " `com.codingful:tandem-rabbitmq`'s RabbitRelay). See LLD-spring-config §4.4.");
    }

    @Bean
    @ConditionalOnMissingBean
    OutboxStore tandemOutboxStore(DataSource dataSource, RelayConfig relayConfig) {
        return new JdbcOutboxStore(dataSource, relayConfig.maxAttempts());
    }

    @Bean
    @ConditionalOnMissingBean
    TandemMetrics tandemMetrics() {
        return TandemMetrics.NOOP;
    }

    @Bean
    @ConditionalOnMissingBean
    BucketSource tandemBucketSource(RelayConfig relayConfig, DataSource dataSource) {
        return BucketSource.forCoordination(relayConfig, dataSource);
    }

    /** Publishes this instance's {@link RelayConfig#coordination()} and honours Admin-API pause/resume (HLD-admin-api §4.1). */
    @Bean
    @ConditionalOnMissingBean
    RelayControlSource tandemRelayControlSource(RelayConfig relayConfig, DataSource dataSource) {
        return new JdbcRelayControlSource(dataSource, relayConfig.coordination(), relayConfig.reclaimInterval());
    }

    /**
     * Where the write-side's post-commit signals arrive (dispatch-latency §3.4), selected by
     * {@code tandem.outbox.wakeup} — the same key the write-side emits under, since a signal only helps
     * when both sides name the same mechanism. The default contributes {@link WakeupSource#NONE}, so an
     * application that sets nothing pays nothing and discovers work by polling exactly as before.
     */
    @Bean
    @ConditionalOnMissingBean
    WakeupSource tandemWakeupSource(TandemOutboxProperties outbox, DataSource dataSource) {
        return WakeupSource.forWakeup(outbox.wakeup(), dataSource);
    }

    @Bean
    @ConditionalOnMissingBean
    WorkerPool tandemWorkerPool(OutboxStore outboxStore, OutboxDispatcher outboxDispatcher, RelayConfig relayConfig,
            TandemMetrics tandemMetrics, BucketSource bucketSource, RelayControlSource relayControlSource,
            WakeupSource wakeupSource) {
        return new WorkerPool(outboxStore, outboxDispatcher, relayConfig, tandemMetrics, Clock.systemUTC(),
                BackoffStrategy.fullJitter(), bucketSource, relayControlSource, wakeupSource);
    }

    @Bean
    @ConditionalOnMissingBean
    RelayLifecycle tandemRelayLifecycle(WorkerPool workerPool, DataSource dataSource, TandemOutboxProperties outbox) {
        // Return the concrete type, not SmartLifecycle: @ConditionalOnMissingBean on the SmartLifecycle
        // interface would back off whenever the application has any other lifecycle bean (a real Boot app
        // has several), silently leaving the relay unstarted.
        return new RelayLifecycle(workerPool, dataSource, outbox.bucketCount());
    }
}
