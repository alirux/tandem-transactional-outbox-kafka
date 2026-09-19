package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.MessageEncoder;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.JdbcRelayControlSource;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.PgNotifyWakeup;
import com.codingful.tandem.jdbc.RelayControlSource;
import com.codingful.tandem.jdbc.WakeupSource;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaMessageEncoder;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.Map;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.FilteredClassLoader;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Wiring tests for the relay autoconfiguration (LLD-spring-config §4.4) that need no Docker, so they run
 * on both Spring generations (§1.2). They cover the engine the module assembles and the {@code tandem.*}
 * keys it binds onto it, plus the cases where it must back off entirely. Only the relay's actual delivery
 * — and the lifecycle really starting the pool — stays in the integration test, which needs a database
 * and a broker.
 */
class TandemRelayAutoConfigurationTest {

    private static final OutboxRecord AUDITED_RECORD = OutboxRecord.builder()
            .id(1)
            .message(OutboxMessage.builder().aggregateId("order-1").aggregateType("Order").seq(1)
                    .payload("{}".getBytes(StandardCharsets.UTF_8)).build())
            .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
            .build();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    TandemKafkaAutoConfiguration.class, TandemRelayAutoConfiguration.class));

    /**
     * The wired engine, with the pool left unstarted. The autoconfigured {@link RelayLifecycle} would
     * open the database at context refresh (the bucket-count guard, then the pool), which is the
     * integration test's job — so this replaces just that one bean, and everything else is the real
     * autoconfigured graph, Kafka producer included.
     */
    private ApplicationContextRunner wiredRelay() {
        return runner.withBean(DataSource.class, NoopDataSource::new)
                .withUserConfiguration(UnstartedLifecycle.class)
                .withPropertyValues(
                        "tandem.kafka.source=/tandem/test",
                        "tandem.kafka.producer[bootstrap.servers]=localhost:9092");
    }

    @Test
    void GIVEN_a_datasource_and_the_kafka_settings_WHEN_the_context_starts_THEN_the_whole_engine_is_wired() {
        wiredRelay().run(context -> {
            assertThat(context).hasSingleBean(TopicRouter.class);
            assertThat(context).hasSingleBean(OutboxDispatcher.class);
            assertThat(context).hasSingleBean(OutboxStore.class);
            assertThat(context).hasSingleBean(BucketSource.class);
            assertThat(context).hasSingleBean(WorkerPool.class);
            assertThat(context).hasSingleBean(RelayConfig.class);
            assertThat(context).hasSingleBean(RelayControlSource.class);
            assertThat(context.getBean(RelayControlSource.class)).isInstanceOf(JdbcRelayControlSource.class);
            assertThat(context.getBean(TandemMetrics.class)).isSameAs(TandemMetrics.NOOP);
            // Nothing is listened for unless asked: the relay discovers work by polling by default.
            assertThat(context.getBean(WakeupSource.class)).isSameAs(WakeupSource.NONE);
        });
    }

    @Test
    void GIVEN_the_wakeup_configured_WHEN_the_context_starts_THEN_the_relay_listens_for_it() {
        // The relay half of the post-commit wakeup (dispatch-latency §3.4). It binds the same
        // tandem.outbox key the write-side emits under, because a signal only helps when both sides
        // name the same mechanism.
        wiredRelay().withPropertyValues("tandem.outbox.wakeup=pg-notify").run(context ->
                assertThat(context.getBean(WakeupSource.class)).isInstanceOf(PgNotifyWakeup.class));
    }

    @Test
    void GIVEN_relay_settings_in_the_configuration_WHEN_the_context_starts_THEN_they_reach_the_engine() {
        wiredRelay().withPropertyValues(
                        "tandem.outbox.bucket-count=512",
                        "tandem.relay.batch-size=55",
                        "tandem.relay.row-lease=90s",
                        "tandem.relay.metrics-interval=33s")
                .run(context -> {
                    RelayConfig config = context.getBean(RelayConfig.class);
                    assertThat(config.bucketCount()).isEqualTo(512);
                    assertThat(config.batchSize()).isEqualTo(55);
                    assertThat(config.rowLease()).isEqualTo(Duration.ofSeconds(90));
                    assertThat(config.metricsInterval()).isEqualTo(Duration.ofSeconds(33));
                    // Unset keys keep RelayConfig's own defaults — the properties never shadow them.
                    assertThat(config.maxAttempts()).isEqualTo(RelayConfig.defaults().maxAttempts());
                });
    }

    @Test
    void GIVEN_an_application_running_a_tracer_WHEN_the_publish_span_is_asked_for_THEN_the_relay_emits_it() {
        wiredRelay().withBean(Propagator.class, TandemRelayAutoConfigurationTest::w3cPropagator)
                .withPropertyValues("tandem.tracing.publish-span=true")
                .run(context -> assertThat(context.getBean(TandemSpanRecorder.class))
                        .isInstanceOf(MicrometerTandemSpanRecorder.class));
    }

    @Test
    void GIVEN_an_application_running_a_tracer_WHEN_the_publish_span_is_not_asked_for_THEN_no_span_is_emitted() {
        wiredRelay().withBean(Propagator.class, TandemRelayAutoConfigurationTest::w3cPropagator)
                .run(context -> assertThat(context).doesNotHaveBean(TandemSpanRecorder.class));
    }

    @Test
    void GIVEN_no_tracing_at_all_WHEN_the_publish_span_is_asked_for_THEN_the_relay_still_starts() {
        wiredRelay().withPropertyValues("tandem.tracing.publish-span=true")
                .run(context -> {
                    assertThat(context).doesNotHaveBean(TandemSpanRecorder.class);
                    assertThat(context).hasSingleBean(OutboxDispatcher.class);
                });
    }

    /**
     * With no tracing library available the relay must still start, simply without span emission. Same
     * limit as the producer module's equivalent test: {@link FilteredClassLoader} only makes the classpath
     * <i>checks</i> fail, so this pins the conditional back-off and not the erased-signature trap that
     * breaks context creation outright — {@code tandem-sample-spring}'s smoke integration test, on a
     * classpath genuinely without the library, is what catches that.
     */
    @Test
    void GIVEN_an_application_without_any_tracing_library_WHEN_the_context_starts_THEN_the_relay_still_starts() {
        wiredRelay().withClassLoader(new FilteredClassLoader(Propagator.class))
                .withPropertyValues("tandem.tracing.publish-span=true")
                .run(context -> {
                    assertThat(context).hasNotFailed();
                    assertThat(context).hasSingleBean(WorkerPool.class);
                });
    }

    private static Propagator w3cPropagator() {
        return new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
                SdkTracerProvider.builder().build().get("tandem-relay"));
    }

    @Test
    void GIVEN_a_custom_topic_router_WHEN_the_context_starts_THEN_it_replaces_the_autoconfigured_one() {
        TopicRouter custom = record -> "fixed-topic";

        wiredRelay().withBean(TopicRouter.class, () -> custom)
                .run(context -> assertThat(context.getBean(TopicRouter.class)).isSameAs(custom));
    }

    /**
     * The portable way to publish a different envelope: the application writes it once against the core
     * port, naming no broker type, and the relay module binds it to its own transport. The same class
     * would serve a different transport adapter unchanged, which is the point of the neutral port.
     */
    @Test
    void GIVEN_an_application_that_publishes_its_own_envelope_WHEN_the_context_starts_THEN_the_relay_binds_it_to_kafka() {
        MessageEncoder portable = record ->
                new EncodedMessage("audit", record.aggregateId().value(), record.payload(),
                        Map.of("envelope", "own".getBytes(StandardCharsets.UTF_8)));

        wiredRelay().withBean(MessageEncoder.class, () -> portable).run(context -> {
            ProducerRecord<String, byte[]> encoded =
                    context.getBean(KafkaMessageEncoder.class).encode(AUDITED_RECORD);

            assertThat(encoded.topic()).isEqualTo("audit");
            assertThat(encoded.key()).isEqualTo(AUDITED_RECORD.aggregateId().value());
            assertThat(encoded.headers().lastHeader("envelope")).isNotNull();
            assertThat(encoded.headers().lastHeader("ce_id")).isNull();   // the default envelope is gone
        });
    }

    @Test
    void GIVEN_an_application_that_publishes_its_own_format_WHEN_the_context_starts_THEN_it_replaces_the_cloudevents_default() {
        KafkaMessageEncoder custom = record -> new ProducerRecord<>("audit", record.payload());

        wiredRelay().withBean(KafkaMessageEncoder.class, () -> custom)
                .run(context -> assertThat(context.getBean(KafkaMessageEncoder.class)).isSameAs(custom));
    }

    /**
     * {@code tandem.kafka.source} configures the CloudEvents envelope and nothing else, so an application
     * publishing its own format is not made to invent a value for a setting its events never carry.
     */
    @Test
    void GIVEN_an_application_that_publishes_its_own_format_WHEN_no_cloudevents_source_is_set_THEN_the_context_still_starts() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withUserConfiguration(UnstartedLifecycle.class)
                .withPropertyValues("tandem.kafka.producer[bootstrap.servers]=localhost:9092")
                .withBean(KafkaMessageEncoder.class,
                        () -> record -> new ProducerRecord<>("audit", record.payload()))
                .run(context -> assertThat(context).hasSingleBean(OutboxDispatcher.class));
    }

    /**
     * The case a second transport adapter creates: the application publishes elsewhere entirely and
     * contributes its own {@link OutboxDispatcher}. It must not be made to set a Kafka property for a
     * producer it never builds.
     */
    @Test
    void GIVEN_an_application_publishing_to_another_broker_WHEN_it_contributes_its_own_dispatcher_THEN_no_kafka_setting_is_demanded() {
        OutboxDispatcher ownDispatcher = record -> CompletableFuture.completedFuture(null);

        runner.withBean(DataSource.class, NoopDataSource::new)
                .withUserConfiguration(UnstartedLifecycle.class)
                .withBean(OutboxDispatcher.class, () -> ownDispatcher)
                .run(context -> assertThat(context.getBean(OutboxDispatcher.class)).isSameAs(ownDispatcher));
    }

    @Test
    void GIVEN_no_datasource_WHEN_the_context_starts_THEN_no_relay_is_contributed() {
        runner.withPropertyValues("tandem.kafka.source=/tandem/test")
                .run(context -> assertThat(context).doesNotHaveBean(WorkerPool.class));
    }

    @Test
    void GIVEN_the_relay_is_disabled_WHEN_the_context_starts_THEN_no_relay_is_contributed() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .withPropertyValues("tandem.relay.enabled=false", "tandem.kafka.source=/tandem/test")
                .run(context -> assertThat(context).doesNotHaveBean(WorkerPool.class));
    }

    /**
     * The CloudEvents source is the one setting with no default (LLD-spring-config §2.3), so its absence
     * must name the key the operator has to set — not surface as a NullPointerException from deep inside
     * the CloudEvents configuration.
     */
    @Test
    void GIVEN_no_cloudevents_source_WHEN_the_context_starts_THEN_it_fails_naming_the_missing_setting() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .run(context -> assertThat(context).getFailure()
                        .rootCause()
                        .isInstanceOf(TandemConfigurationException.class)
                        .hasMessageContaining("tandem.kafka.source"));
    }

    /**
     * The autoconfigured lifecycle, with {@code start}/{@code stop} suppressed: a real one would connect
     * to the database at context refresh. Everything the wiring tests assert is built by the
     * autoconfiguration itself — only the starting of the pool is out of scope here.
     */
    @Configuration(proxyBeanMethods = false)
    static class UnstartedLifecycle {

        @Bean
        RelayLifecycle tandemRelayLifecycle(WorkerPool workerPool, DataSource dataSource,
                TandemOutboxProperties outbox) {
            return new RelayLifecycle(workerPool, dataSource, outbox.bucketCount()) {
                @Override
                public void start() {
                }

                @Override
                public void stop() {
                }
            };
        }
    }
}
