package com.codingful.tandem.spring.relay;

import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.boundKeys;
import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.resource;
import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.core.port.TandemSpanRecorder;
import com.codingful.tandem.core.port.TopicRouter;
import com.codingful.tandem.jdbc.RelayConfig;
import com.codingful.tandem.jdbc.Wakeup;
import com.codingful.tandem.jdbc.WakeupSource;
import com.codingful.tandem.jdbc.WorkerPool;
import com.codingful.tandem.kafka.KafkaMessageEncoder;
import com.codingful.tandem.micrometer.MicrometerTandemMetrics;
import com.codingful.tandem.test.reference.ReferenceFile;
import com.codingful.tandem.test.reference.ReferenceValues;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.config.MeterFilter;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import io.micrometer.tracing.otel.bridge.OtelPropagator;
import io.micrometer.tracing.propagation.Propagator;
import io.opentelemetry.api.trace.propagation.W3CTraceContextPropagator;
import io.opentelemetry.context.propagation.ContextPropagators;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.Header;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.assertj.AssertableApplicationContext;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Pins every {@code tandem.*} key the relay module binds, at the value a Spring application gets when it
 * sets none (LLD-api-stability §8.3). The keys come from the module's generated configuration metadata,
 * so a key added without a line here fails the test. Each value is read from the object that applies it
 * (the {@code RelayConfig} the engine runs on, the message the encoder produces, the meter the metrics
 * adapter registers, the beans a key switches on), so the file records where the Spring tier hands a
 * value of its own as well as where it leaves the engine's.
 *
 * <p>The context holds the beans startup needs: a {@code DataSource}, and the relay's lifecycle with its
 * start suppressed, since a real one connects to the database. The keys startup fails without are given
 * a value, and each is recorded as required once the test has seen startup fail without it. A key whose
 * effect needs a bean the application may not have is read with that bean added: the metrics ceiling
 * with a {@code MeterRegistry}, the publish span with a tracing {@code Propagator}.
 */
class EffectiveDefaultsReferenceTest {

    private static final String PUBLISH_LATENCY = "tandem.outbox.publish.latency";

    /** The keys startup fails without, each with a value to start with and what the failure without it names. */
    private static final List<RequiredKey> REQUIRED_KEYS = List.of(
            new RequiredKey("tandem.kafka.source", "tandem.kafka.source=/tandem/reference", "tandem.kafka.source"),
            new RequiredKey("tandem.kafka.producer", "tandem.kafka.producer[bootstrap.servers]=localhost:9092",
                    "bootstrap.servers"));

    /** A row that stored no content type and no data schema, so the encoder applies its defaults to it. */
    private static final OutboxRecord RECORD = OutboxRecord.builder()
            .id(1)
            .message(OutboxMessage.builder().aggregateType("Order").aggregateId("order-1").unsequenced()
                    .payload("{}".getBytes(StandardCharsets.UTF_8)).build())
            .createdAt(Instant.parse("2025-03-01T10:15:30Z"))
            .build();

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemMicrometerAutoConfiguration.class,
                    TandemKafkaAutoConfiguration.class, TandemRelayAutoConfiguration.class))
            .withBean(DataSource.class, NoopDataSource::new)
            .withUserConfiguration(TandemRelayAutoConfigurationTest.UnstartedLifecycle.class);

    @Test
    void GIVEN_an_application_that_sets_no_tandem_key_WHEN_the_relay_starts_THEN_the_effective_defaults_match_their_reference_file() {
        Map<String, String> lines = new TreeMap<>();

        for (RequiredKey required : REQUIRED_KEYS) {
            startedWithout(required).run(context -> assertThat(context).getFailure().rootCause()
                    .hasMessageContaining(required.failureNames()));
            lines.put(required.key(), ReferenceValues.REQUIRED);
        }

        startedWithout(null).run(context -> {
            assertThat(context).hasNotFailed();
            readRelay(context, lines);
            readKafka(context, lines);
        });

        Map<String, DistributionStatisticConfig> declaredMeters = new HashMap<>();
        startedWithout(null).withBean(MeterRegistry.class, () -> registryRecording(declaredMeters)).run(context -> {
            assertThat(context.getBean(TandemMetrics.class)).isInstanceOf(MicrometerTandemMetrics.class);
            Duration ceiling = Duration.ofNanos(
                    declaredMeters.get(PUBLISH_LATENCY).getMaximumExpectedValueAsDouble().longValue());
            lines.put("tandem.metrics.max-publish-latency", ReferenceValues.of(ceiling));
        });

        startedWithout(null).withBean(Propagator.class, EffectiveDefaultsReferenceTest::w3cPropagator).run(context ->
                lines.put("tandem.tracing.publish-span",
                        ReferenceValues.of(!context.getBeansOfType(TandemSpanRecorder.class).isEmpty())));

        assertThat(lines.keySet()).as("a line for every key the module binds, and for no other")
                .containsExactlyInAnyOrderElementsOf(boundKeys(resource(EffectiveDefaultsReferenceTest.class,
                        "/META-INF/spring-configuration-metadata.json")));
        List<String> rendered = new ArrayList<>();
        lines.forEach((key, value) -> rendered.add(key + "=" + value));
        ReferenceFile.of("effective-defaults.txt", EffectiveDefaultsReferenceTest.class).assertMatches(rendered);
    }

    /** {@code tandem.relay.*} maps onto the engine's {@code RelayConfig}, one accessor per key, and switches the relay on. */
    private static void readRelay(AssertableApplicationContext context, Map<String, String> lines)
            throws ReflectiveOperationException {
        RelayConfig config = context.getBean(RelayConfig.class);
        for (String key : boundKeys(resource(EffectiveDefaultsReferenceTest.class,
                "/META-INF/spring-configuration-metadata.json"))) {
            if (!key.startsWith("tandem.relay.")) {
                continue;
            }
            String setting = camelCase(key.substring("tandem.relay.".length()));
            String value = switch (setting) {
                case "enabled" -> ReferenceValues.of(!context.getBeansOfType(WorkerPool.class).isEmpty());
                case "workersPerInstance" -> workersPerInstance(config.workersPerInstance());
                case "instanceId" -> instanceId(config.instanceId());
                default -> ReferenceValues.of(RelayConfig.class.getMethod(setting).invoke(config));
            };
            lines.put(key, value);
        }
        lines.put("tandem.outbox.bucket-count", ReferenceValues.of(config.bucketCount()));

        WakeupSource wakeupSource = context.getBean(WakeupSource.class);
        DataSource dataSource = context.getBean(DataSource.class);
        List<Wakeup> applied = Arrays.stream(Wakeup.values())
                .filter(wakeup -> WakeupSource.forWakeup(wakeup, dataSource).getClass() == wakeupSource.getClass())
                .toList();
        assertThat(applied).as("the wakeup whose source the relay listens on").hasSize(1);
        lines.put("tandem.outbox.wakeup", ReferenceValues.of(applied.get(0)));
    }

    /** The CloudEvents defaults show on the message the encoder produces, the topic suffix on the topic it names. */
    private static void readKafka(AssertableApplicationContext context, Map<String, String> lines) {
        ProducerRecord<String, byte[]> message = context.getBean(KafkaMessageEncoder.class).encode(RECORD);
        lines.put("tandem.kafka.default-content-type", headerValue(message, "content-type"));
        lines.put("tandem.kafka.default-data-schema", headerValue(message, "ce_dataschema"));

        String topic = context.getBean(TopicRouter.class).topicFor(RECORD);
        String kebabAggregateType = TopicRouter.kebabCase(RECORD.aggregateType());
        assertThat(topic).startsWith(kebabAggregateType);
        lines.put("tandem.kafka.topic-suffix", topic.substring(kebabAggregateType.length()));
    }

    private static String headerValue(ProducerRecord<String, byte[]> message, String name) {
        Header header = message.headers().lastHeader(name);
        return header == null ? ReferenceValues.NONE : new String(header.value(), StandardCharsets.UTF_8);
    }

    private static String workersPerInstance(int workers) {
        assertThat(workers).isEqualTo(Runtime.getRuntime().availableProcessors() * 2);
        return ReferenceValues.derived("availableProcessors() * 2");
    }

    private static String instanceId(String instanceId) {
        assertThat(instanceId)
                .startsWith("tandem-")
                .contains("-" + ProcessHandle.current().pid() + "-")
                .matches(".*-[0-9a-f]{1,4}")
                .hasSizeLessThanOrEqualTo(64);
        return ReferenceValues.derived("tandem-{host}-{pid}-{random hex}");
    }

    /** {@code poll-interval-floor} is {@code pollIntervalFloor}. */
    private static String camelCase(String kebab) {
        StringBuilder camel = new StringBuilder();
        boolean upper = false;
        for (char c : kebab.toCharArray()) {
            if (c == '-') {
                upper = true;
            } else {
                camel.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return camel.toString();
    }

    /** The relay with every required key set but {@code missing}; {@code null} sets them all. */
    private ApplicationContextRunner startedWithout(RequiredKey missing) {
        String[] properties = REQUIRED_KEYS.stream()
                .filter(required -> required != missing)
                .map(RequiredKey::property)
                .toArray(String[]::new);
        return runner.withPropertyValues(properties);
    }

    /** A real registry, with a filter that records the histogram settings each meter states before the registry merges its own. */
    private static MeterRegistry registryRecording(Map<String, DistributionStatisticConfig> declared) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.config().meterFilter(new MeterFilter() {
            @Override
            public DistributionStatisticConfig configure(Meter.Id id, DistributionStatisticConfig config) {
                declared.put(id.getName(), config);
                return config;
            }
        });
        return registry;
    }

    private static Propagator w3cPropagator() {
        return new OtelPropagator(ContextPropagators.create(W3CTraceContextPropagator.getInstance()),
                SdkTracerProvider.builder().build().get("tandem-relay"));
    }

    private record RequiredKey(String key, String property, String failureNames) {
    }
}
