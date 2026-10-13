package com.codingful.tandem.micrometer;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.test.reference.ReferenceFile;
import com.codingful.tandem.test.reference.ReferenceValues;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Tag;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;

/**
 * Pins what the Micrometer adapter puts on the wire (LLD-api-stability §8.4): every meter it registers,
 * with its type, the base unit it declares and its tag keys, which Prometheus builds the exported name
 * from; and the publish-latency histogram's ceiling, a default no key exposes without Spring (§8.3).
 */
class MicrometerReferenceTest {

    private static final String PUBLISH_LATENCY = "tandem.outbox.publish.latency";

    /** An argument for each parameter type a recording method takes; a method taking another type fails the test. */
    private static final Map<Class<?>, Object> ARGUMENTS = Map.of(
            long.class, 3L,
            int.class, 2,
            double.class, 1.5,
            Duration.class, Duration.ofMillis(250),
            String.class, "reference_check");

    private final DeclaredMeters declared = new DeclaredMeters();
    private final SimpleMeterRegistry registry = registryRecording(declared);

    @Test
    void GIVEN_every_recording_method_called_once_WHEN_the_meters_are_listed_THEN_they_match_their_reference_file()
            throws ReflectiveOperationException {
        TandemMetrics metrics = new MicrometerTandemMetrics(registry);

        // Every method the port declares, so a meter registered on first use is listed too and a new
        // recording method cannot add a meter this test never sees.
        for (Method method : TandemMetrics.class.getDeclaredMethods()) {
            if (Modifier.isStatic(method.getModifiers()) || method.isSynthetic()) {
                continue;
            }
            method.invoke(metrics, Arrays.stream(method.getParameterTypes()).map(this::argumentFor).toArray());
        }

        assertThat(declared.ids()).extracting(Meter.Id::getName)
                .containsExactlyInAnyOrderElementsOf(registry.getMeters().stream().map(meter -> meter.getId().getName()).toList());
        List<String> lines = declared.ids().stream()
                .map(MicrometerReferenceTest::meterLine)
                .distinct()
                .sorted()
                .toList();
        ReferenceFile.of("meters.txt", MicrometerReferenceTest.class).assertMatches(lines);
    }

    @Test
    void GIVEN_no_ceiling_given_WHEN_the_adapter_registers_its_meters_THEN_its_defaults_match_their_reference_file() {
        new MicrometerTandemMetrics(registry);

        Duration ceiling = Duration.ofNanos(declared.distribution(PUBLISH_LATENCY).getMaximumExpectedValueAsDouble().longValue());

        ReferenceFile.of("defaults.txt", MicrometerReferenceTest.class)
                .assertMatches(List.of("MicrometerTandemMetrics.maxExpectedPublishLatency=" + ReferenceValues.of(ceiling)));
    }

    private Object argumentFor(Class<?> parameterType) {
        assertThat(ARGUMENTS).as("an argument for a recording method's parameter of type %s", parameterType)
                .containsKey(parameterType);
        return ARGUMENTS.get(parameterType);
    }

    private static String meterLine(Meter.Id id) {
        String baseUnit = id.getBaseUnit() == null ? "-" : id.getBaseUnit();
        String tagKeys = id.getTags().stream().map(Tag::getKey).sorted()
                .collect(Collectors.joining(", ", "[", "]"));
        return id.getName() + " " + id.getType().name().toLowerCase(Locale.ROOT) + " " + baseUnit + " " + tagKeys;
    }

    private static SimpleMeterRegistry registryRecording(DeclaredMeters declared) {
        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        registry.config().meterFilter(declared);
        return registry;
    }
}
