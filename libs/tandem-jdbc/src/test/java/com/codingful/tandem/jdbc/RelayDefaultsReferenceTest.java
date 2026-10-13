package com.codingful.tandem.jdbc;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.test.reference.ReferenceFile;
import com.codingful.tandem.test.reference.ReferenceValues;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;

/**
 * Pins the defaults of the relay engine as an application wiring it without Spring gets them
 * (LLD-api-stability §8.3): every public accessor of {@code RelayConfig.defaults()}, read by reflection so
 * an accessor added without its line fails here, and the bounds of the default retry backoff. The two
 * defaults computed at run time are recorded as their rule, once the test has checked the value follows it.
 */
class RelayDefaultsReferenceTest {

    private static final String WORKERS_RULE = "availableProcessors() * 2";
    private static final String INSTANCE_ID_RULE = "tandem-{host}-{pid}-{random hex}";

    @Test
    void GIVEN_an_application_that_configures_nothing_WHEN_the_relay_engine_is_built_THEN_its_defaults_match_their_reference_file()
            throws ReflectiveOperationException {
        RelayConfig defaults = RelayConfig.defaults();
        List<String> lines = new ArrayList<>();
        for (Method accessor : publicAccessors()) {
            Object value = accessor.invoke(defaults);
            String rendered = switch (accessor.getName()) {
                case "workersPerInstance" -> workersPerInstance((int) value);
                case "instanceId" -> instanceId((String) value);
                default -> ReferenceValues.of(value);
            };
            lines.add("RelayConfig." + accessor.getName() + "=" + rendered);
        }

        assertThat(BackoffStrategy.fullJitter()).isInstanceOf(FullJitterBackoff.class);
        FullJitterBackoff backoff = (FullJitterBackoff) BackoffStrategy.fullJitter();
        lines.add("BackoffStrategy.fullJitter.base=" + ReferenceValues.of(backoff.base()));
        lines.add("BackoffStrategy.fullJitter.cap=" + ReferenceValues.of(backoff.cap()));

        ReferenceFile.of("defaults.txt", RelayDefaultsReferenceTest.class).assertMatches(lines.stream().sorted().toList());
    }

    private static List<Method> publicAccessors() {
        return Arrays.stream(RelayConfig.class.getDeclaredMethods())
                .filter(method -> Modifier.isPublic(method.getModifiers()) && !Modifier.isStatic(method.getModifiers()))
                .filter(method -> method.getParameterCount() == 0 && method.getReturnType() != void.class)
                .filter(method -> !method.isSynthetic())
                .toList();
    }

    private static String workersPerInstance(int workers) {
        assertThat(workers).isEqualTo(Runtime.getRuntime().availableProcessors() * 2);
        return ReferenceValues.derived(WORKERS_RULE);
    }

    private static String instanceId(String instanceId) {
        assertThat(instanceId)
                .startsWith("tandem-")
                .contains("-" + ProcessHandle.current().pid() + "-")
                .matches(".*-[0-9a-f]{1,4}")
                .hasSizeLessThanOrEqualTo(64);
        return ReferenceValues.derived(INSTANCE_ID_RULE);
    }
}
