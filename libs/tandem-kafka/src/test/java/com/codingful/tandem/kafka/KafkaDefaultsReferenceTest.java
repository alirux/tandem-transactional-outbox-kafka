package com.codingful.tandem.kafka;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.test.reference.ReferenceFile;
import com.codingful.tandem.test.reference.ReferenceValues;
import java.lang.reflect.RecordComponent;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.jupiter.api.Test;

/**
 * Pins the defaults an application wiring the Kafka adapter without Spring gets (LLD-api-stability §8.3):
 * those of {@code KafkaRelayConfig.of(source)}, and the producer settings the relay applies to a producer
 * configuration that sets nothing. A setting the relay applies even over the application's own value is
 * found by setting one and seeing it overridden, not by a list in this test.
 */
class KafkaDefaultsReferenceTest {

    private static final String SOURCE = "/tandem/reference";

    /** The floor of the filled-in delivery timeout, below Kafka's own two minutes so it stays under the row lease. */
    private static final long DELIVERY_TIMEOUT_FLOOR_MS = 30_000;
    private static final String DELIVERY_TIMEOUT_RULE =
            "max(" + DELIVERY_TIMEOUT_FLOOR_MS + ", linger.ms + request.timeout.ms)";

    /**
     * A value an application could set for each setting the relay applies, different from the relay's own
     * and accepted by its validation, to see whether the relay keeps it or overrides it.
     */
    private static final Map<String, Object> APPLICATION_VALUES = Map.of(
            ProducerConfig.ACKS_CONFIG, "-1",
            ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true",
            ProducerConfig.RETRIES_CONFIG, 3,
            ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG, 120_000,
            ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName(),
            ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

    @Test
    void GIVEN_an_application_that_sets_nothing_WHEN_the_relay_configures_kafka_THEN_the_defaults_match_their_reference_file()
            throws ReflectiveOperationException {
        List<String> lines = new ArrayList<>();

        KafkaRelayConfig config = KafkaRelayConfig.of(SOURCE);
        for (RecordComponent component : KafkaRelayConfig.class.getRecordComponents()) {
            Object value = component.getAccessor().invoke(config);
            // The source is the argument the application must pass, not a default.
            String rendered = Objects.equals(value, URI.create(SOURCE)) ? ReferenceValues.REQUIRED : ReferenceValues.of(value);
            lines.add("KafkaRelayConfig." + component.getName() + "=" + rendered);
        }

        Map<String, Object> applied = KafkaProducerConfig.harden(Map.of());
        for (String setting : new TreeSet<>(applied.keySet())) {
            lines.add("KafkaRelay.producer." + setting + "=" + producerSetting(setting, applied.get(setting)));
        }

        ReferenceFile.of("defaults.txt", KafkaDefaultsReferenceTest.class).assertMatches(lines.stream().sorted().toList());
    }

    private static String producerSetting(String setting, Object applied) {
        assertThat(APPLICATION_VALUES).as("an application value to probe the setting %s with", setting)
                .containsKey(setting);
        Object applicationValue = APPLICATION_VALUES.get(setting);
        Object kept = KafkaProducerConfig.harden(Map.of(setting, applicationValue)).get(setting);
        if (!applicationValue.equals(kept)) {
            return ReferenceValues.always(applied);
        }
        if (setting.equals(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG)) {
            return deliveryTimeout(applied);
        }
        return ReferenceValues.of(applied);
    }

    /** The delivery timeout grows with the producer's own floor, which Kafka computes from two other settings. */
    private static String deliveryTimeout(Object applied) {
        Map<String, Object> kafkaDefaults = ProducerConfig.configDef().defaultValues();
        long requestTimeout = ((Number) kafkaDefaults.get(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG)).longValue();
        long linger = ((Number) kafkaDefaults.get(ProducerConfig.LINGER_MS_CONFIG)).longValue();
        assertThat(((Number) applied).longValue())
                .isEqualTo(Math.max(DELIVERY_TIMEOUT_FLOOR_MS, linger + requestTimeout));

        long longLinger = 20_000;
        Object grown = KafkaProducerConfig.harden(Map.of(ProducerConfig.LINGER_MS_CONFIG, longLinger))
                .get(ProducerConfig.DELIVERY_TIMEOUT_MS_CONFIG);
        assertThat(((Number) grown).longValue())
                .isEqualTo(Math.max(DELIVERY_TIMEOUT_FLOOR_MS, longLinger + requestTimeout));
        return ReferenceValues.derived(DELIVERY_TIMEOUT_RULE);
    }
}
