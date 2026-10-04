package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.Map;
import org.apache.kafka.clients.CommonClientConfigs;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.SaslConfigs;
import org.junit.jupiter.api.Test;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.env.SystemEnvironmentPropertySource;

class TandemKafkaPropertiesTest {

    private static final String BROKER = "broker:9092";
    private static final String PROTOCOL = "SASL_SSL";
    private static final String JAAS_MODULE = "org.apache.kafka.common.security.plain.PlainLoginModule required;";

    @Test
    void GIVEN_no_producer_properties_WHEN_bound_THEN_the_producer_map_is_empty_not_null() {
        TandemKafkaProperties properties =
                new TandemKafkaProperties(URI.create("/tandem/test"), "application/json", null, "-topic", null);

        assertThat(properties.producer()).isEmpty();
    }

    /**
     * A container is configured through its environment, and Kafka's setting names are dotted. Spring
     * joins the words of the variable name back with dots when the target is a map, which is what lets
     * a variable alone reach the producer under the name Kafka reads.
     */
    @Test
    void GIVEN_producer_settings_given_as_environment_variables_WHEN_bound_THEN_they_reach_the_producer_under_kafkas_own_names() {
        StandardEnvironment environment = new StandardEnvironment();
        environment.getPropertySources().replace(
                StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME,
                new SystemEnvironmentPropertySource(StandardEnvironment.SYSTEM_ENVIRONMENT_PROPERTY_SOURCE_NAME, Map.of(
                        "TANDEM_KAFKA_SOURCE", "/tandem/test",
                        "TANDEM_KAFKA_PRODUCER_BOOTSTRAP_SERVERS", BROKER,
                        "TANDEM_KAFKA_PRODUCER_SECURITY_PROTOCOL", PROTOCOL,
                        "TANDEM_KAFKA_PRODUCER_SASL_JAAS_CONFIG", JAAS_MODULE)));

        TandemKafkaProperties properties =
                Binder.get(environment).bind("tandem.kafka", TandemKafkaProperties.class).get();

        assertThat(properties.producer()).containsOnly(
                Map.entry(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER),
                Map.entry(CommonClientConfigs.SECURITY_PROTOCOL_CONFIG, PROTOCOL),
                Map.entry(SaslConfigs.SASL_JAAS_CONFIG, JAAS_MODULE));
    }
}
