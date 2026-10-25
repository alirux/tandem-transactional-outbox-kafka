package com.codingful.tandem.sample.jpa;

import com.codingful.tandem.test.TandemTestContainer;
import java.util.Map;
import org.springframework.context.ApplicationContextInitializer;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.core.env.MapPropertySource;

/**
 * [DEMO-ONLY] Starts PostgreSQL and Kafka before Boot configures the DataSource.
 * Registered in META-INF/spring.factories for Boot 4 discovery. A real application
 * removes this initializer and supplies spring.datasource and tandem.kafka properties instead.
 */
public class DemoContainerInitializer implements ApplicationContextInitializer<ConfigurableApplicationContext> {
    static final String TOPIC = "order-topic";

    @Override
    public void initialize(ConfigurableApplicationContext context) {
        var infrastructure = new TandemTestContainer().start();
        infrastructure.createTopic(TOPIC, 4);
        Runtime.getRuntime().addShutdownHook(new Thread(infrastructure::close, "tandem-jpa-demo-infra-close"));
        Map<String, Object> connectionDetails = Map.of(
                "spring.datasource.url", infrastructure.postgresJdbcUrl(),
                "spring.datasource.username", infrastructure.postgresUsername(),
                "spring.datasource.password", infrastructure.postgresPassword(),
                "tandem.kafka.source", "/tandem/sample-jpa",
                "tandem.kafka.producer[bootstrap.servers]", infrastructure.bootstrapServers());
        context.getEnvironment().getPropertySources()
                .addFirst(new MapPropertySource("tandemDemoContainers", connectionDetails));
        context.getBeanFactory().registerSingleton("tandemInfrastructure", infrastructure);
    }
}
