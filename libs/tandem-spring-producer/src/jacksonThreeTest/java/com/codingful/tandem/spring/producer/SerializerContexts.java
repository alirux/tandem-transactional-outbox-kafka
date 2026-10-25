package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.PayloadSerializer;
import com.codingful.tandem.test.InMemoryOutbox;
import java.nio.charset.StandardCharsets;
import javax.sql.DataSource;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

/**
 * The application the Jackson 3 gate starts: the write side with an in-memory outbox, so no database is
 * touched, plus whichever of Spring Boot's own Jackson autoconfigurations a test asks for. Those are
 * resolved by name, because the Jackson 2 one exists only on one of the two classpaths, and they are
 * Boot's real ones: a mapper bean handed to the runner would not show what an application actually gets.
 */
final class SerializerContexts {

    static final String JACKSON_3_AUTOCONFIGURATION = "org.springframework.boot.jackson.autoconfigure.JacksonAutoConfiguration";
    static final String JACKSON_2_AUTOCONFIGURATION = "org.springframework.boot.jackson2.autoconfigure.Jackson2AutoConfiguration";

    /** The application's naming for each generation, so the JSON shows which mapper wrote it. */
    static final String JACKSON_3_SNAKE_CASE = "spring.jackson.property-naming-strategy=SNAKE_CASE";
    static final String JACKSON_2_KEBAB_CASE = "spring.jackson2.property-naming-strategy=KEBAB_CASE";
    static final String PREFER_JACKSON_2 = JacksonPayloadSerializers.PREFERRED_JSON_MAPPER + "=jackson2";

    static final String WRITTEN_BY_JACKSON_3_BEAN = "{\"order_id\":\"order-42\"}";
    static final String WRITTEN_BY_JACKSON_2_BEAN = "{\"order-id\":\"order-42\"}";
    static final String WRITTEN_BY_A_PLAIN_MAPPER = "{\"orderId\":\"order-42\"}";

    record OrderPlaced(String orderId) {
    }

    private SerializerContexts() {
    }

    static ApplicationContextRunner writeSide(String... autoConfigurations) {
        Class<?>[] classes = new Class<?>[autoConfigurations.length + 1];
        classes[0] = TandemProducerAutoConfiguration.class;
        for (int i = 0; i < autoConfigurations.length; i++) {
            try {
                classes[i + 1] = Class.forName(autoConfigurations[i]);
            } catch (ClassNotFoundException e) {
                throw new IllegalStateException("Not on this classpath: " + autoConfigurations[i], e);
            }
        }
        return new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(classes))
                .withBean(DataSource.class, SimpleDriverDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new);
    }

    static String serialized(ApplicationContext context) {
        byte[] bytes = context.getBean(PayloadSerializer.class).serialize(new OrderPlaced("order-42"));
        return new String(bytes, StandardCharsets.UTF_8);
    }
}
