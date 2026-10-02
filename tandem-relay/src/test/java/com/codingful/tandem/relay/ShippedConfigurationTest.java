package com.codingful.tandem.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.zaxxer.hikari.HikariConfig;
import java.io.IOException;
import org.junit.jupiter.api.Test;
import org.postgresql.PGProperty;
import org.springframework.boot.context.properties.bind.Binder;
import org.springframework.boot.env.YamlPropertySourceLoader;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.core.io.ClassPathResource;

/**
 * The defaults the application ships in {@code application.yml}, bound the way Spring Boot binds them
 * (LLD-relay §6.1). A key one level off in that file is ignored without a word, and the default it was
 * meant to set would simply not exist.
 */
class ShippedConfigurationTest {

    @Test
    void GIVEN_the_shipped_configuration_WHEN_the_connection_pool_is_bound_THEN_the_driver_is_handed_a_socket_timeout()
            throws IOException {
        StandardEnvironment environment = new StandardEnvironment();
        new YamlPropertySourceLoader().load("shipped", new ClassPathResource("application.yml"))
                .forEach(environment.getPropertySources()::addLast);

        HikariConfig pool = Binder.get(environment).bind("spring.datasource.hikari", HikariConfig.class).get();

        // Under the name the PostgreSQL driver itself reads, and a positive number of seconds.
        String socketTimeout = pool.getDataSourceProperties().getProperty(PGProperty.SOCKET_TIMEOUT.getName());
        assertThat(socketTimeout).isNotNull();
        assertThat(Integer.parseInt(socketTimeout)).isPositive();
    }
}
