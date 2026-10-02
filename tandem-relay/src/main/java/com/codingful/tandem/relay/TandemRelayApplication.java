package com.codingful.tandem.relay;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.ConfigurationPropertiesScan;

/**
 * The application around the relay: everything it runs is contributed by the autoconfigurations of
 * {@code tandem-spring-relay} and {@code tandem-admin}, chosen by {@code tandem.relay.enabled} and
 * {@code tandem.admin.enabled} (LLD-relay §4).
 */
@SpringBootApplication
@ConfigurationPropertiesScan
public class TandemRelayApplication {

    public static void main(String[] args) {
        SpringApplication.run(TandemRelayApplication.class, args);
    }
}
