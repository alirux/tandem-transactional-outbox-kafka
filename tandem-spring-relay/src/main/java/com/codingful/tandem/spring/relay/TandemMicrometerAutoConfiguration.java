package com.codingful.tandem.spring.relay;

import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.micrometer.MicrometerTandemMetrics;
import io.micrometer.core.instrument.MeterRegistry;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;

/**
 * Contributes a Micrometer-backed {@link TandemMetrics} bean ahead of
 * {@link TandemRelayAutoConfiguration}'s own no-op default (LLD-micrometer §5, Q31).
 *
 * <p>A separate {@code @AutoConfiguration} class, not a second {@code @Bean} method inside
 * {@link TandemRelayAutoConfiguration}: Spring only <em>guarantees</em>
 * {@link ConditionalOnMissingBean} ordering across explicitly ordered auto-configuration classes
 * ({@code before}/{@code after}), never across multiple {@code @Bean} methods declared in one class —
 * so this class is ordered {@code before} that one, and its own no-op bean is left untouched; Boot's
 * guaranteed cross-class ordering means it sees this bean already registered (when the conditions
 * below were met) and backs off on its own.
 *
 * <p>{@code before = TandemRelayAutoConfiguration.class} is a direct class literal, deliberately —
 * unlike the {@code afterName} string-based ordering that class uses against Spring's own relocatable
 * autoconfigurations (LLD-spring-config §1.1). That rule exists because Spring's classes move between
 * Boot generations; this one is Tandem's own, always in the same package on both.
 *
 * <p>It is ordered <b>after Spring Boot's own metrics support</b>, by name, for the opposite reason: the
 * {@code MeterRegistry} of a real application is contributed by those autoconfigurations, and the
 * {@link ConditionalOnBean} below is evaluated when this class is processed. Left unordered, this class
 * sorts ahead of them, sees no registry, and the relay silently runs on the no-op metrics. Naming the
 * composite registry autoconfiguration alone is enough, since every exporter orders itself before it.
 */
@AutoConfiguration(before = TandemRelayAutoConfiguration.class, afterName = {
        // Spring Boot 3.x
        "org.springframework.boot.actuate.autoconfigure.metrics.CompositeMeterRegistryAutoConfiguration",
        // Spring Boot 4.x, relocated into spring-boot-micrometer-metrics
        "org.springframework.boot.micrometer.metrics.autoconfigure.CompositeMeterRegistryAutoConfiguration"})
@ConditionalOnClass({MeterRegistry.class, MicrometerTandemMetrics.class})
@EnableConfigurationProperties(TandemMetricsProperties.class)
public class TandemMicrometerAutoConfiguration {

    @Bean
    @ConditionalOnBean(MeterRegistry.class)
    @ConditionalOnMissingBean(TandemMetrics.class)
    TandemMetrics tandemMicrometerMetrics(MeterRegistry registry, TandemMetricsProperties properties) {
        return properties.maxPublishLatency() != null
                ? new MicrometerTandemMetrics(registry, properties.maxPublishLatency())
                : new MicrometerTandemMetrics(registry);
    }
}
