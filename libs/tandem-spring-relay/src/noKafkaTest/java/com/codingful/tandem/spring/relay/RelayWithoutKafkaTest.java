package com.codingful.tandem.spring.relay;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.jdbc.WorkerPool;
import java.sql.Connection;
import java.util.concurrent.CompletableFuture;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.datasource.AbstractDataSource;

/**
 * The relay autoconfiguration on a classpath that genuinely has no Kafka adapter on it.
 *
 * <p><b>Why a whole source set rather than a {@code FilteredClassLoader} test.</b> A filtered
 * classloader only makes classpath <em>checks</em> fail: the configuration class stays loaded by the
 * parent loader, so a Kafka type left in a {@code @Bean} signature would still resolve and the test
 * would pass while every real Kafka-free application failed with a {@code NoClassDefFoundError} during
 * introspection. That failure mode has already happened once in this project, for the Micrometer
 * Tracing bridge. Only a classpath that really lacks the jar catches it, which is what this is.
 *
 * <p>What it therefore covers: that {@link TandemRelayAutoConfiguration} loads, introspects and wires a
 * complete relay with no Kafka class anywhere. Whether Boot's own ASM gate keeps
 * {@link TandemKafkaAutoConfiguration} out of the picture is Boot's behaviour, exercised by every
 * application that does have Kafka.
 */
class RelayWithoutKafkaTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemRelayAutoConfiguration.class));

    @Test
    void GIVEN_no_kafka_adapter_at_all_WHEN_the_application_publishes_through_its_own_transport_THEN_the_relay_is_wired_around_it() {
        OutboxDispatcher ownTransport = record -> CompletableFuture.completedFuture(null);

        runner.withBean(DataSource.class, UnconnectedDataSource::new)
                .withBean(OutboxDispatcher.class, () -> ownTransport)
                .withUserConfiguration(UnstartedLifecycle.class)
                .run(context -> {
                    assertThat(context).hasSingleBean(WorkerPool.class);
                    assertThat(context.getBean(OutboxDispatcher.class)).isSameAs(ownTransport);
                });
    }

    @Test
    void GIVEN_nothing_to_publish_with_WHEN_the_context_starts_THEN_the_failure_names_what_to_add() {
        runner.withBean(DataSource.class, UnconnectedDataSource::new)
                .withUserConfiguration(UnstartedLifecycle.class)
                .run(context -> assertThat(context).getFailure()
                        .rootCause()
                        .isInstanceOf(TandemConfigurationException.class)
                        .hasMessageContaining("tandem-kafka")
                        .hasMessageContaining("tandem-rabbitmq"));
    }

    /** Present so the relay wires, never connected to: opening it would be a test bug. */
    static final class UnconnectedDataSource extends AbstractDataSource {

        @Override
        public Connection getConnection() {
            throw new UnsupportedOperationException("This wiring test must not open a connection");
        }

        @Override
        public Connection getConnection(String username, String password) {
            return getConnection();
        }
    }

    /** The autoconfigured lifecycle with start/stop suppressed: a real one would open the database. */
    @Configuration(proxyBeanMethods = false)
    static class UnstartedLifecycle {

        @Bean
        RelayLifecycle tandemRelayLifecycle(WorkerPool workerPool, DataSource dataSource,
                TandemOutboxProperties outbox) {
            return new RelayLifecycle(workerPool, dataSource, outbox.bucketCount()) {
                @Override
                public void start() {
                }

                @Override
                public void stop() {
                }
            };
        }
    }
}
