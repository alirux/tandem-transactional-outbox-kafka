package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;

import ch.qos.logback.classic.Level;
import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.spi.ILoggingEvent;
import ch.qos.logback.core.read.ListAppender;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.test.InMemoryOutbox;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

class TandemDataSourceDiagnosticsAutoConfigurationTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(
                    TandemProducerAutoConfiguration.class, TandemDataSourceDiagnosticsAutoConfiguration.class));

    private final ListAppender<ILoggingEvent> logs = new ListAppender<>();
    private Logger logger;

    @BeforeEach
    void captureLogs() {
        logger = (Logger) LoggerFactory.getLogger(TandemDataSourceDiagnosticsAutoConfiguration.class);
        logs.start();
        logger.addAppender(logs);
    }

    @AfterEach
    void releaseLogs() {
        logger.detachAppender(logs);
    }

    @Test
    void GIVEN_two_datasources_and_no_primary_WHEN_the_context_starts_THEN_the_write_side_is_absent_and_a_warning_says_why() {
        runner.withBean("ordersDataSource", DataSource.class, NoopDataSource::new)
                .withBean("tandemDataSource", DataSource.class, NoopDataSource::new)
                .run(context -> {
                    assertThat(context).doesNotHaveBean(OutboxRepository.class);
                    assertThat(logs.list).singleElement().satisfies(event -> {
                        assertThat(event.getLevel()).isEqualTo(Level.WARN);
                        assertThat(event.getFormattedMessage()).contains("dataSourceCount:2");
                    });
                });
    }

    @Test
    void GIVEN_two_datasources_and_one_primary_WHEN_the_context_starts_THEN_nothing_is_warned() {
        runner.withBean("ordersDataSource", DataSource.class, NoopDataSource::new)
                .withBean("tandemDataSource", DataSource.class, NoopDataSource::new, definition -> definition.setPrimary(true))
                .run(context -> assertThat(logs.list).isEmpty());
    }

    @Test
    void GIVEN_a_single_datasource_WHEN_the_context_starts_THEN_nothing_is_warned() {
        runner.withBean(DataSource.class, NoopDataSource::new)
                .run(context -> assertThat(logs.list).isEmpty());
    }

    @Test
    void GIVEN_two_datasources_and_a_user_repository_WHEN_the_context_starts_THEN_nothing_is_warned() {
        runner.withBean("ordersDataSource", DataSource.class, NoopDataSource::new)
                .withBean("tandemDataSource", DataSource.class, NoopDataSource::new)
                .withBean(OutboxRepository.class, InMemoryOutbox::new)
                .run(context -> assertThat(logs.list).isEmpty());
    }
}
