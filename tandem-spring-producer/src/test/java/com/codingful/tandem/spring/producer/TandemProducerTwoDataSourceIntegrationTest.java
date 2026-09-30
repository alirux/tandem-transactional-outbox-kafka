package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.test.TandemTestContainer;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.transaction.TransactionAutoConfiguration;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import com.codingful.tandem.core.port.TandemAggregate;

/**
 * The phantom event (LLD-spring-producer §8): the domain transaction runs on a {@code DataSource} other than
 * the one Tandem writes to. Each write-side tier must refuse, and no outbox row may survive. Runs against a
 * real PostgreSQL; the "domain" {@code DataSource} is a second, separate one on the same database.
 */
@Tag("integration")
class TandemProducerTwoDataSourceIntegrationTest {

    private static TandemTestContainer container;

    @BeforeAll
    static void startContainer() {
        container = new TandemTestContainer().start();
    }

    @AfterAll
    static void stopContainer() {
        if (container != null) {
            container.close();
        }
    }

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemProducerAutoConfiguration.class, TransactionAutoConfiguration.class,
                    AopAutoConfiguration.class))
            .withUserConfiguration(TwoDataSources.class);

    private static long outboxRowCount(String aggregateId) {
        try (Connection connection = container.dataSource().getConnection();
                PreparedStatement statement = connection.prepareStatement(
                        "SELECT count(*) FROM tandem_outbox WHERE aggregate_id = ?")) {
            statement.setString(1, aggregateId);
            try (ResultSet resultSet = statement.executeQuery()) {
                resultSet.next();
                return resultSet.getLong(1);
            }
        } catch (SQLException e) {
            throw new IllegalStateException("counting outbox rows failed", e);
        }
    }

    private static OutboxMessage message(String aggregateId) {
        return OutboxMessage.builder().aggregateType("Order").aggregateId(aggregateId).seq(1L)
                .payload("{}".getBytes(StandardCharsets.UTF_8)).build();
    }

    @Test
    void GIVEN_a_domain_transaction_on_another_datasource_WHEN_an_event_is_published_THEN_it_is_refused_and_no_row_remains() {
        runner.run(context -> {
            TransactionTemplate domainTx = new TransactionTemplate(context.getBean("domainTx", PlatformTransactionManager.class));
            ApplicationEventPublisher publisher = context;

            assertThatThrownBy(() -> domainTx.executeWithoutResult(status -> publisher.publishEvent(message("two-ds-event"))))
                    .isInstanceOf(OutboxInsertException.class);
            assertThat(outboxRowCount("two-ds-event")).isZero();
        });
    }

    @Test
    void GIVEN_a_domain_transaction_on_another_datasource_WHEN_an_annotated_method_runs_THEN_it_is_refused_and_no_row_remains() {
        runner.run(context -> {
            TransactionTemplate domainTx = new TransactionTemplate(context.getBean("domainTx", PlatformTransactionManager.class));
            OrderService service = context.getBean(OrderService.class);

            assertThatThrownBy(() -> domainTx.executeWithoutResult(status -> service.place("two-ds-annotation")))
                    .isInstanceOf(OutboxInsertException.class);
            assertThat(outboxRowCount("two-ds-annotation")).isZero();
        });
    }

    @Test
    void GIVEN_the_template_bound_to_another_transaction_manager_WHEN_work_runs_THEN_it_is_refused_and_no_row_remains() {
        runner.run(context -> {
            TransactionalOutboxTemplate template = context.getBean(TransactionalOutboxTemplate.class);

            assertThatThrownBy(() -> template.executeWithoutResult(outbox ->
                    outbox.record("Order", "two-ds-template", 1L, new Payload("two-ds-template"))))
                    .isInstanceOf(OutboxInsertException.class);
            assertThat(outboxRowCount("two-ds-template")).isZero();
        });
    }

    @Test
    void GIVEN_verification_switched_off_and_no_bound_connection_WHEN_an_event_is_published_THEN_the_autocommit_is_still_refused() {
        runner.withPropertyValues("tandem.outbox.verify-transaction-binding=false").run(context -> {
            TransactionTemplate domainTx = new TransactionTemplate(context.getBean("domainTx", PlatformTransactionManager.class));

            assertThatThrownBy(() -> domainTx.executeWithoutResult(status -> context.publishEvent(message("two-ds-autocommit"))))
                    .isInstanceOf(OutboxInsertException.class);
            assertThat(outboxRowCount("two-ds-autocommit")).isZero();
        });
    }

    @Test
    void GIVEN_a_transaction_on_the_tandem_datasource_WHEN_an_event_is_published_THEN_the_row_is_inserted() {
        runner.run(context -> {
            TransactionTemplate tandemTx = new TransactionTemplate(context.getBean("tandemTx", PlatformTransactionManager.class));

            tandemTx.executeWithoutResult(status -> context.publishEvent(message("two-ds-ok")));

            assertThat(outboxRowCount("two-ds-ok")).isEqualTo(1);
        });
    }

    static class OrderService {
        @TransactionalOutbox
        public TandemAggregate place(String id) {
            return () -> List.of(message(id));
        }
    }

    record Payload(String id) {
    }

    @Configuration
    static class TwoDataSources {
        @Bean
        @Primary
        DataSource tandemDataSource() {
            return container.dataSource();
        }

        /** A separate pool on the same database: the application's own domain data source. */
        @Bean
        DataSource domainDataSource() {
            return new org.springframework.jdbc.datasource.DriverManagerDataSource(
                    container.postgresJdbcUrl(), container.postgresUsername(), container.postgresPassword());
        }

        @Bean
        PlatformTransactionManager tandemTx(@org.springframework.beans.factory.annotation.Qualifier("tandemDataSource") DataSource ds) {
            return new DataSourceTransactionManager(ds);
        }

        @Bean
        @Primary
        PlatformTransactionManager domainTx(@org.springframework.beans.factory.annotation.Qualifier("domainDataSource") DataSource ds) {
            return new DataSourceTransactionManager(ds);
        }

        @Bean
        OrderService orderService() {
            return new OrderService();
        }
    }
}
