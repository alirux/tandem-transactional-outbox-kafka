package com.codingful.tandem.spring.producer;

import java.util.List;
import javax.sql.DataSource;
import com.codingful.tandem.core.port.OutboxRepository;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.ListableBeanFactory;
import org.springframework.beans.factory.ObjectProvider;
import org.springframework.boot.autoconfigure.AutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.context.annotation.Bean;

/**
 * Says so when {@link TandemProducerAutoConfiguration} backs off because the application has several
 * {@link DataSource}s and none is primary (LLD-spring-config §4.1). The back-off is silent by nature of
 * {@code @ConditionalOnSingleCandidate}, and the application then starts with no Tandem write side and no
 * error, so this is the one place the cause is named. It lives in its own autoconfiguration because the
 * producer's is exactly the one that does not load in this situation.
 */
@AutoConfiguration(afterName = {
        // Spring Boot 3.x
        "org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration",
        // Spring Boot 4.x
        "org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration"})
@ConditionalOnBean(DataSource.class)
public class TandemDataSourceDiagnosticsAutoConfiguration {

    private static final Logger LOG = LoggerFactory.getLogger(TandemDataSourceDiagnosticsAutoConfiguration.class);

    @Bean
    @ConditionalOnMissingBean(OutboxRepository.class)
    DataSourceAmbiguityCheck tandemDataSourceAmbiguityCheck(ObjectProvider<DataSource> dataSources,
            ListableBeanFactory beanFactory) {
        List<String> names = List.of(beanFactory.getBeanNamesForType(DataSource.class));
        if (names.size() > 1 && dataSources.getIfUnique() == null) {
            LOG.warn("Tandem write side not configured, several DataSources and none is primary;"
                    + " mark one @Primary or declare the Tandem beans explicitly dataSourceCount:{}, dataSourceBeans:{}",
                    names.size(), names);
        }
        return new DataSourceAmbiguityCheck();
    }

    /** Marker so the check runs once, as a bean, after the DataSources exist. */
    static final class DataSourceAmbiguityCheck {
    }
}
