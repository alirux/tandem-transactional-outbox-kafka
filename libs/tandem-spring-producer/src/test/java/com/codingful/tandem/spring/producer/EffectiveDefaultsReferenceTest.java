package com.codingful.tandem.spring.producer;

import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.boundKeys;
import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.resource;
import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.TracePropagator;
import com.codingful.tandem.test.InMemoryOutbox;
import com.codingful.tandem.test.reference.ReferenceFile;
import com.codingful.tandem.test.reference.ReferenceValues;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.LazyInitializationBeanFactoryPostProcessor;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

/**
 * Pins every {@code tandem.*} key the write side binds, at the value a Spring application gets when it
 * sets none (LLD-api-stability §8.3). The keys come from the module's generated configuration metadata,
 * so a key added without a line here fails the test.
 *
 * <p>The context holds a {@code DataSource}, the one bean startup needs, and never builds the JDBC
 * repository, which checks the bucket count against the database as it is built: this test has no
 * database. So the context starts lazily, and the events tier's listener, which a starting context builds
 * whatever its laziness and which would build the repository, is one over an in-memory outbox. The two
 * keys the autoconfiguration hands to the repository, the bucket count and the wakeup, are read from the
 * bound properties it hands them from. The others are read from the beans that apply them: the
 * transaction-binding guard, and the trace propagator, whose MDC key is read with
 * {@code tandem.tracing.enabled} set, the one key it needs to exist.
 */
class EffectiveDefaultsReferenceTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemProducerAutoConfiguration.class))
            .withInitializer(context -> context.addBeanFactoryPostProcessor(new LazyInitializationBeanFactoryPostProcessor()))
            .withBean(DataSource.class, NoopDataSource::new)
            .withBean(OutboxEventListener.class, () -> new OutboxEventListener(new InMemoryOutbox(),
                    OutboxEventMapperRegistry.of(List.of()), TransactionBindingGuard.disabled()));

    @Test
    void GIVEN_an_application_that_sets_no_tandem_key_WHEN_the_write_side_starts_THEN_the_effective_defaults_match_their_reference_file() {
        Map<String, String> lines = new TreeMap<>();

        runner.run(context -> {
            assertThat(context).hasNotFailed();
            TandemOutboxProperties outbox = context.getBean(TandemOutboxProperties.class);
            lines.put("tandem.outbox.bucket-count", ReferenceValues.of(outbox.bucketCount()));
            lines.put("tandem.outbox.wakeup", ReferenceValues.of(outbox.wakeup()));
            lines.put("tandem.outbox.verify-transaction-binding",
                    ReferenceValues.of(context.getBean(TransactionBindingGuard.class).checksBinding()));
            lines.put("tandem.tracing.enabled",
                    ReferenceValues.of(context.getBeanNamesForType(TracePropagator.class).length > 0));
        });

        runner.withPropertyValues("tandem.tracing.enabled=true").run(context -> {
            TracePropagator propagator = context.getBean(TracePropagator.class);
            assertThat(propagator).isInstanceOf(MdcCorrelationTracePropagator.class);
            lines.put("tandem.tracing.correlation-id-mdc-key",
                    ReferenceValues.of(((MdcCorrelationTracePropagator) propagator).mdcKey()));
        });

        assertThat(lines.keySet()).as("a line for every key the module binds, and for no other")
                .containsExactlyInAnyOrderElementsOf(boundKeys(resource(EffectiveDefaultsReferenceTest.class,
                        "/META-INF/spring-configuration-metadata.json")));
        List<String> rendered = new ArrayList<>();
        lines.forEach((key, value) -> rendered.add(key + "=" + value));
        ReferenceFile.of("effective-defaults.txt", EffectiveDefaultsReferenceTest.class).assertMatches(rendered);
    }
}
