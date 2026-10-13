package com.codingful.tandem.admin;

import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.boundKeys;
import static com.codingful.tandem.test.spring.ConfigurationMetadataReference.resource;
import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.test.reference.ReferenceFile;
import com.codingful.tandem.test.reference.ReferenceValues;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.WebApplicationContextRunner;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.servlet.config.annotation.EnableWebMvc;
import org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerMapping;

/**
 * Pins every {@code tandem.*} key the Admin API binds, at the value a Spring application gets when it sets
 * none (LLD-api-stability §8.3). The keys come from the module's configuration metadata, written by hand,
 * so a key added without a line fails the test, and the values are read off the running context rather
 * than off that metadata: whether the API's controllers exist, and the base path of the request
 * mappings Spring MVC builds from them. The base path is read with {@code tandem.admin.enabled} set, the
 * one key it needs to have any mapping at all.
 */
class EffectiveDefaultsReferenceTest {

    /** The contract-version segment every mapping carries after the base path (HLD-admin-api §1). */
    private static final String VERSION_SEGMENT = "/v1/";

    private final WebApplicationContextRunner runner = new WebApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(TandemAdminAutoConfiguration.class))
            .withUserConfiguration(Mvc.class)
            .withBean(DataSource.class, NoopDataSource::new);

    @Test
    void GIVEN_an_application_that_sets_no_tandem_key_WHEN_the_admin_api_starts_THEN_the_effective_defaults_match_their_reference_file() {
        Map<String, String> lines = new TreeMap<>();

        runner.run(context -> {
            assertThat(context).hasNotFailed();
            lines.put("tandem.admin.enabled",
                    ReferenceValues.of(context.getBeanNamesForAnnotation(RestController.class).length > 0));
        });

        runner.withPropertyValues("tandem.admin.enabled=true").run(context -> {
            Set<String> patterns = context.getBean(RequestMappingHandlerMapping.class).getHandlerMethods().keySet()
                    .stream().flatMap(mapping -> mapping.getPatternValues().stream()).collect(Collectors.toSet());
            Set<String> basePaths = patterns.stream()
                    .map(pattern -> pattern.substring(0, pattern.indexOf(VERSION_SEGMENT)))
                    .collect(Collectors.toSet());
            assertThat(basePaths).as("one base path for every mapping of %s", patterns).hasSize(1);
            lines.put("tandem.admin.base-path", basePaths.iterator().next());
        });

        assertThat(lines.keySet()).as("a line for every key the module binds, and for no other")
                .containsExactlyInAnyOrderElementsOf(boundKeys(resource(EffectiveDefaultsReferenceTest.class,
                        "/META-INF/spring-configuration-metadata.json")));
        List<String> rendered = new ArrayList<>();
        lines.forEach((key, value) -> rendered.add(key + "=" + value));
        ReferenceFile.of("effective-defaults.txt", EffectiveDefaultsReferenceTest.class).assertMatches(rendered);
    }

    /** Spring MVC's own infrastructure, which turns the controllers' mappings into request mappings. */
    @Configuration(proxyBeanMethods = false)
    @EnableWebMvc
    static class Mvc {
    }
}
