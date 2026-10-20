package com.codingful.tandem.admin.internal;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * What the Admin API needs from Spring MVC itself, imported by the root autoconfiguration. The condition
 * sits on this class, not on its {@code @Bean} method: without Spring MVC the method's signature names a
 * type that cannot be loaded, and introspecting the class that declares it fails the whole context. A
 * class condition is read from metadata before the class is, so an application hosting the Admin API on
 * another stack never loads this one.
 */
@Configuration(proxyBeanMethods = false)
@ConditionalOnClass(name = "org.springframework.web.servlet.HandlerExceptionResolver")
public class TandemAdminServletConfiguration {

    /** Renders the 404 and 405 raised before any controller is chosen. */
    @Bean
    TandemAdminPathExceptionResolver tandemAdminPathExceptionResolver(
            @Value("${tandem.admin.base-path:/tandem/admin}") String basePath) {
        return new TandemAdminPathExceptionResolver(basePath);
    }
}
