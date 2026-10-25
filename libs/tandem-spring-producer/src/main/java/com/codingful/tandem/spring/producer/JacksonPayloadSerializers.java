package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.port.PayloadSerializer;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.core.env.Environment;
import org.springframework.util.ClassUtils;

/**
 * Chooses the Jackson generation the default {@link PayloadSerializer} runs on (LLD-spring-producer §2.1):
 * the payload is written with the mapper the application renders its HTTP responses with.
 *
 * <ol>
 *   <li>An application mapper of the generation Spring Boot prefers: Jackson 3's {@code JsonMapper}, unless
 *       {@value #PREFERRED_JSON_MAPPER} is {@code jackson2}, which makes it Jackson 2's
 *       {@code ObjectMapper}. This is the rule Spring Boot 4 applies to its own HTTP converters.</li>
 *   <li>Otherwise an application mapper of the other generation: a configured mapper beats a default
 *       one.</li>
 *   <li>Otherwise a plain mapper, Jackson 2's when that library is present, as before Jackson 3
 *       existed.</li>
 * </ol>
 *
 * <p>The choice is made when the bean is created, never by bean conditions, so it does not depend on the
 * order in which the Jackson autoconfigurations run. The class names below are resolved through the
 * application's class loader, and each generation's code lives in a class of its own, loaded only once its
 * library is known to be present.
 */
final class JacksonPayloadSerializers {

    static final String JACKSON_2_MAPPER = "com.fasterxml.jackson.databind.ObjectMapper";
    static final String JACKSON_3_MAPPER = "tools.jackson.databind.json.JsonMapper";
    static final String PREFERRED_JSON_MAPPER = "spring.http.converters.preferred-json-mapper";

    private static final String JACKSON_2 = "jackson2";

    private JacksonPayloadSerializers() {
    }

    /**
     * @return the serializer; never {@code null} as long as one of the two generations is present, which
     *         {@link OnJacksonCondition} guarantees
     */
    static PayloadSerializer forApplication(ConfigurableListableBeanFactory beanFactory, Environment environment) {
        ClassLoader classLoader = beanFactory.getBeanClassLoader();
        boolean jackson2 = ClassUtils.isPresent(JACKSON_2_MAPPER, classLoader);
        boolean jackson3 = ClassUtils.isPresent(JACKSON_3_MAPPER, classLoader);
        boolean jackson2Preferred = JACKSON_2.equalsIgnoreCase(environment.getProperty(PREFERRED_JSON_MAPPER));

        PayloadSerializer serializer = jackson2Preferred
                ? jackson2Mapper(beanFactory, jackson2)
                : jackson3Mapper(beanFactory, jackson3);
        if (serializer == null) {
            serializer = jackson2Preferred
                    ? jackson3Mapper(beanFactory, jackson3)
                    : jackson2Mapper(beanFactory, jackson2);
        }
        if (serializer == null) {
            serializer = jackson2
                    ? JacksonPayloadSerializer.onDefaultMapper()
                    : Jackson3PayloadSerializer.onDefaultMapper();
        }
        return serializer;
    }

    private static PayloadSerializer jackson2Mapper(ConfigurableListableBeanFactory beanFactory, boolean present) {
        return present ? JacksonPayloadSerializer.onApplicationMapper(beanFactory) : null;
    }

    private static PayloadSerializer jackson3Mapper(ConfigurableListableBeanFactory beanFactory, boolean present) {
        return present ? Jackson3PayloadSerializer.onApplicationMapper(beanFactory) : null;
    }
}
