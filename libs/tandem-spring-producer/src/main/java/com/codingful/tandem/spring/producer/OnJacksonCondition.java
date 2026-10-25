package com.codingful.tandem.spring.producer;

import org.springframework.boot.autoconfigure.condition.AnyNestedCondition;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;

/**
 * Matches when either Jackson generation is on the classpath, the condition of the default
 * {@code PayloadSerializer} (LLD-spring-producer §2). The classes are named as strings, so the condition
 * stays readable in an application that has neither.
 */
final class OnJacksonCondition extends AnyNestedCondition {

    OnJacksonCondition() {
        super(ConfigurationPhase.REGISTER_BEAN);
    }

    @ConditionalOnClass(name = JacksonPayloadSerializers.JACKSON_2_MAPPER)
    static final class Jackson2Present {
    }

    @ConditionalOnClass(name = JacksonPayloadSerializers.JACKSON_3_MAPPER)
    static final class Jackson3Present {
    }
}
