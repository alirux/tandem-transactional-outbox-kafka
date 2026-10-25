package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.exception.PayloadSerializationException;
import com.codingful.tandem.core.port.PayloadSerializer;
import java.util.Objects;
import org.springframework.beans.factory.BeanFactory;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.json.JsonMapper;

/**
 * Jackson 3 ({@code tools.jackson}) {@link PayloadSerializer}, the generation a stock Spring Boot 4
 * application carries (LLD-spring-producer §2). Only {@link JacksonPayloadSerializers} touches this class,
 * and only once Jackson 3 is known to be on the classpath, so no other class of the module names a Jackson 3
 * type.
 */
final class Jackson3PayloadSerializer implements PayloadSerializer {

    private static final String CONTENT_TYPE = "application/json";

    private final JsonMapper jsonMapper;

    /**
     * @param jsonMapper the mapper to serialize with, typically the application's shared bean
     * @throws NullPointerException if {@code jsonMapper} is null
     */
    Jackson3PayloadSerializer(JsonMapper jsonMapper) {
        this.jsonMapper = Objects.requireNonNull(jsonMapper, "jsonMapper");
    }

    /** The serializer on the application's {@code JsonMapper} bean, or {@code null} when it has none. */
    static PayloadSerializer onApplicationMapper(BeanFactory beanFactory) {
        JsonMapper mapper = beanFactory.getBeanProvider(JsonMapper.class).getIfAvailable();
        return mapper == null ? null : new Jackson3PayloadSerializer(mapper);
    }

    static PayloadSerializer onDefaultMapper() {
        return new Jackson3PayloadSerializer(new JsonMapper());
    }

    @Override
    public byte[] serialize(Object payload) {
        try {
            return jsonMapper.writeValueAsBytes(payload);
        } catch (JacksonException e) {
            throw new PayloadSerializationException("Failed to serialize payload to JSON", e);
        }
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }
}
