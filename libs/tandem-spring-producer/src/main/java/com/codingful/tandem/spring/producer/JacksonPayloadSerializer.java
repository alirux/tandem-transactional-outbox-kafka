package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.exception.PayloadSerializationException;
import com.codingful.tandem.core.port.PayloadSerializer;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.util.Objects;
import org.springframework.beans.factory.BeanFactory;

/**
 * Jackson 2 ({@code com.fasterxml.jackson}) {@link PayloadSerializer}, the optional object-payload path for
 * the write-side tiers on the generation Spring Boot 3 carries (LLD-spring-producer §2). It is
 * auto-configured only when Jackson is on the consumer's classpath and is never forced, so the
 * {@code byte[]} path stays dependency-free; {@link JacksonPayloadSerializers} decides between it and
 * {@link Jackson3PayloadSerializer}.
 */
final class JacksonPayloadSerializer implements PayloadSerializer {

    private static final String CONTENT_TYPE = "application/json";

    private final ObjectMapper objectMapper;

    /**
     * @param objectMapper the mapper to serialize with — typically the application's shared bean
     * @throws NullPointerException if {@code objectMapper} is null
     */
    JacksonPayloadSerializer(ObjectMapper objectMapper) {
        this.objectMapper = Objects.requireNonNull(objectMapper, "objectMapper");
    }

    /** The serializer on the application's {@code ObjectMapper} bean, or {@code null} when it has none. */
    static PayloadSerializer onApplicationMapper(BeanFactory beanFactory) {
        ObjectMapper mapper = beanFactory.getBeanProvider(ObjectMapper.class).getIfAvailable();
        return mapper == null ? null : new JacksonPayloadSerializer(mapper);
    }

    static PayloadSerializer onDefaultMapper() {
        return new JacksonPayloadSerializer(new ObjectMapper());
    }

    @Override
    public byte[] serialize(Object payload) {
        try {
            return objectMapper.writeValueAsBytes(payload);
        } catch (JsonProcessingException e) {
            throw new PayloadSerializationException("Failed to serialize payload to JSON", e);
        }
    }

    @Override
    public String contentType() {
        return CONTENT_TYPE;
    }
}
