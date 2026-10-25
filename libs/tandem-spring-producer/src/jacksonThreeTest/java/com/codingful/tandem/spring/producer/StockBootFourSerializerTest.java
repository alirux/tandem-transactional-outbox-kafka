package com.codingful.tandem.spring.producer;

import static com.codingful.tandem.spring.producer.SerializerContexts.JACKSON_3_AUTOCONFIGURATION;
import static com.codingful.tandem.spring.producer.SerializerContexts.JACKSON_3_SNAKE_CASE;
import static com.codingful.tandem.spring.producer.SerializerContexts.PREFER_JACKSON_2;
import static com.codingful.tandem.spring.producer.SerializerContexts.WRITTEN_BY_A_PLAIN_MAPPER;
import static com.codingful.tandem.spring.producer.SerializerContexts.WRITTEN_BY_JACKSON_3_BEAN;
import static com.codingful.tandem.spring.producer.SerializerContexts.serialized;
import static com.codingful.tandem.spring.producer.SerializerContexts.writeSide;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.PayloadSerializationException;
import com.codingful.tandem.core.port.PayloadSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The default serializer on a stock Spring Boot 4 classpath: Jackson 3 and no Jackson 2 databind, which
 * the first test asserts, so the others mean what they claim.
 */
@Tag("jackson3-only")
class StockBootFourSerializerTest {

    static final class Unreadable {
        public String getState() {
            throw new IllegalStateException("not readable");
        }
    }

    @Test
    void GIVEN_this_classpath_WHEN_jackson_two_is_looked_up_THEN_it_is_absent_as_it_is_in_a_stock_application() {
        assertThatThrownBy(() -> Class.forName(JacksonPayloadSerializers.JACKSON_2_MAPPER))
                .isInstanceOf(ClassNotFoundException.class);
    }

    @Test
    void GIVEN_a_stock_boot_four_application_WHEN_an_object_payload_is_serialized_THEN_the_applications_mapper_writes_it() {
        writeSide(JACKSON_3_AUTOCONFIGURATION)
                .withPropertyValues(JACKSON_3_SNAKE_CASE)
                .run(context -> assertThat(serialized(context)).isEqualTo(WRITTEN_BY_JACKSON_3_BEAN));
    }

    @Test
    void GIVEN_a_stock_boot_four_application_WHEN_the_serializer_is_asked_for_its_content_type_THEN_it_is_json() {
        writeSide(JACKSON_3_AUTOCONFIGURATION)
                .run(context -> assertThat(context.getBean(PayloadSerializer.class).contentType())
                        .isEqualTo("application/json"));
    }

    @Test
    void GIVEN_jackson_three_without_a_mapper_bean_WHEN_an_object_payload_is_serialized_THEN_a_plain_mapper_writes_it() {
        writeSide().run(context -> assertThat(serialized(context)).isEqualTo(WRITTEN_BY_A_PLAIN_MAPPER));
    }

    @Test
    void GIVEN_jackson_two_preferred_but_absent_WHEN_an_object_payload_is_serialized_THEN_the_jackson_three_mapper_writes_it() {
        writeSide(JACKSON_3_AUTOCONFIGURATION)
                .withPropertyValues(JACKSON_3_SNAKE_CASE, PREFER_JACKSON_2)
                .run(context -> assertThat(serialized(context)).isEqualTo(WRITTEN_BY_JACKSON_3_BEAN));
    }

    @Test
    void GIVEN_a_payload_jackson_three_cannot_serialize_WHEN_it_is_serialized_THEN_it_fails_with_a_tandem_exception() {
        // Unlike Jackson 2, Jackson 3 writes a bare Object as {}; a property that cannot be read fails both.
        writeSide(JACKSON_3_AUTOCONFIGURATION)
                .run(context -> assertThatThrownBy(() -> context.getBean(PayloadSerializer.class).serialize(new Unreadable()))
                        .isInstanceOf(PayloadSerializationException.class));
    }
}
