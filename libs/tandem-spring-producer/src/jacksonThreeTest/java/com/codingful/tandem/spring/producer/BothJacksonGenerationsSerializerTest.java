package com.codingful.tandem.spring.producer;

import static com.codingful.tandem.spring.producer.SerializerContexts.JACKSON_2_AUTOCONFIGURATION;
import static com.codingful.tandem.spring.producer.SerializerContexts.JACKSON_2_KEBAB_CASE;
import static com.codingful.tandem.spring.producer.SerializerContexts.JACKSON_3_AUTOCONFIGURATION;
import static com.codingful.tandem.spring.producer.SerializerContexts.JACKSON_3_SNAKE_CASE;
import static com.codingful.tandem.spring.producer.SerializerContexts.PREFER_JACKSON_2;
import static com.codingful.tandem.spring.producer.SerializerContexts.WRITTEN_BY_JACKSON_2_BEAN;
import static com.codingful.tandem.spring.producer.SerializerContexts.WRITTEN_BY_JACKSON_3_BEAN;
import static com.codingful.tandem.spring.producer.SerializerContexts.serialized;
import static com.codingful.tandem.spring.producer.SerializerContexts.writeSide;
import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.port.PayloadSerializer;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

/**
 * The default serializer on a Spring Boot 4 classpath carrying both Jackson generations: an application
 * that opted back into Jackson 2, or one that has Jackson 2 only because another library brought it. Each
 * generation's mapper is configured with a naming of its own, so the JSON shows which one wrote it.
 */
@Tag("both-jackson-generations")
class BothJacksonGenerationsSerializerTest {

    @Test
    void GIVEN_jackson_two_brought_only_by_another_library_WHEN_an_object_payload_is_serialized_THEN_the_applications_jackson_three_mapper_writes_it() {
        writeSide(JACKSON_3_AUTOCONFIGURATION)
                .withPropertyValues(JACKSON_3_SNAKE_CASE)
                .run(context -> assertThat(serialized(context)).isEqualTo(WRITTEN_BY_JACKSON_3_BEAN));
    }

    @Test
    void GIVEN_both_mappers_and_no_preference_WHEN_an_object_payload_is_serialized_THEN_jackson_three_writes_it_as_spring_renders_http() {
        writeSide(JACKSON_3_AUTOCONFIGURATION, JACKSON_2_AUTOCONFIGURATION)
                .withPropertyValues(JACKSON_3_SNAKE_CASE, JACKSON_2_KEBAB_CASE)
                .run(context -> assertThat(serialized(context)).isEqualTo(WRITTEN_BY_JACKSON_3_BEAN));
    }

    @Test
    void GIVEN_both_mappers_and_jackson_two_preferred_WHEN_an_object_payload_is_serialized_THEN_jackson_two_writes_it() {
        writeSide(JACKSON_3_AUTOCONFIGURATION, JACKSON_2_AUTOCONFIGURATION)
                .withPropertyValues(JACKSON_3_SNAKE_CASE, JACKSON_2_KEBAB_CASE, PREFER_JACKSON_2)
                .run(context -> assertThat(serialized(context)).isEqualTo(WRITTEN_BY_JACKSON_2_BEAN));
    }

    @Test
    void GIVEN_only_a_jackson_two_mapper_WHEN_an_object_payload_is_serialized_THEN_it_writes_it_over_a_plain_jackson_three_one() {
        writeSide(JACKSON_2_AUTOCONFIGURATION)
                .withPropertyValues(JACKSON_2_KEBAB_CASE)
                .run(context -> assertThat(serialized(context)).isEqualTo(WRITTEN_BY_JACKSON_2_BEAN));
    }

    @Test
    void GIVEN_both_generations_and_no_mapper_bean_WHEN_the_context_starts_THEN_a_plain_jackson_two_mapper_is_used_as_before_jackson_three() {
        writeSide().run(context -> assertThat(context.getBean(PayloadSerializer.class))
                .isInstanceOf(JacksonPayloadSerializer.class));
    }
}
