package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.AMQP;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.junit.jupiter.api.Test;

class AmqpMessageTest {

    private static final String SECRET_PAYLOAD = "{\"iban\":\"IT60X0542811101000000123456\"}";
    private static final String SECRET_HEADER_VALUE = "Bearer super-secret-token";
    private static final String HEADER_NAME = "authorization";

    @Test
    void GIVEN_a_message_carrying_business_data_WHEN_it_is_rendered_for_a_log_THEN_only_its_shape_is_printed() {
        AmqpMessage message = new AmqpMessage(new AmqpRoute("tandem-events", "order-topic"),
                new AMQP.BasicProperties.Builder().headers(Map.of(HEADER_NAME, SECRET_HEADER_VALUE)).build(),
                SECRET_PAYLOAD.getBytes(StandardCharsets.UTF_8));

        assertThat(message.toString())
                .doesNotContain(SECRET_PAYLOAD)
                .doesNotContain(SECRET_HEADER_VALUE)
                .contains(HEADER_NAME)
                .contains("order-topic")
                .contains("bodyBytes=" + SECRET_PAYLOAD.getBytes(StandardCharsets.UTF_8).length);
    }

    @Test
    void GIVEN_a_message_with_no_headers_at_all_WHEN_it_is_rendered_for_a_log_THEN_it_still_renders() {
        AmqpMessage message = new AmqpMessage(new AmqpRoute("", "order-topic"),
                new AMQP.BasicProperties.Builder().build(), new byte[0]);

        assertThat(message.toString()).contains("headerNames=[]");
    }
}
