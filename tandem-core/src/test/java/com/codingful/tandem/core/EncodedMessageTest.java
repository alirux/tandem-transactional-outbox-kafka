package com.codingful.tandem.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;

class EncodedMessageTest {

    private static EncodedMessage messageWith(byte[] body, Map<String, byte[]> headers) {
        return new EncodedMessage("order-topic", "order-1", body, headers);
    }

    @Test
    void GIVEN_a_sensitive_body_and_header_value_WHEN_toString_THEN_neither_appears_but_structural_fields_do() {
        String piiInBody = "email:jane@example.com";
        String secretHeaderValue = "Bearer super-secret-token";
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put("authorization", secretHeaderValue.getBytes(StandardCharsets.UTF_8));
        byte[] body = piiInBody.getBytes(StandardCharsets.UTF_8);

        String rendered = messageWith(body, headers).toString();

        assertThat(rendered)
                .doesNotContain(piiInBody)
                .doesNotContain(secretHeaderValue)
                .contains("bodyBytes=" + body.length)
                .contains("headerNames=")
                .contains("authorization")
                .contains("destination=order-topic");
    }

    @Test
    void GIVEN_an_encoded_message_WHEN_a_transport_adapter_reads_its_headers_THEN_it_cannot_add_one_of_its_own() {
        EncodedMessage message = messageWith(new byte[] {1}, new LinkedHashMap<>());

        assertThatThrownBy(() -> message.headers().put("injected", new byte[] {2}))
                .isInstanceOf(UnsupportedOperationException.class);
    }

    @Test
    void GIVEN_a_transport_with_no_ordering_key_WHEN_a_message_is_built_without_one_THEN_it_is_accepted() {
        EncodedMessage message = new EncodedMessage("order-topic", null, new byte[] {1}, Map.of());

        assertThat(message.key()).isNull();
    }
}
