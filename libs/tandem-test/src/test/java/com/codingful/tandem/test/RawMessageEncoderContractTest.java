package com.codingful.tandem.test;

import static org.assertj.core.api.Assertions.assertThatCode;

import com.codingful.tandem.core.RawHeaders;
import com.codingful.tandem.core.RawMessageEncoder;
import com.codingful.tandem.core.port.TopicRouter;
import org.junit.jupiter.api.Test;

class RawMessageEncoderContractTest {

    @Test
    void GIVEN_the_raw_passthrough_envelope_WHEN_it_is_held_to_the_encoder_contract_THEN_it_keeps_every_invariant() {
        MessageEncoderContract contract = MessageEncoderContract
                .of(new RawMessageEncoder(TopicRouter.kebabWithSuffix("-topic")), MessageEncoderContract.header(RawHeaders.ID))
                .consumesHeaders(RawHeaders.ID, RawHeaders.TYPE);

        assertThatCode(contract::verify).doesNotThrowAnyException();
    }
}
