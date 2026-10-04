package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ShutdownSignalException;
import java.io.IOException;
import java.net.SocketTimeoutException;
import org.junit.jupiter.api.Test;

class DefaultRabbitErrorClassifierTest {

    private final DefaultRabbitErrorClassifier classifier = new DefaultRabbitErrorClassifier();

    private static ShutdownSignalException channelClosedWith(int replyCode, String replyText) {
        AMQP.Channel.Close reason = new AMQP.Channel.Close.Builder()
                .replyCode(replyCode).replyText(replyText).classId(60).methodId(40).build();
        return new ShutdownSignalException(false, false, reason, "channel");
    }

    @Test
    void GIVEN_an_exchange_that_does_not_exist_WHEN_the_publish_fails_THEN_the_row_is_not_retried() {
        IOException failure = new IOException(channelClosedWith(404, "NOT_FOUND - no exchange"));

        assertThat(classifier.classify(failure).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_credentials_without_permission_to_publish_WHEN_the_publish_fails_THEN_the_row_is_not_retried() {
        assertThat(classifier.classify(channelClosedWith(403, "ACCESS_REFUSED")).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_a_broker_precondition_the_publish_violates_WHEN_it_fails_THEN_the_row_is_not_retried() {
        assertThat(classifier.classify(channelClosedWith(406, "PRECONDITION_FAILED")).isRetriable()).isFalse();
    }

    @Test
    void GIVEN_the_whole_connection_refused_for_permissions_WHEN_the_publish_fails_THEN_the_row_is_not_retried() {
        AMQP.Connection.Close reason = new AMQP.Connection.Close.Builder()
                .replyCode(403).replyText("ACCESS_REFUSED").classId(10).methodId(40).build();

        assertThat(classifier.classify(new ShutdownSignalException(true, false, reason, "connection")).isRetriable())
                .isFalse();
    }

    @Test
    void GIVEN_a_broker_that_went_away_WHEN_the_publish_fails_THEN_the_row_is_retried() {
        assertThat(classifier.classify(channelClosedWith(320, "CONNECTION_FORCED")).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_network_failure_WHEN_the_publish_fails_THEN_the_row_is_retried() {
        assertThat(classifier.classify(new SocketTimeoutException("read timed out")).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_failure_nobody_anticipated_WHEN_the_publish_fails_THEN_the_row_is_retried_rather_than_abandoned() {
        assertThat(classifier.classify(new IllegalStateException("something new")).isRetriable()).isTrue();
    }

    @Test
    void GIVEN_a_failure_WHEN_it_is_classified_THEN_the_verdict_and_the_cause_survive_into_the_message() {
        assertThat(classifier.classify(new SocketTimeoutException("read timed out")))
                .hasMessageContaining("retriable")
                .hasMessageContaining("SocketTimeoutException")
                .hasMessageContaining("read timed out");
    }
}
