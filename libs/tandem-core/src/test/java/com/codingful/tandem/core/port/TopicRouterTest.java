package com.codingful.tandem.core.port;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import java.time.Instant;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;

class TopicRouterTest {

    private static OutboxRecord recordOfType(String aggregateType) {
        OutboxMessage message = OutboxMessage.builder()
                .aggregateId("id-1")
                .aggregateType(aggregateType)
                .seq(1)
                .payload(new byte[] {0})
                .build();
        return OutboxRecord.builder().id(1).message(message).createdAt(Instant.EPOCH).build();
    }

    @ParameterizedTest(name = "{0} is routed to {1}")
    @CsvSource({
            "Order, order-topic",
            "OrderLine, order-line-topic",
            "HTTPServer, http-server-topic",
            "order, order-topic",
            "HTTP, http-topic",
            "OrderID, order-id-topic",
            "XMLHttpRequest, xml-http-request-topic",
            "S3Bucket, s3-bucket-topic"
    })
    void GIVEN_an_aggregate_type_WHEN_routed_THEN_the_topic_is_its_hyphenated_lower_case_name_plus_the_suffix(
            String aggregateType, String expectedTopic) {
        TopicRouter router = TopicRouter.kebabWithSuffix("-topic");

        assertThat(router.topicFor(recordOfType(aggregateType))).isEqualTo(expectedTopic);
    }
}