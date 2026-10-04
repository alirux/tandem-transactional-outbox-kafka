package com.codingful.tandem.rabbitmq;

import static org.assertj.core.api.Assertions.assertThat;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import java.time.Instant;
import org.junit.jupiter.api.Test;

class RabbitRouterTest {

    private static final String EXCHANGE = "tandem-events";

    private static OutboxRecord record(String aggregateId, String aggregateType) {
        return OutboxRecord.builder()
                .id(1)
                .message(OutboxMessage.builder()
                        .aggregateId(aggregateId).aggregateType(aggregateType).seq(1).payload("{}".getBytes()).build())
                .createdAt(Instant.parse("2024-01-01T00:00:00Z"))
                .build();
    }

    @Test
    void GIVEN_a_consistent_hash_exchange_WHEN_a_row_is_routed_THEN_its_aggregate_decides_the_queue_it_lands_on() {
        AmqpRoute route = RabbitRouter.byAggregateId(EXCHANGE).routeFor(record("order-1", "Order"));

        assertThat(route).isEqualTo(new AmqpRoute(EXCHANGE, "order-1"));
    }
}
