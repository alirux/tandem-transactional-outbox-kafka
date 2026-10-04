package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.TopicRouter;
import java.util.Objects;

/**
 * Maps a record to its {@link AmqpRoute} (LLD-rabbitmq §6). The routing source field is
 * {@code aggregate_type}, not the finer CloudEvents {@code type}, for the same reason as on Kafka
 * (LLD-kafka §5): the granularity a consumer binds to is the aggregate.
 */
@FunctionalInterface
public interface RabbitRouter {

    /**
     * @param record the row about to be published
     * @return where to publish it
     */
    AmqpRoute routeFor(OutboxRecord record);

    /**
     * The default router: one configured exchange, and {@code kebab-case(aggregateType) + suffix} as
     * the routing key. Deliberately the same transform as {@link TopicRouter#kebabWithSuffix}, so that
     * the two adapters name the same aggregate the same way.
     *
     * @param exchange the exchange every record is published to
     * @param suffix   appended to the kebab-cased aggregate type, e.g. {@code -topic}
     * @return the router
     * @throws NullPointerException if either argument is {@code null}
     */
    static RabbitRouter kebabWithSuffix(String exchange, String suffix) {
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(suffix, "suffix");
        return record -> new AmqpRoute(exchange, TopicRouter.kebabCase(record.aggregateType()) + suffix);
    }

    /**
     * Routes by {@code aggregate_id}, which is what a <b>consistent-hash exchange</b> needs to keep an
     * aggregate's events on one queue and therefore in order under competing consumers (LLD-rabbitmq
     * §7). The structural equivalent of publishing to Kafka with the aggregate id as the partition key.
     *
     * @param exchange the consistent-hash exchange every record is published to
     * @return the router
     * @throws NullPointerException if {@code exchange} is {@code null}
     */
    static RabbitRouter byAggregateId(String exchange) {
        Objects.requireNonNull(exchange, "exchange");
        return record -> new AmqpRoute(exchange, record.aggregateId().value());
    }
}
