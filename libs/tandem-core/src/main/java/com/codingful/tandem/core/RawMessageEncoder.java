package com.codingful.tandem.core;

import com.codingful.tandem.core.port.MessageEncoder;
import com.codingful.tandem.core.port.TopicRouter;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * The raw passthrough envelope (HLD-alternative-envelope §6, LLD-alternative-envelope §2): the stored
 * payload as the body, the aggregate id as the key, and the row's own headers, with no CloudEvents
 * attribute around them. Opt-in: the default envelope stays CloudEvents, and an application selects
 * this one by wiring it like any other {@link MessageEncoder}.
 *
 * <p>The wire contract, frozen under the additive rule (HLD §1.4):
 * <ul>
 *   <li>{@link RawHeaders#ID} carries the outbox row id, so consumers can deduplicate and a replay
 *       republishes the same id;</li>
 *   <li>{@link RawHeaders#TYPE} carries the event type, or the aggregate type when the row stored
 *       none;</li>
 *   <li>every row header follows verbatim and in stored order, except one named like raw's own
 *       headers (see {@link RawHeaders} for the {@code tandem-*} reservation);</li>
 *   <li>{@link TandemHeaders#CONTENT_TYPE} is always present when the record has a content type: the
 *       stored header, or the record's content type when the row stored none;</li>
 *   <li>no sequence number is published, and the ordering key is visible only as the broker key.</li>
 * </ul>
 *
 * <p>The destination comes from a {@link TopicRouter}. On Kafka it is the topic; lifted onto AMQP it
 * becomes the routing key, and there the router decides ordering: the ordered AMQP topology needs the
 * aggregate id as routing key ({@code r -> r.aggregateId().value()}), since every event of a type would
 * otherwise share one routing key (LLD-rabbitmq §7).
 *
 * <p>Stateless beyond its router, so thread-safe whenever the router is.
 */
public final class RawMessageEncoder implements MessageEncoder {

    private final TopicRouter router;

    /**
     * @param router names the destination of each record: the topic on Kafka, the routing key on AMQP
     * @throws NullPointerException if {@code router} is {@code null}
     */
    public RawMessageEncoder(TopicRouter router) {
        this.router = Objects.requireNonNull(router, "router");
    }

    @Override
    public EncodedMessage encode(OutboxRecord record) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put(RawHeaders.ID, utf8(String.valueOf(record.id())));
        // The same fallback the CloudEvents envelope applies (LLD-kafka §3.4), restated rather than
        // shared: the factory lives in a module the core cannot depend on.
        headers.put(RawHeaders.TYPE, utf8(record.type() != null ? record.type() : record.aggregateType()));
        record.headers().forEach((name, value) -> {
            if (!name.equals(RawHeaders.ID) && !name.equals(RawHeaders.TYPE)) {
                headers.put(name, utf8(value));
            }
        });
        // A row read from the store carries its content type as a header already; a record built by
        // hand may carry it only as the typed field. Raw has no attribute to hold it, so restore it.
        if (!record.headers().containsKey(TandemHeaders.CONTENT_TYPE) && record.contentType() != null) {
            headers.put(TandemHeaders.CONTENT_TYPE, utf8(record.contentType()));
        }
        return new EncodedMessage(router.topicFor(record), record.aggregateId().value(), record.payload(), headers);
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
