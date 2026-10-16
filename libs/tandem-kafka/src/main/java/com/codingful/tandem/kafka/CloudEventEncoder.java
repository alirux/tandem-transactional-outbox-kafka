package com.codingful.tandem.kafka;

import com.codingful.tandem.cloudevents.CloudEventFactory;
import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.TopicRouter;
import io.cloudevents.SpecVersion;
import io.cloudevents.kafka.KafkaMessageFactory;
import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Tandem's default format (LLD-kafka §3): writes the CloudEvent a {@link CloudEventFactory} built as a
 * Kafka {@link ProducerRecord} in <b>binary</b> mode, so attributes become {@code ce_*} headers, the
 * payload becomes the body, and the record key is the {@code aggregate_id} (per-aggregate events then
 * share a partition).
 *
 * <p>This class is only the Kafka binding. What a consumer reads, meaning the attributes and their
 * sources, is decided by the factory and is the same event over any transport.
 */
final class CloudEventEncoder implements KafkaMessageEncoder {

    /**
     * The headers of every attribute the envelope owns: the spec's own and Tandem's extensions. A row
     * header with one of these names never reaches the wire, whether or not this event carries the
     * attribute: an application that copies a received {@code ce_id} onto its row would otherwise give
     * the event a second identity, and a forwarded {@code ce_seq} would number an event its writer
     * declared unsequenced.
     */
    private static final Set<String> RESERVED_HEADERS = reservedHeaders();

    private final TopicRouter router;
    private final CloudEventFactory factory;

    /**
     * @param router maps each record to its destination topic
     * @param cfg    the CloudEvents binding settings ({@code source}, default content type/schema)
     * @throws NullPointerException if either argument is {@code null}
     */
    CloudEventEncoder(TopicRouter router, KafkaRelayConfig cfg) {
        this.router = Objects.requireNonNull(router, "router");
        Objects.requireNonNull(cfg, "cfg");
        this.factory = new CloudEventFactory(cfg.source(), cfg.defaultContentType(), cfg.defaultDataSchema());
    }

    @Override
    public ProducerRecord<String, byte[]> encode(OutboxRecord record) {
        String topic = router.topicFor(record);
        String key = record.aggregateId().value();

        ProducerRecord<String, byte[]> producerRecord = KafkaMessageFactory.<String>createWriter(topic, key)
                .writeBinary(factory.build(record));

        factory.passthroughHeaders(record).forEach((name, value) -> {
            if (!RESERVED_HEADERS.contains(name)) {
                producerRecord.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });
        return producerRecord;
    }

    private static Set<String> reservedHeaders() {
        Set<String> attributes = new HashSet<>(SpecVersion.V1.getAllAttributes());
        attributes.add(CloudEventsHeaders.EXT_SEQ);
        attributes.add(CloudEventsHeaders.EXT_PARTITION_KEY);
        Set<String> headers = new HashSet<>();
        attributes.forEach(attribute -> headers.add(CloudEventsHeaders.BINARY_PREFIX + attribute));
        return Set.copyOf(headers);
    }
}
