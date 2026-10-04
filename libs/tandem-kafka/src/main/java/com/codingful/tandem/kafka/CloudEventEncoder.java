package com.codingful.tandem.kafka;

import com.codingful.tandem.cloudevents.CloudEventFactory;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.TopicRouter;
import io.cloudevents.kafka.KafkaMessageFactory;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
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
public final class CloudEventEncoder implements KafkaMessageEncoder {

    private final TopicRouter router;
    private final CloudEventFactory factory;

    /**
     * @param router maps each record to its destination topic
     * @param cfg    the CloudEvents binding settings ({@code source}, default content type/schema)
     * @throws NullPointerException if either argument is {@code null}
     */
    public CloudEventEncoder(TopicRouter router, KafkaRelayConfig cfg) {
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

        factory.passthroughHeaders(record).forEach(
                (name, value) -> producerRecord.headers().add(name, value.getBytes(StandardCharsets.UTF_8)));
        return producerRecord;
    }
}
