package com.codingful.tandem.kafka;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.MessageEncoder;
import com.codingful.tandem.core.port.TopicRouter;
import java.util.Objects;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * The message-format port as Kafka sees it (LLD-kafka §3): a {@link MessageEncoder} that writes a
 * {@link ProducerRecord} directly. {@link KafkaRelay} takes one, so the published format is a choice
 * a consumer of this module makes rather than something the dispatcher decides.
 *
 * <p>It exists alongside the transport-neutral {@link MessageEncoder} because the formats worth
 * having are split between the two. A format with a Kafka-specific binding (CloudEvents, whose
 * attributes become {@code ce_*} headers here and {@code cloudEvents_*} over AMQP) is built with the
 * binding library rather than reassembled from a neutral value, and {@link #cloudEvents} does exactly
 * that. A format with nothing Kafka-specific about it implements {@link MessageEncoder} once and
 * reaches Kafka through {@link #from(MessageEncoder)}, and any other transport adapter unchanged.
 *
 * <p><b>SPI:</b> the application implements this interface and Tandem calls it. A method added in a
 * minor version is always {@code default}, so an existing implementation keeps working
 * (HLD-api-stability §4.1).
 */
public interface KafkaMessageEncoder {

    /**
     * @param record the row about to be published
     * @return the record to send, keyed by {@code aggregate_id} so an aggregate's events share a partition
     * @throws RuntimeException if the row cannot be encoded; {@link KafkaRelay} fails it permanently (§4)
     */
    ProducerRecord<String, byte[]> encode(OutboxRecord record);

    /**
     * Tandem's default format: the record as a CloudEvent in Kafka's <b>binary</b> mode, its attributes
     * in {@code ce_*} headers, the payload as the body and the {@code aggregate_id} as the record key.
     *
     * @param router maps each record to its destination topic
     * @param cfg    the CloudEvents settings ({@code source}, default content type and schema)
     * @return the CloudEvents encoder
     * @throws NullPointerException if either argument is {@code null}
     */
    static KafkaMessageEncoder cloudEvents(TopicRouter router, KafkaRelayConfig cfg) {
        return new CloudEventEncoder(router, cfg);
    }

    /**
     * Lifts a transport-neutral encoder onto Kafka: the destination becomes the topic, the ordering
     * key the record key, and each header a Kafka header verbatim.
     *
     * @param encoder the neutral encoder to adapt
     * @return a Kafka encoder delegating to it
     * @throws NullPointerException if {@code encoder} is {@code null}
     */
    static KafkaMessageEncoder from(MessageEncoder encoder) {
        Objects.requireNonNull(encoder, "encoder");
        return record -> {
            EncodedMessage message = encoder.encode(record);
            ProducerRecord<String, byte[]> producerRecord =
                    new ProducerRecord<>(message.destination(), message.key(), message.body());
            message.headers().forEach((name, value) -> producerRecord.headers().add(name, value));
            return producerRecord;
        };
    }
}
