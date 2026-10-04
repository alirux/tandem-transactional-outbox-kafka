package com.codingful.tandem.kafka;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.MessageEncoder;
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
 * binding library rather than reassembled from a neutral value, and {@link CloudEventEncoder} does
 * exactly that. A format with nothing Kafka-specific about it implements {@link MessageEncoder} once
 * and reaches Kafka through {@link #from(MessageEncoder)}, and any other transport adapter unchanged.
 */
public interface KafkaMessageEncoder {

    /**
     * @param record the row about to be published
     * @return the record to send, keyed by {@code aggregate_id} so an aggregate's events share a partition
     * @throws RuntimeException if the row cannot be encoded; {@link KafkaRelay} fails it permanently (§4)
     */
    ProducerRecord<String, byte[]> encode(OutboxRecord record);

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
