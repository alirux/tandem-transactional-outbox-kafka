package com.codingful.tandem.cloudevents;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.TandemHeaders;
import io.cloudevents.CloudEvent;
import io.cloudevents.core.builder.CloudEventBuilder;
import java.net.URI;
import java.time.ZoneOffset;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Builds the CloudEvent for a stored row (HLD-cloudevents §3, §5), with no opinion on which broker
 * carries it.
 *
 * <p>The envelope is the same event whether it goes out over Kafka or AMQP; only the mapping onto the
 * wire differs, and that mapping belongs to the transport adapter (LLD-kafka §3). Everything a
 * consumer reads is decided here: the attribute sources, the {@code type} fallback, where
 * {@code datacontenttype} and {@code dataschema} come from, and whether {@code seq} is emitted at all.
 *
 * <p>Immutable and thread-safe: the relay's workers share one instance.
 */
public final class CloudEventFactory {

    private final URI source;
    private final String defaultContentType;
    private final URI defaultDataSchema;

    /**
     * @param source             the CloudEvents {@code source} attribute, one configured URI for the
     *                           whole outbox (HLD-cloudevents §8); per-{@code aggregate_type}
     *                           derivation can be added later without breaking consumers
     * @param defaultContentType the {@code datacontenttype} for a row that stored none, e.g.
     *                           {@code application/json}
     * @param defaultDataSchema  the {@code dataschema} for a row that stored none; {@code null} to
     *                           omit the attribute, which is the usual case outside schema-registry
     *                           setups (HLD-cloudevents §7)
     * @throws NullPointerException if {@code source} or {@code defaultContentType} is {@code null}
     */
    public CloudEventFactory(URI source, String defaultContentType, URI defaultDataSchema) {
        this.source = Objects.requireNonNull(source, "source");
        this.defaultContentType = Objects.requireNonNull(defaultContentType, "defaultContentType");
        this.defaultDataSchema = defaultDataSchema;
    }

    /**
     * @param record the row about to be published
     * @return the event to put on the wire, with the payload as its {@code data}
     */
    public CloudEvent build(OutboxRecord record) {
        String subject = record.aggregateId().value();
        CloudEventBuilder builder = CloudEventBuilder.v1()
                .withId(String.valueOf(record.id()))                 // id = outbox id (§6)
                .withSource(source)
                .withType(typeOf(record))                            // null `type` falls back to aggregate_type (Q20)
                .withSubject(subject)
                .withTime(record.createdAt().atOffset(ZoneOffset.UTC));

        String contentType = contentTypeOf(record);
        URI dataSchema = dataSchemaOf(record);
        if (dataSchema != null) {
            builder.withData(contentType, dataSchema, record.payload());
        } else {
            builder.withData(contentType, record.payload());
        }

        // ce_seq only for a row that has a number: omitted for one written unsequenced
        // (HLD-managed-seq §4.5). Consumers deduplicate on ce_id, unique by construction, so the
        // extension's absence costs them nothing, while emitting a 0 in its place would be a
        // sequence number that belongs to no event.
        if (record.hasSeq()) {
            builder.withExtension(CloudEventsHeaders.EXT_SEQ, record.seq());
        }
        // Always = the transport's ordering key, so a consumer can see which key the event was
        // partitioned by without knowing the broker's own key concept.
        builder.withExtension(CloudEventsHeaders.EXT_PARTITION_KEY, subject);
        return builder.build();
    }

    /**
     * The stored headers a transport adapter passes through verbatim: everything except the ones the
     * envelope already carries as attributes, which would otherwise reach the consumer twice under two
     * names ({@code content-type} as {@code datacontenttype}, {@code dataschema} as its own attribute).
     *
     * @param record the row about to be published
     * @return the headers to copy onto the message, in the order they were stored; never {@code null}
     */
    public Map<String, String> passthroughHeaders(OutboxRecord record) {
        Map<String, String> passthrough = new LinkedHashMap<>();
        record.headers().forEach((name, value) -> {
            if (!name.equals(TandemHeaders.CONTENT_TYPE) && !name.equals(TandemHeaders.DATA_SCHEMA)) {
                passthrough.put(name, value);
            }
        });
        return passthrough;
    }

    /** CloudEvents {@code type} is required; fall back to {@code aggregate_type} when the column is null (Q20). */
    private static String typeOf(OutboxRecord record) {
        return record.type() != null ? record.type() : record.aggregateType();
    }

    private String contentTypeOf(OutboxRecord record) {
        String stored = record.headers().get(TandemHeaders.CONTENT_TYPE);
        if (stored != null) {
            return stored;
        }
        return record.contentType() != null ? record.contentType() : defaultContentType;
    }

    private URI dataSchemaOf(OutboxRecord record) {
        String stored = record.headers().get(TandemHeaders.DATA_SCHEMA);
        if (stored != null) {
            return URI.create(stored);
        }
        return defaultDataSchema;
    }
}
