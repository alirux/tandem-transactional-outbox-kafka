package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.cloudevents.CloudEventFactory;
import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxRecord;
import com.rabbitmq.client.AMQP;
import io.cloudevents.CloudEvent;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Tandem's default format on AMQP (LLD-rabbitmq §5): writes the CloudEvent a {@link CloudEventFactory}
 * built in <b>binary</b> mode, so attributes become {@code cloudEvents_*} headers, {@code
 * datacontenttype} becomes the AMQP {@code contentType} property, and the payload becomes the body.
 *
 * <p>This class is only the AMQP binding. What a consumer reads, meaning the attributes and their
 * sources, is decided by the factory and is the same event over any transport. The binding is written
 * by hand because the CloudEvents SDK ships one for AMQP 1.0 (Proton) and none for 0.9.1.
 */
public final class CloudEventAmqpEncoder implements RabbitMessageEncoder {

    private static final String PREFIX = CloudEventsHeaders.AMQP_BINARY_PREFIX;

    private final RabbitRouter router;
    private final CloudEventFactory factory;

    /**
     * @param router maps each record to its exchange and routing key
     * @param cfg    the CloudEvents binding settings ({@code source}, default content type/schema)
     * @throws NullPointerException if either argument is {@code null}
     */
    public CloudEventAmqpEncoder(RabbitRouter router, RabbitRelayConfig cfg) {
        this.router = Objects.requireNonNull(router, "router");
        Objects.requireNonNull(cfg, "cfg");
        this.factory = new CloudEventFactory(cfg.source(), cfg.defaultContentType(), cfg.defaultDataSchema());
    }

    @Override
    public AmqpMessage encode(OutboxRecord record) {
        CloudEvent event = factory.build(record);

        Map<String, Object> headers = new LinkedHashMap<>();
        headers.put(PREFIX + "specversion", event.getSpecVersion().toString());
        headers.put(PREFIX + "id", event.getId());
        headers.put(PREFIX + "source", event.getSource().toString());
        headers.put(PREFIX + "type", event.getType());
        if (event.getSubject() != null) {
            headers.put(PREFIX + "subject", event.getSubject());
        }
        if (event.getTime() != null) {
            headers.put(PREFIX + "time", event.getTime().format(DateTimeFormatter.ISO_OFFSET_DATE_TIME));
        }
        if (event.getDataSchema() != null) {
            headers.put(PREFIX + "dataschema", event.getDataSchema().toString());
        }
        // Extensions (seq when the row has one, partitionkey always) carry whatever type the factory set,
        // rendered as text: an AMQP field table would otherwise encode a long and a string differently
        // for the same attribute depending on the row.
        for (String name : event.getExtensionNames()) {
            headers.put(PREFIX + name, String.valueOf(event.getExtension(name)));
        }
        // datacontenttype rides the contentType property, not a header: that is what the AMQP binding
        // specifies and where an AMQP consumer looks for it.
        factory.passthroughHeaders(record).forEach(headers::put);

        AMQP.BasicProperties properties = new AMQP.BasicProperties.Builder()
                .contentType(event.getDataContentType())
                .headers(headers)
                .build();

        byte[] body = event.getData() == null ? new byte[0] : event.getData().toBytes();
        return new AmqpMessage(router.routeFor(record), properties, body);
    }
}
