package com.codingful.tandem.test.encoder;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.port.MessageEncoder;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * A worked example of an application's own envelope (LLD-alternative-envelope §4.1), shaped like the
 * messages Debezium's outbox event router produces: the topic {@code outbox.event.<aggregate type>},
 * the aggregate id as key, the payload as body, and an {@value #ID_HEADER} header.
 *
 * <p>It illustrates that shape; it is neither faithful to Debezium nor supported by Tandem.
 *
 * <p>Debezium's {@code id} is typically a UUID, where this one is Tandem's numeric row id. That is the
 * first thing a real migration must reconcile, since consumers deduplicating on {@code id} see a
 * different value space.
 *
 * <p>A consumer built on Spring Messaging must read {@code id} from the raw transport header: Spring
 * reserves {@code id} on the message it builds and replaces it with its own.
 *
 * <p>A test fixture of this repository, never published. It is what the guide's custom-encoder
 * chapter walks through and what the custom-encoder end-to-end tests run on both transports.
 */
public final class DebeziumStyleEncoder implements MessageEncoder {

    /** The header carrying the event id: the outbox row id, as decimal text. */
    public static final String ID_HEADER = "id";

    @Override
    public EncodedMessage encode(OutboxRecord record) {
        Map<String, byte[]> headers = new LinkedHashMap<>();
        headers.put(ID_HEADER, String.valueOf(record.id()).getBytes(StandardCharsets.UTF_8));
        record.headers().forEach((name, value) -> {
            if (!name.equals(ID_HEADER)) {
                headers.put(name, value.getBytes(StandardCharsets.UTF_8));
            }
        });
        return new EncodedMessage("outbox.event." + record.aggregateType(), record.aggregateId().value(),
                record.payload(), headers);
    }
}
