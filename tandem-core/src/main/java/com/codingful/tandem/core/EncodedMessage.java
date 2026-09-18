package com.codingful.tandem.core;

import java.util.Collections;
import java.util.Map;
import java.util.Objects;

/**
 * One wire-ready message, as a {@link com.codingful.tandem.core.port.MessageEncoder} produced it
 * (LLD-core §1.5): the transport-neutral shape every broker Tandem publishes to has in common, that
 * is a destination, an ordering key, a body and a flat set of headers.
 *
 * <p><b>Headers are {@code byte[]}</b>, the one representation no broker loses: Kafka headers are
 * bytes already, and a typed property model (AMQP) can narrow bytes to its own types, while the
 * reverse would force this value to pick a type system and every other transport to undo it.
 *
 * <p><b>This value hands over ownership and copies nothing.</b> It lives for exactly one dispatch on
 * the relay's hot path, between the encoder that built it and the transport adapter that writes it,
 * so cloning the body and every header value per message would be pure allocation. An encoder
 * therefore must not retain or mutate what it passes here. That is the opposite of
 * {@link OutboxMessage}, which the client holds and the store persists, and which does copy.
 */
public final class EncodedMessage {

    private final String destination;
    private final String key;
    private final byte[] body;
    private final Map<String, byte[]> headers;

    /**
     * @param destination where the transport adapter sends this message: a Kafka topic, an AMQP
     *                    exchange, whatever names a stream on the target broker
     * @param key         the ordering key, {@code aggregate_id} for every encoder Tandem ships: it is
     *                    what keeps an aggregate's events on one partition and so what makes the
     *                    published order observable (HLD §6). May be {@code null} only for a
     *                    transport that has no such concept
     * @param body        the message body; not copied, see the class javadoc
     * @param headers     the message headers; not copied, and iterated in its own order
     * @throws NullPointerException if {@code destination}, {@code body} or {@code headers} is {@code null}
     */
    public EncodedMessage(String destination, String key, byte[] body, Map<String, byte[]> headers) {
        this.destination = Objects.requireNonNull(destination, "destination");
        this.key = key;
        this.body = Objects.requireNonNull(body, "body");
        this.headers = Collections.unmodifiableMap(Objects.requireNonNull(headers, "headers"));
    }

    public String destination() {
        return destination;
    }

    /** The ordering key; {@code null} only where the transport has no such concept. */
    public String key() {
        return key;
    }

    public byte[] body() {
        return body;
    }

    public Map<String, byte[]> headers() {
        return headers;
    }

    @Override
    public String toString() {
        return "EncodedMessage{destination=" + destination
                + ", key=" + key
                + ", bodyBytes=" + body.length
                + ", headerNames=" + headers.keySet() + '}';
    }
}
