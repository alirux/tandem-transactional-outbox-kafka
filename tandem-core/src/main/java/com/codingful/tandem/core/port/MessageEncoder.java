package com.codingful.tandem.core.port;

import com.codingful.tandem.core.EncodedMessage;
import com.codingful.tandem.core.OutboxRecord;

/**
 * Message-format port (LLD-core §2.4): turns a stored row into the bytes and headers that go on the
 * wire, independently of which broker carries them.
 *
 * <p>This is the seam between <b>what</b> Tandem publishes and <b>where</b> it publishes it. The
 * format is the part a consumer sees and therefore the part an application is most likely to already
 * have decided (CloudEvents is Tandem's default, not its premise; HLD-cloudevents §8), and the
 * envelope is orthogonal to the payload encoding, which rides in {@code datacontenttype}. Splitting
 * it from {@link OutboxDispatcher} means a new format costs an encoder rather than a transport
 * adapter, and a new transport inherits every format already written.
 *
 * <p><b>This is what an application implements to publish an envelope of its own.</b> Written against
 * this port the envelope names no broker type, so the same class serves whatever transport adapter it
 * is deployed against; each adapter binds it to its own wire (in Spring, contributing a bean of this
 * type is all the wiring there is, LLD-spring-config §4.4).
 *
 * <p>An encoder is a <b>pure function of the record</b> and is called once per dispatch, possibly
 * from several relay worker threads at once, so an implementation must be thread-safe and must not
 * retain the message it returns (see {@link EncodedMessage}). A failure to encode is <b>permanent</b>
 * by construction: re-encoding the same row fails the same way, so a dispatcher must fail the row
 * rather than schedule a retry (LLD-kafka §4).
 *
 * <p>A format whose binding differs per transport (CloudEvents names its attributes {@code ce_*} on
 * Kafka and {@code cloudEvents_*} on AMQP) is encoded against the transport's own binding in that
 * transport's adapter instead ({@code KafkaMessageEncoder} in {@code tandem-kafka}), which is why
 * this port describes the neutral case rather than pretending every format has one shape.
 */
public interface MessageEncoder {

    /**
     * @param record the row about to be published
     * @return the wire-ready message; never {@code null}
     * @throws RuntimeException if the row cannot be encoded; treated as permanent by the dispatcher
     */
    EncodedMessage encode(OutboxRecord record);
}
