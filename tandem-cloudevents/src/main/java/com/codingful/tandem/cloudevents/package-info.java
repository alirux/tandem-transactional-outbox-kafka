/**
 * The CloudEvents envelope, independent of the transport that carries it (HLD-cloudevents §5):
 * {@link com.codingful.tandem.cloudevents.CloudEventFactory} turns an
 * {@link com.codingful.tandem.core.OutboxRecord} into the {@code CloudEvent} a consumer reads, and
 * names the stored headers a transport passes through verbatim.
 *
 * <p>The mapping onto the wire is deliberately <b>not</b> here, because it is per transport: the
 * attributes become {@code ce_*} Kafka headers under one binding and {@code cloudEvents_*}
 * application-properties under the AMQP one. Each transport adapter owns its binding and depends on
 * this module for the event itself, so a new adapter inherits every decision a consumer can observe
 * rather than restating it.
 *
 * <p>Relay-side only, never on the client write-side (§1.3): the write side stores the {@code type}
 * and the payload, and the relay builds the envelope from the row.
 */
package com.codingful.tandem.cloudevents;
