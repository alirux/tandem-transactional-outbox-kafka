/**
 * The hexagonal <b>ports</b> (LLD-core §2): interfaces the core defines and the adapters implement,
 * so the core never depends on any adapter. Persistence ports
 * ({@link com.codingful.tandem.core.port.OutboxRepository} write-side,
 * {@link com.codingful.tandem.core.port.OutboxStore} relay-side,
 * {@link com.codingful.tandem.core.port.OutboxQuery} the Admin API's read side,
 * {@link com.codingful.tandem.core.port.DiscardService} the Admin API's discard action,
 * {@link com.codingful.tandem.core.port.RelayQuery}/{@link com.codingful.tandem.core.port.RelayControl}
 * the Admin API's relay observability and control) are implemented by {@code tandem-jdbc}; the
 * publish port ({@link com.codingful.tandem.core.port.OutboxDispatcher}) by {@code tandem-kafka}.
 *
 * <p>{@link com.codingful.tandem.core.port.MessageEncoder} splits <i>what</i> goes on the wire from
 * <i>where</i> it goes, so a new format costs an encoder rather than a transport adapter. A format
 * whose binding differs per transport, CloudEvents among them, is encoded against that transport's own
 * binding in its adapter instead ({@code KafkaMessageEncoder}).
 *
 * <p>The optional ports ({@link com.codingful.tandem.core.port.TandemMetrics},
 * {@link com.codingful.tandem.core.port.TracePropagator},
 * {@link com.codingful.tandem.core.port.TandemSpanRecorder}) ship a no-op default and an
 * {@code isEnabled()} guard, so the off-path costs nothing until a real adapter is wired.
 */
package com.codingful.tandem.core.port;
