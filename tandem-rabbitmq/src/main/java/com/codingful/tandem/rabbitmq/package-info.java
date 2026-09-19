/**
 * The RabbitMQ publish adapter (LLD-rabbitmq): {@link com.codingful.tandem.rabbitmq.RabbitRelay}
 * implements the {@link com.codingful.tandem.core.port.OutboxDispatcher} port by handing each
 * {@link com.codingful.tandem.core.OutboxRecord} to a
 * {@link com.codingful.tandem.rabbitmq.RabbitMessageEncoder}
 * ({@link com.codingful.tandem.rabbitmq.CloudEventAmqpEncoder}, a CloudEvent in binary mode, unless
 * the caller supplies another) and publishing it on one channel with publisher confirms, completing
 * the returned future on the broker's confirm.
 *
 * <p>The channel is hardened to safe values (confirms on, persistent delivery, {@code mandatory}),
 * the route comes from a {@link com.codingful.tandem.rabbitmq.RabbitRouter}, and AMQP failures are
 * mapped to the retriable/permanent verdict by a
 * {@link com.codingful.tandem.rabbitmq.RabbitErrorClassifier}. This module is relay-side only, never
 * on the client write-side (§1.3), and carries its own version (LLD-rabbitmq §9).
 */
package com.codingful.tandem.rabbitmq;
