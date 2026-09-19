package com.codingful.tandem.rabbitmq;

import java.net.URI;
import java.time.Duration;
import java.util.Objects;

/**
 * Tandem-specific settings for the AMQP adapter (LLD-rabbitmq §1, §3, §5), separate from the AMQP
 * connection settings, which are the caller's {@link com.rabbitmq.client.ConnectionFactory}.
 *
 * @param source             the single configured CloudEvents {@code source} URI
 * @param defaultContentType {@code datacontenttype} when a row stored none, e.g. {@code application/json}
 * @param defaultDataSchema  optional default {@code dataschema} URI; {@code null} to omit the attribute
 * @param exchange           the exchange every record is published to; verified to exist at startup
 *                           and never declared by Tandem (§1)
 * @param routingKeySuffix   appended to the kebab-cased aggregate type by the default router (§6)
 * @param confirmTimeout     how long a publisher confirm may take before the row is failed retriably.
 *                           Not a tuning knob: with no deadline a lost confirm holds an in-flight slot
 *                           for the life of the process (§3). Must be positive, and must stay below
 *                           {@code rowLease}, which the relay checks for itself
 */
public record RabbitRelayConfig(URI source, String defaultContentType, URI defaultDataSchema,
                                String exchange, String routingKeySuffix, Duration confirmTimeout) {

    /** The default confirm timeout, matching {@code tandem-kafka}'s default delivery timeout. */
    public static final Duration DEFAULT_CONFIRM_TIMEOUT = Duration.ofSeconds(30);

    /**
     * @throws NullPointerException     if any argument but {@code defaultDataSchema} is {@code null}
     * @throws IllegalArgumentException if {@code confirmTimeout} is not positive
     */
    public RabbitRelayConfig {
        Objects.requireNonNull(source, "source");
        Objects.requireNonNull(defaultContentType, "defaultContentType");
        Objects.requireNonNull(exchange, "exchange");
        Objects.requireNonNull(routingKeySuffix, "routingKeySuffix");
        Objects.requireNonNull(confirmTimeout, "confirmTimeout");
        if (confirmTimeout.isZero() || confirmTimeout.isNegative()) {
            throw new IllegalArgumentException("confirmTimeout must be positive, got " + confirmTimeout);
        }
    }

    /**
     * With {@code application/json}, no default data schema, the {@code -topic} routing-key suffix and
     * the default confirm timeout.
     */
    public static RabbitRelayConfig of(String source, String exchange) {
        return new RabbitRelayConfig(URI.create(source), "application/json", null, exchange, "-topic",
                DEFAULT_CONFIRM_TIMEOUT);
    }
}
