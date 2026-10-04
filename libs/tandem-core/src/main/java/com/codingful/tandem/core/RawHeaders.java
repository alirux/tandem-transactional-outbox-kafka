package com.codingful.tandem.core;

/**
 * Wire names of the raw passthrough envelope written by {@link RawMessageEncoder}
 * (HLD-alternative-envelope §6). Declared once in the dependency-free core because consumers read
 * these exact names off the wire; frozen once published (HLD §1.4).
 *
 * <p><b>The {@value #PREFIX} prefix is reserved.</b> Raw's headers share one namespace with the
 * application's row headers, so every name raw emits is a name the application loses: a row header
 * named like one of raw's own does not reach the wire. Applications must therefore not name row
 * headers {@code tandem-*}. Every header raw adds later lives under the same prefix, so the envelope
 * grows without shadowing one more application header.
 */
public final class RawHeaders {

    private RawHeaders() {
    }

    /** The namespace reserved for raw's own headers. */
    public static final String PREFIX = "tandem-";

    /** The outbox row id, as decimal text: the event id a consumer deduplicates on. */
    public static final String ID = PREFIX + "id";

    /** The event type, falling back to the aggregate type when the row stored none. */
    public static final String TYPE = PREFIX + "type";
}
