package com.codingful.tandem.core.port;

/**
 * Object → bytes (LLD-core §2.4). There is <b>no default in core</b>: a JSON serializer needs a JSON
 * library, which the dependency-free core must not pull in (§1.3). A Jackson-based default ships in
 * {@code tandem-spring-producer}; non-Spring users supply one or pass bytes directly.
 *
 * <p><b>SPI:</b> the application implements this interface and Tandem calls it. A method added in a
 * minor version is always {@code default}, so an existing implementation keeps working
 * (HLD-api-stability §4.1).
 */
public interface PayloadSerializer {

    /** @throws com.codingful.tandem.core.exception.PayloadSerializationException if serialization fails */
    byte[] serialize(Object payload);

    /** e.g. {@code "application/json"}; stored into {@code headers["content-type"]}. */
    String contentType();
}
