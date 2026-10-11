/**
 * Tandem's Admin API (HLD-admin-api.md, admin-api.openapi.yaml): an optional, API-first REST
 * operations layer over the outbox and the relay, off by default (gated on
 * {@code tandem.admin.enabled}).
 *
 * <p>The module's contract is the HTTP API of the OpenAPI document, its {@code tandem.admin.*}
 * properties, and the class name of {@link com.codingful.tandem.admin.TandemAdminAutoConfiguration},
 * the one type in this package (HLD-api-stability §2, §3). The autoconfiguration also contributes the
 * DB-derived adapter beans more than one feature needs. Everything it wires lives in
 * {@link com.codingful.tandem.admin.internal}, which carries no compatibility promise: the generic
 * error handling, the shared RFC 9457 builder, and one sub-package per REST feature
 * ({@code internal.outbox}, {@code internal.relay}) holding its use cases, controller, DTOs and
 * feature-specific problem-slug mapping, imported by the autoconfiguration so it inherits that class's
 * gating.
 *
 * <p>Depends on {@code tandem-jdbc} for outbox access and, for the REST layer, on Spring — kept
 * {@code compileOnly} so no Spring version is redistributed to a consumer, the same footprint
 * discipline as {@code tandem-spring-relay}. Never a dependency of the client write-side.
 */
package com.codingful.tandem.admin;
