/**
 * The implementation of the Admin API, wired by
 * {@link com.codingful.tandem.admin.TandemAdminAutoConfiguration}. <b>Not API:</b> the types in this
 * package and its sub-packages may change in any release. They are public only because the
 * autoconfiguration and the feature sub-packages reach them across a package boundary; an application
 * uses the Admin API over HTTP, never through these types (HLD-api-stability §3).
 *
 * <p>This package holds what every feature shares:
 * {@link com.codingful.tandem.admin.internal.TandemAdminExceptionHandler}, the generic 400/500
 * mapping each feature composes alongside its own, and
 * {@link com.codingful.tandem.admin.internal.ProblemDetails}, the RFC 9457 builder. Each REST feature
 * has its own sub-package ({@code outbox}, {@code relay}).
 */
package com.codingful.tandem.admin.internal;
