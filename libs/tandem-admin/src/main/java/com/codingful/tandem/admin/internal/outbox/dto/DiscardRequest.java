package com.codingful.tandem.admin.internal.outbox.dto;

/** Wire representation of the OpenAPI {@code DiscardRequest} schema ({@code POST /outbox/messages/{id}/discard}). */
public record DiscardRequest(boolean acknowledgeOrderingBreak, String reason) {
}
