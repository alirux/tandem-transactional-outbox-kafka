package com.codingful.tandem.admin.internal;

/**
 * A request value the Admin API cannot accept: an unknown status name, a cursor that is not a number,
 * a criterion outside its bounds. It is what {@link TandemAdminExceptionHandler} answers with
 * {@code 400 invalid-parameter}, so that an {@link IllegalArgumentException} raised while reading the
 * stored data (a status code a newer relay wrote, say) is an internal error and not the caller's.
 */
public class InvalidRequestException extends RuntimeException {

    /**
     * @param message the detail returned to the caller; it names the rejected value, never stored data
     * @param cause   the parsing or validation failure the request value caused
     */
    public InvalidRequestException(String message, Throwable cause) {
        super(message, cause);
    }
}
