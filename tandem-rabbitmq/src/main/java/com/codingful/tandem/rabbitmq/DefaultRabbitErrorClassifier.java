package com.codingful.tandem.rabbitmq;

import com.codingful.tandem.core.exception.OutboxDispatchException;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.ShutdownSignalException;

/**
 * Default AMQP error mapping (LLD-rabbitmq §4).
 *
 * <ul>
 *   <li><b>Permanent</b> — the broker refused this message for a reason no retry changes: a missing
 *       exchange ({@code 404}), a refused permission ({@code 403}), or a violated precondition
 *       ({@code 406}). Each is a configuration fact, so retrying would only block the aggregate for
 *       the whole backoff ladder.</li>
 *   <li><b>Retriable</b> — everything else: a nack, a channel or connection shutdown, an I/O failure,
 *       <b>and unknown errors by default</b>. Failing a row permanently is the unrecoverable
 *       direction, so the default leans the other way, exactly as the Kafka classifier does.</li>
 * </ul>
 */
public final class DefaultRabbitErrorClassifier implements RabbitErrorClassifier {

    private static final int ACCESS_REFUSED = 403;
    private static final int NOT_FOUND = 404;
    private static final int PRECONDITION_FAILED = 406;

    @Override
    public OutboxDispatchException classify(Throwable cause) {
        boolean retriable = !isPermanent(cause);
        String message = "AMQP publish failed (" + (retriable ? "retriable" : "permanent") + "): "
                + cause.getClass().getSimpleName()
                + (cause.getMessage() == null ? "" : " - " + cause.getMessage());
        return new OutboxDispatchException(message, retriable, cause);
    }

    /** Walks the cause chain: the client wraps a channel close inside an {@code IOException}. */
    private static boolean isPermanent(Throwable cause) {
        for (Throwable current = cause; current != null; current = current.getCause()) {
            if (current instanceof ShutdownSignalException shutdown && isPermanentReason(shutdown)) {
                return true;
            }
            if (current.getCause() == current) {
                break;
            }
        }
        return false;
    }

    private static boolean isPermanentReason(ShutdownSignalException shutdown) {
        Object reason = shutdown.getReason();
        int replyCode;
        if (reason instanceof AMQP.Channel.Close channelClose) {
            replyCode = channelClose.getReplyCode();
        } else if (reason instanceof AMQP.Connection.Close connectionClose) {
            replyCode = connectionClose.getReplyCode();
        } else {
            // Any other shutdown reason (an application-initiated close, a connection-level protocol
            // error) says nothing about this message: retriable, like every unrecognised failure.
            return false;
        }
        return replyCode == ACCESS_REFUSED || replyCode == NOT_FOUND || replyCode == PRECONDITION_FAILED;
    }
}
