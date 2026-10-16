package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxMessage;
import java.util.Collection;

/**
 * Write-side port (LLD-core §2.1): insert outbox rows <b>within the caller's transaction</b>, atomically
 * with the business state change. Implemented by {@code tandem-jdbc} and by {@code InMemoryOutbox}.
 *
 * <p><b>Provided:</b> Tandem implements this interface and the application calls it. A minor version
 * may add abstract methods, so an application implementing it follows Tandem's minor versions
 * (HLD-api-stability §4.1).
 */
public interface OutboxRepository {

    /**
     * Insert one message within the caller's transaction.
     *
     * @param message the row to insert
     * @throws IllegalArgumentException if the relay could never publish it: its
     *         {@link com.codingful.tandem.core.TandemHeaders#DATA_SCHEMA} header is not a URI. Nothing is written
     * @throws com.codingful.tandem.core.exception.OutboxInsertException if the insert fails
     */
    void insert(OutboxMessage message);

    /**
     * Insert several messages within the same transaction.
     *
     * @param messages the rows to insert, in order
     * @throws IllegalArgumentException if the relay could never publish one of them, as for
     *         {@link #insert}. Nothing is written
     * @throws com.codingful.tandem.core.exception.OutboxInsertException if the insert fails
     */
    void insertAll(Collection<OutboxMessage> messages);
}
