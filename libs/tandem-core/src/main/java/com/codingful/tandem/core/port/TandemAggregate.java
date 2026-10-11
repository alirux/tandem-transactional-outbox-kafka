package com.codingful.tandem.core.port;

import com.codingful.tandem.core.OutboxMessage;
import java.util.Collection;

/**
 * Implemented by the client's aggregate to expose the outbox messages it produced (the annotation
 * tier, HLD §3.1, LLD-core §2.6).
 *
 * <p><b>SPI:</b> the application implements this interface and Tandem calls it. A method added in a
 * minor version is always {@code default}, so an existing implementation keeps working
 * (HLD-api-stability §4.1).
 */
public interface TandemAggregate {

    Collection<OutboxMessage> pendingOutboxMessages();
}
