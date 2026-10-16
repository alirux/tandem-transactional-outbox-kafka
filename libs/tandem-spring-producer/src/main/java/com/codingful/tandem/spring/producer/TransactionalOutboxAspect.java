package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.OutboxInsertException;
import com.codingful.tandem.core.port.OutboxRepository;
import com.codingful.tandem.core.port.TandemAggregate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Applies the {@link TransactionalOutbox} annotation tier (LLD-spring-producer §4): after the annotated
 * method runs, it extracts the pending messages from the returned {@code TandemAggregate}(s) and inserts
 * them within the same transaction.
 *
 * <p>The advice must run <em>inside</em> the transaction the composed {@code @Transactional} opened. If a
 * misordering ever placed it outside, {@link #insertPendingMessages} would see no active transaction and
 * fail fast rather than insert non-atomically — the loud backstop the design relies on; the atomicity
 * itself is pinned by a rollback integration test.
 */
@Aspect
class TransactionalOutboxAspect {

    private final OutboxRepository outboxRepository;
    private final TransactionBindingGuard bindingGuard;

    TransactionalOutboxAspect(OutboxRepository outboxRepository, TransactionBindingGuard bindingGuard) {
        this.outboxRepository = Objects.requireNonNull(outboxRepository, "outboxRepository");
        this.bindingGuard = Objects.requireNonNull(bindingGuard, "bindingGuard");
    }

    @Around("@annotation(transactionalOutbox)")
    Object insertPendingMessages(ProceedingJoinPoint joinPoint, TransactionalOutbox transactionalOutbox)
            throws Throwable {
        Object result = joinPoint.proceed();
        insertPending(extract(result), transactionalOutbox.aggregateType());
        return result;
    }

    /**
     * Guard the extracted messages, then insert them within the active transaction — failing fast when
     * there is none, so a misordering can never insert non-atomically. Separated from the advice so it is
     * testable without AOP.
     */
    void insertPending(List<OutboxMessage> messages, String declaredAggregateType) {
        guardAggregateType(messages, declaredAggregateType);
        if (messages.isEmpty()) {
            return;
        }
        if (!TransactionSynchronizationManager.isActualTransactionActive()) {
            throw new OutboxInsertException("@TransactionalOutbox produced outbox messages outside an active"
                    + " transaction — the insert would not be atomic");
        }
        List<OutboxMessage> fresh = notYetWritten(messages);
        if (fresh.isEmpty()) {
            return;
        }
        bindingGuard.requireAtomic();
        outboxRepository.insertAll(fresh);
        written().addAll(fresh);
    }

    /**
     * The messages this transaction has not written yet. An aggregate keeps its events pending until it is
     * discarded, so one returned by two annotated methods in the same transaction (one calling the other)
     * still holds the first method's events when the second returns: written again, each would become a
     * second event with an id of its own, which no consumer could recognise as a duplicate. Compared by
     * identity, never by content: two changes that happen to produce equal messages are two events.
     */
    private List<OutboxMessage> notYetWritten(List<OutboxMessage> messages) {
        Set<OutboxMessage> written = written();
        List<OutboxMessage> fresh = new ArrayList<>(messages.size());
        for (OutboxMessage message : messages) {
            if (!written.contains(message)) {
                fresh.add(message);
            }
        }
        return fresh;
    }

    /**
     * The messages already written in the current transaction, bound to it and released when it ends. Empty,
     * and kept nowhere, when the transaction runs no synchronization: nothing could release it then.
     */
    @SuppressWarnings("unchecked")
    private Set<OutboxMessage> written() {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) {
            return Collections.newSetFromMap(new IdentityHashMap<>());
        }
        Set<OutboxMessage> written = (Set<OutboxMessage>) TransactionSynchronizationManager.getResource(this);
        if (written == null) {
            written = Collections.newSetFromMap(new IdentityHashMap<>());
            TransactionSynchronizationManager.bindResource(this, written);
            TransactionSynchronizationManager.registerSynchronization(new TransactionSynchronization() {
                @Override
                public void afterCompletion(int status) {
                    TransactionSynchronizationManager.unbindResourceIfPossible(TransactionalOutboxAspect.this);
                }
            });
        }
        return written;
    }

    /**
     * Collect the pending messages a method's return value exposes: a single {@code TandemAggregate}, or
     * each {@code TandemAggregate} in an {@link Iterable} (non-aggregate elements are ignored). Any other
     * return value — including {@code void}/{@code null} — yields nothing, which is allowed.
     */
    static List<OutboxMessage> extract(Object returnValue) {
        if (returnValue instanceof TandemAggregate aggregate) {
            return List.copyOf(aggregate.pendingOutboxMessages());
        }
        if (returnValue instanceof Iterable<?> iterable) {
            List<OutboxMessage> all = new ArrayList<>();
            for (Object element : iterable) {
                if (element instanceof TandemAggregate aggregate) {
                    all.addAll(aggregate.pendingOutboxMessages());
                }
            }
            return all;
        }
        return List.of();
    }

    /** When a {@code aggregateType} is declared on the annotation, assert every message carries it. */
    static void guardAggregateType(List<OutboxMessage> messages, String declaredAggregateType) {
        if (declaredAggregateType == null || declaredAggregateType.isEmpty()) {
            return;
        }
        for (OutboxMessage message : messages) {
            if (!declaredAggregateType.equals(message.aggregateType())) {
                throw new OutboxInsertException("Extracted outbox message does not match the @TransactionalOutbox"
                        + " declaration declaredAggregateType:" + declaredAggregateType
                        + ", messageAggregateType:" + message.aggregateType());
            }
        }
    }
}
