package com.codingful.tandem.spring.producer;

import org.springframework.transaction.TransactionDefinition;
import org.springframework.transaction.support.AbstractPlatformTransactionManager;
import org.springframework.transaction.support.DefaultTransactionStatus;

/**
 * A real, resource-less transaction manager for the no-database runtime tests. Built on Spring's own
 * {@link AbstractPlatformTransactionManager}, it opens, joins and closes transaction scopes, marks them
 * active and runs their synchronizations exactly as a JDBC manager would, so the write-side tiers'
 * "must run inside a transaction" guards see the truth and a method joining its caller's transaction
 * behaves as it does in production.
 *
 * <p>It manages no resource, so it deliberately cannot demonstrate that a rollback discards outbox rows —
 * that is what the Testcontainers integration tests are for.
 */
final class StubTransactionManager extends AbstractPlatformTransactionManager {

    private static final ThreadLocal<Boolean> OPEN = ThreadLocal.withInitial(() -> false);

    private int begun;

    /** How many transactions this manager opened: which manager ran a method is what a qualifier decides. */
    int begun() {
        return begun;
    }

    @Override
    protected Object doGetTransaction() {
        return new Object();
    }

    @Override
    protected boolean isExistingTransaction(Object transaction) {
        return OPEN.get();
    }

    @Override
    protected void doBegin(Object transaction, TransactionDefinition definition) {
        begun++;
        OPEN.set(true);
    }

    @Override
    protected void doCommit(DefaultTransactionStatus status) {
    }

    @Override
    protected void doRollback(DefaultTransactionStatus status) {
    }

    @Override
    protected void doCleanupAfterCompletion(Object transaction) {
        OPEN.set(false);
    }
}
