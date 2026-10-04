package com.codingful.tandem.spring.producer;

import com.codingful.tandem.core.exception.OutboxInsertException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.Objects;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.TransactionAwareDataSourceProxy;
import org.springframework.transaction.support.TransactionSynchronizationManager;

/**
 * Checks that the active transaction is one Tandem's insert really takes part in. A transaction bound to
 * another {@link DataSource} still makes {@code isActualTransactionActive()} true, yet Tandem's insert would
 * then run on its own autocommitted connection: the domain change rolls back, the outbox row stays and the
 * relay publishes an event for a change that never happened (LLD-spring-producer §8).
 *
 * <p>Two checks, both run inside an active transaction:
 * <ol>
 *   <li><b>Binding</b>: the transaction manager bound a connection for the raw {@code DataSource}. Skipped
 *       under JTA, which binds none and is still atomic.</li>
 *   <li><b>Autocommit</b>: the connection the insert would use is not in autocommit, which is a certain
 *       loss of atomicity whatever the transaction manager. It is the only check that also catches a
 *       binding the first one cannot see through.</li>
 * </ol>
 * {@code tandem.outbox.verify-transaction-binding=false} turns the binding check off for a setup it misjudges;
 * the autocommit check has no false positive and stays on.
 */
final class TransactionBindingGuard {

    /** Checks nothing; for tests that run without a database. */
    private static final TransactionBindingGuard DISABLED = new TransactionBindingGuard(null, null);

    private final DataSource boundDataSource; // null = skip the binding check
    private final DataSource connectionSource; // null = skip everything

    private TransactionBindingGuard(DataSource boundDataSource, DataSource connectionSource) {
        this.boundDataSource = boundDataSource;
        this.connectionSource = connectionSource;
    }

    /**
     * @param dataSource    the raw {@code DataSource} Tandem writes to
     * @param checkBinding  whether to require a transaction-bound connection for it (false under JTA)
     */
    static TransactionBindingGuard forDataSource(DataSource dataSource, boolean checkBinding) {
        Objects.requireNonNull(dataSource, "dataSource");
        // Transaction managers unwrap a transaction-aware proxy and bind its target, so the key is the target.
        DataSource bound = dataSource instanceof TransactionAwareDataSourceProxy proxy
                ? proxy.getTargetDataSource() : dataSource;
        return new TransactionBindingGuard(checkBinding ? bound : null,
                new TransactionAwareDataSourceProxy(dataSource));
    }

    static TransactionBindingGuard disabled() {
        return DISABLED;
    }

    boolean checksBinding() {
        return boundDataSource != null;
    }

    boolean isDisabled() {
        return connectionSource == null;
    }

    /**
     * @throws OutboxInsertException if the insert would not be atomic with the active transaction
     */
    void requireAtomic() {
        if (connectionSource == null) {
            return;
        }
        if (boundDataSource != null && !TransactionSynchronizationManager.hasResource(boundDataSource)) {
            throw new OutboxInsertException("The active transaction is not bound to the DataSource Tandem writes"
                    + " to, so the outbox insert would not be atomic with it. Point Tandem at the DataSource"
                    + " your transaction uses, or set tandem.outbox.verify-transaction-binding=false if the"
                    + " transaction is atomic by other means");
        }
        try (Connection connection = connectionSource.getConnection()) {
            if (connection.getAutoCommit()) {
                throw new OutboxInsertException("The DataSource Tandem writes to is in autocommit inside an active"
                        + " transaction, so the outbox insert would commit on its own. Point Tandem at the"
                        + " DataSource your transaction uses");
            }
        } catch (SQLException e) {
            throw new OutboxInsertException("Could not check the connection Tandem writes through", e);
        }
    }
}
