package com.codingful.tandem.spring.producer;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.exception.OutboxInsertException;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.transaction.support.TransactionSynchronizationManager;

class TransactionBindingGuardTest {

    private final DataSource tandemDataSource = new NoopDataSource();

    @Test
    void GIVEN_a_transaction_bound_only_to_another_datasource_WHEN_checked_THEN_the_insert_is_refused() {
        DataSource ordersDataSource = new NoopDataSource();
        TransactionSynchronizationManager.bindResource(ordersDataSource, new Object());
        try {
            assertThatThrownBy(TransactionBindingGuard.forDataSource(tandemDataSource, true)::requireAtomic)
                    .isInstanceOf(OutboxInsertException.class)
                    .hasMessageContaining("verify-transaction-binding");
        } finally {
            TransactionSynchronizationManager.unbindResource(ordersDataSource);
        }
    }
}
