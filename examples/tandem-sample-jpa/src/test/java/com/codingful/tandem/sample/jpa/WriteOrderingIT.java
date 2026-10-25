package com.codingful.tandem.sample.jpa;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.port.OutboxRepository;
import jakarta.persistence.EntityManager;
import jakarta.persistence.EntityManagerFactory;
import java.time.Duration;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.orm.jpa.JpaTransactionManager;
import org.springframework.orm.jpa.SharedEntityManagerCreator;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

@Tag("integration")
@Timeout(60)
@SpringBootTest(properties = "tandem.relay.enabled=false")
@ActiveProfiles("test")
class WriteOrderingIT {
    @Autowired
    private ApplicationContext context;
    @Autowired
    private OrderService orders;
    @Autowired
    private JdbcTemplate jdbc;
    @Autowired
    private PlatformTransactionManager manager;
    private TransactionTemplate transactions;

    @BeforeEach
    void configureTransactions() {
        assertThat(manager).isInstanceOf(JpaTransactionManager.class);
        transactions = new TransactionTemplate(manager);
    }

    @Test
    void GIVEN_one_aggregate_WHEN_writers_overlap_THEN_the_second_insert_waits_for_the_first_commit() throws Exception {
        String id = create();
        concurrent(List.of(id), () -> orders.append(List.of(id)), () -> orders.append(List.of(id)));
        assertThat(sequences(id)).containsExactly(1L, 2L);
    }

    @Test
    void GIVEN_two_aggregates_WHEN_commands_reverse_their_order_THEN_both_streams_keep_their_sequence() throws Exception {
        String a = create();
        String b = create();
        concurrent(List.of(a, b), () -> orders.append(List.of(a, b)), () -> orders.append(List.of(b, a)));
        assertThat(sequences(a)).containsExactly(1L, 2L);
        assertThat(sequences(b)).containsExactly(1L, 2L);
    }

    /** Holds the first writer open and checks inside PostgreSQL that the second waits for it. */
    private void concurrent(List<String> writtenByFirst, Runnable first, Runnable second) throws Exception {
        var inserted = new CountDownLatch(1);
        var commit = new CountDownLatch(1);
        var secondPid = new CompletableFuture<Integer>();
        var workers = Executors.newFixedThreadPool(2);
        try {
            var one = workers.submit(() -> transactions.executeWithoutResult(status -> {
                first.run();
                inserted.countDown();
                await(commit);
            }));
            assertThat(inserted.await(15, TimeUnit.SECONDS)).isTrue();
            var two = workers.submit(() -> transactions.executeWithoutResult(status -> {
                secondPid.complete(PostgresLocks.backendPid(jdbc));
                second.run();
            }));
            int pid = secondPid.get(15, TimeUnit.SECONDS);
            assertThat(PostgresLocks.awaitBlocked(jdbc, pid, Duration.ofSeconds(15))).isTrue();
            assertThat(two.isDone()).isFalse();
            for (String id : writtenByFirst) assertThat(sequences(id)).isEmpty();
            commit.countDown();
            one.get(15, TimeUnit.SECONDS);
            two.get(15, TimeUnit.SECONDS);
        } finally {
            commit.countDown();
            workers.shutdownNow();
            assertThat(workers.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void GIVEN_independent_aggregates_WHEN_one_transaction_stays_open_THEN_the_other_can_commit() throws Exception {
        String a = create();
        String b = create();
        var inserted = new CountDownLatch(1);
        var commit = new CountDownLatch(1);
        var workers = Executors.newFixedThreadPool(2);
        try {
            var first = workers.submit(() -> transactions.executeWithoutResult(status -> {
                orders.append(List.of(a));
                inserted.countDown();
                await(commit);
            }));
            assertThat(inserted.await(15, TimeUnit.SECONDS)).isTrue();
            workers.submit(() -> orders.append(List.of(b))).get(15, TimeUnit.SECONDS);
            assertThat(sequences(a)).isEmpty();
            assertThat(sequences(b)).containsExactly(1L);
            commit.countDown();
            first.get(15, TimeUnit.SECONDS);
        } finally {
            commit.countDown();
            workers.shutdownNow();
            assertThat(workers.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void GIVEN_multiple_notes_WHEN_written_in_one_transaction_THEN_each_gets_its_own_revision() {
        String id = create();
        orders.append(List.of(id, id));
        orders.append(List.of(id));
        assertThat(sequences(id)).containsExactly(1L, 2L, 3L);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sample_order_note WHERE order_id = ?",
                Long.class, id)).isEqualTo(3);
    }

    @Test
    void GIVEN_inserted_events_WHEN_the_business_transaction_fails_THEN_notes_counter_and_outbox_roll_back() {
        String id = create();
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            orders.append(List.of(id, id));
            throw new IllegalStateException("Abort demo transaction");
        })).isInstanceOf(IllegalStateException.class);
        assertThat(sequences(id)).isEmpty();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM sample_order_note WHERE order_id = ?",
                Long.class, id)).isZero();
        assertThat(jdbc.queryForObject("SELECT event_sequence FROM sample_order WHERE id = ?",
                Long.class, id)).isZero();
        orders.append(List.of(id));
        assertThat(sequences(id)).containsExactly(1L);
    }

    @Test
    void GIVEN_a_state_change_WHEN_confirmation_is_repeated_THEN_only_the_flushed_version_is_emitted() {
        String id = create();
        orders.confirmWithVersion(id);
        orders.confirmWithVersion(id);
        assertThat(sequences(id)).containsExactly(jdbc.queryForObject(
                "SELECT version FROM sample_order WHERE id = ?", Long.class, id));
        assertThat(sequences(id)).containsExactly(1L);
    }

    @Test
    void GIVEN_child_only_writers_with_tandems_aggregate_lock_WHEN_they_overlap_THEN_the_second_waits_and_numbers_follow_commit_order() throws Exception {
        String id = create();
        concurrent(List.of(id), () -> orders.appendChildOnly(id), () -> orders.appendChildOnly(id));
        assertThat(sequences(id)).hasSize(2).isSorted().doesNotHaveDuplicates();
    }

    @Test
    void GIVEN_child_only_writes_without_a_root_lock_WHEN_the_second_commits_first_THEN_the_earlier_event_is_invisible() throws Exception {
        String id = create();
        var inserted = new CountDownLatch(1);
        var commit = new CountDownLatch(1);
        var workers = Executors.newFixedThreadPool(2);
        try {
            var first = workers.submit(() -> transactions.executeWithoutResult(status -> {
                unsafeChildWrite(id, 1);
                inserted.countDown();
                await(commit);
            }));
            assertThat(inserted.await(15, TimeUnit.SECONDS)).isTrue();
            workers.submit(() -> transactions.executeWithoutResult(status -> unsafeChildWrite(id, 2)))
                    .get(15, TimeUnit.SECONDS);
            // This is what a relay polling now can see: it cannot wait for an invisible seq=1.
            assertThat(sequences(id)).containsExactly(2L);
            commit.countDown();
            first.get(15, TimeUnit.SECONDS);
            assertThat(sequences(id)).containsExactly(1L, 2L);
        } finally {
            commit.countDown();
            workers.shutdownNow();
            assertThat(workers.awaitTermination(15, TimeUnit.SECONDS)).isTrue();
        }
    }

    @Test
    void GIVEN_two_events_using_an_unflushed_version_WHEN_they_are_inserted_THEN_the_transaction_aborts() {
        String id = create();
        assertThatThrownBy(() -> transactions.executeWithoutResult(status -> {
            var entities = entities();
            var order = entities.find(PurchaseOrder.class, id);
            order.confirm();
            long stale = order.version();
            var outbox = context.getBean(OutboxRepository.class);
            outbox.insert(message(id, stale));
            outbox.insert(message(id, stale));
        })).isInstanceOf(DuplicateSeqException.class);
        assertThat(sequences(id)).isEmpty();
        assertThat(jdbc.queryForObject("SELECT status FROM sample_order WHERE id = ?", String.class, id))
                .isEqualTo("new");
    }

    // Deliberately unsafe counterexample, never used by the application service.
    private void unsafeChildWrite(String id, long seq) {
        var entities = entities();
        entities.persist(new OrderNote(id));
        entities.flush();
        context.getBean(OutboxRepository.class).insert(message(id, seq));
    }

    private EntityManager entities() {
        return SharedEntityManagerCreator.createSharedEntityManager(
                context.getBean(EntityManagerFactory.class));
    }

    private static OutboxMessage message(String id, long seq) {
        return OutboxMessage.builder().aggregateType("Order").aggregateId(id)
                .seq(seq).payload(new byte[] {'{', '}'}).build();
    }

    private String create() {
        String id = "order:" + UUID.randomUUID();
        orders.create(id);
        return id;
    }

    private List<Long> sequences(String id) {
        // Intentionally order by insertion id, never by seq: sorting seq would hide a reorder.
        return jdbc.queryForList("SELECT seq FROM tandem_outbox WHERE aggregate_id = ? ORDER BY id",
                Long.class, id);
    }

    private static void await(CountDownLatch latch) {
        try {
            if (!latch.await(30, TimeUnit.SECONDS)) throw new IllegalStateException("Timed out awaiting commit");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while awaiting commit", ex);
        }
    }
}
