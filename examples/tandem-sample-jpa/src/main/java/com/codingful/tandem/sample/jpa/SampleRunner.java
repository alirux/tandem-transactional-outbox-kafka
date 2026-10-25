package com.codingful.tandem.sample.jpa;

import com.codingful.tandem.core.CloudEventsHeaders;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.test.TandemTestContainer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Profile;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.core.env.Environment;
import org.springframework.stereotype.Component;
import org.springframework.transaction.PlatformTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * [DEMO-ONLY] Narrates JPA writes, concurrent root locking and Kafka delivery.
 * Spring starts the relay automatically; this runner only acts as a caller and observer.
 */
@Component
@Profile("!test")
class SampleRunner implements CommandLineRunner {
    static final String SINGLE = "order:single";
    static final String LEFT = "order:left";
    static final String RIGHT = "order:right";
    static final String VERSIONED = "order:versioned";
    static final String ROLLBACK = "order:rollback";

    private final OrderService orders;
    private final JdbcTemplate jdbc;
    private final TandemTestContainer infrastructure;
    private final TransactionTemplate transactions;
    private final Environment environment;

    SampleRunner(OrderService orders, JdbcTemplate jdbc, TandemTestContainer infrastructure,
            PlatformTransactionManager manager, Environment environment) {
        this.environment = environment;
        this.orders = orders;
        this.jdbc = jdbc;
        this.infrastructure = infrastructure;
        this.transactions = new TransactionTemplate(manager);
    }

    @Override
    public void run(String... args) throws Exception {
        banner();
        System.out.println("► JPA writes: lock the root, allocate each event revision, insert, commit.");
        for (String id : List.of(SINGLE, LEFT, RIGHT, VERSIONED, ROLLBACK)) orders.create(id);
        orders.append(List.of(SINGLE, SINGLE));
        System.out.println("  Two child notes in one transaction: separate event revisions 1 and 2.");
        System.out.println("  @Version is persistence metadata; it is not a counter for every event.");

        System.out.println("\n► Concurrent transactions on one aggregate");
        concurrent(List.of(SINGLE), List.of(SINGLE));
        System.out.println("  The next two revisions are 3 and 4, in the order of the serialized writes.");

        System.out.println("\n► Concurrent transactions mentioning two aggregates in opposite order");
        concurrent(List.of(LEFT, RIGHT), List.of(RIGHT, LEFT));
        System.out.println("  Both commands lock the complete root set in sorted ID order before writing.");
        System.out.println("  Each stream has revisions 1 and 2; independent streams have no global order.");

        System.out.println("\n► A separate stream using the flushed JPA version");
        orders.confirmWithVersion(VERSIONED);
        orders.confirmWithVersion(VERSIONED);
        System.out.println("  Confirmation emits version 1. Repeating it changes no state and emits nothing.");
        System.out.println("  Reading the version before flush can reuse a stale sequence.");

        System.out.println("\n► Rollback after the domain and outbox have both been written");
        transactions.executeWithoutResult(status -> {
            orders.append(List.of(ROLLBACK, ROLLBACK));
            status.setRollbackOnly();
        });
        require(jdbc.queryForObject("SELECT count(*) FROM tandem_outbox WHERE aggregate_id = ?",
                Long.class, ROLLBACK) == 0, "Rolled-back outbox rows remained visible");
        orders.append(List.of(ROLLBACK));
        System.out.println("  The rolled-back revisions disappear; the next successful command uses 1.");

        printOutboxRows();
        consumeAndVerify();
        System.out.println("\nDone. The integration tests also demonstrate stale-version collisions and");
        System.out.println("child-only writes that reorder when the shared root lock is missing.");
        printAdminApiHints(manufactureAFailedRow());
    }

    /** Hold the first transaction open and observe the second waiting inside PostgreSQL. */
    private void concurrent(List<String> first, List<String> second) throws Exception {
        var inserted = new CountDownLatch(1);
        var commit = new CountDownLatch(1);
        var secondPid = new CompletableFuture<Integer>();
        var workers = Executors.newFixedThreadPool(2);
        try {
            var one = workers.submit(() -> transactions.executeWithoutResult(status -> {
                orders.append(first);
                inserted.countDown();
                await(commit);
            }));
            require(inserted.await(15, TimeUnit.SECONDS), "First writer did not reach its outbox insert");
            var two = workers.submit(() -> transactions.executeWithoutResult(status -> {
                secondPid.complete(PostgresLocks.backendPid(jdbc));
                orders.append(second);
            }));
            int pid = secondPid.get(15, TimeUnit.SECONDS);
            require(PostgresLocks.awaitBlocked(jdbc, pid, Duration.ofSeconds(15)),
                    "Second writer did not wait for the first transaction's lock");
            System.out.println("  PostgreSQL confirms: T2 is blocked on T1's lock before its outbox insert.");
            System.out.println("  Committing T1 now releases T2; only commit or rollback releases a row lock.");
            commit.countDown();
            one.get(15, TimeUnit.SECONDS);
            two.get(15, TimeUnit.SECONDS);
        } finally {
            commit.countDown();
            workers.shutdownNow();
            require(workers.awaitTermination(15, TimeUnit.SECONDS), "Demo writers did not stop");
        }
    }

    private void printOutboxRows() {
        System.out.println("\n► Outbox rows in insertion order (the relay is already running)");
        System.out.printf("    %-5s %-20s %-5s %-18s %s%n", "ID", "AGGREGATE", "SEQ", "EVENT", "STATUS");
        jdbc.query("SELECT id, aggregate_id, seq, type, status FROM tandem_outbox ORDER BY id", rs -> {
            System.out.printf("    %-5d %-20s %-5d %-18s %s%n",
                    rs.getLong(1), rs.getString(2), rs.getLong(3), rs.getString(4), OutboxStatus.fromCode(rs.getInt(5)));
        });
    }

    private void consumeAndVerify() {
        System.out.println("\n► Consuming what the autoconfigured relay published to Kafka");
        Map<String, List<Long>> expected = Map.of(
                SINGLE, List.of(1L, 2L, 3L, 4L), LEFT, List.of(1L, 2L), RIGHT, List.of(1L, 2L),
                VERSIONED, List.of(1L), ROLLBACK, List.of(1L));
        Map<String, List<Long>> delivered = new LinkedHashMap<>();
        var seen = new HashSet<String>();
        try (var consumer = infrastructure.newConsumer("jpa-demo-" + UUID.randomUUID(), DemoContainerInitializer.TOPIC)) {
            long deadline = System.nanoTime() + Duration.ofSeconds(60).toNanos();
            int expectedCount = expected.values().stream().mapToInt(List::size).sum();
            while (seen.size() < expectedCount && System.nanoTime() < deadline) {
                for (var record : consumer.poll(Duration.ofMillis(200))) {
                    require(expected.containsKey(record.key()), "Unexpected aggregate in demo topic");
                    String eventId = new String(record.headers().lastHeader(CloudEventsHeaders.CE_ID).value(),
                            StandardCharsets.UTF_8);
                    // Delivery is at least once. Deduplicate by event id, never by seq.
                    if (!seen.add(eventId)) continue;
                    long seq = Long.parseLong(new String(
                            record.headers().lastHeader(CloudEventsHeaders.CE_SEQ).value(), StandardCharsets.UTF_8));
                    delivered.computeIfAbsent(record.key(), ignored -> new ArrayList<>()).add(seq);
                    System.out.printf("  partition:%d, aggregateId:%s, seq:%d%n", record.partition(), record.key(), seq);
                }
            }
        }
        require(expected.equals(delivered), "Kafka delivery did not match expected per-aggregate revisions: " + delivered);
        System.out.println("\n  Observed sequence per aggregate (without sorting):");
        delivered.forEach((id, seqs) -> System.out.printf("    %-20s → ce_seq %s%n", id, seqs));
        System.out.println("\n  ✓ All 10 committed events arrived in per-aggregate sequence order.");
        System.out.println("  Rolled-back events were absent. Duplicate deliveries are deduplicated by event ID.");
    }

    /** Demo-only state change, after Kafka delivery and the relay's acknowledgement. */
    private long manufactureAFailedRow() throws InterruptedException {
        long id = jdbc.queryForObject("SELECT id FROM tandem_outbox WHERE aggregate_id = ?", Long.class, ROLLBACK);
        long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
        while (System.nanoTime() < deadline) {
            // Conditional update cannot race the relay while it still owns an IN_FLIGHT row.
            if (jdbc.update("UPDATE tandem_outbox SET status = ?, last_error = ? WHERE id = ? AND status = ?",
                    OutboxStatus.FAILED.code(), "Simulated failure for the JPA demo", id, OutboxStatus.DONE.code()) == 1) {
                return id;
            }
            Thread.sleep(20);
        }
        throw new IllegalStateException("Relay did not acknowledge the demo event id:" + id);
    }

    private void printAdminApiHints(long failedId) {
        String port = environment.getProperty("local.server.port", environment.getProperty("server.port", "8080"));
        String base = "http://localhost:" + port + "/tandem/admin/v1";
        System.out.println("\n► Admin API: try it yourself (the app stays running; Ctrl+C stops it)\n");
        System.out.println("  curl " + base + "/outbox/summary");
        System.out.println("  curl " + base + "/outbox/messages");
        System.out.println("  curl " + base + "/outbox/messages/" + failedId);
        System.out.printf("%n  %s (id=%d) was delivered, then marked FAILED for this demo only.%n", ROLLBACK, failedId);
        System.out.println("  This is a simulated delivery failure, not a failed JPA transaction.");
        System.out.println("  Replay sends the same event again; consumers must deduplicate it.");
        System.out.println("  Choose replay or discard:");
        System.out.println("  curl -X POST " + base + "/outbox/messages/" + failedId + "/replay");
        System.out.println("  curl -X POST " + base + "/outbox/messages/" + failedId
                + "/discard -H 'Content-Type: application/json'"
                + " -d '{\"acknowledgeOrderingBreak\":true,\"reason\":\"demo\"}'");
        System.out.println("\n  Relay control (SINGLE coordination):");
        System.out.println("  curl " + base + "/relay/status");
        System.out.println("  curl -X POST " + base + "/relay/pause");
        System.out.println("  curl -X POST " + base + "/relay/resume");
        System.out.println("  Bucket and worker endpoints require LEASE coordination; SINGLE returns 409.");
        System.out.println("  See examples/tandem-sample-spring/run-lease.sh for that separate walkthrough.");
    }

    private static void banner() {
        System.out.println("=".repeat(60));
        System.out.println(" Tandem: the write side with Spring Boot 4 and JPA/Hibernate");
        System.out.println("=".repeat(60));
        System.out.println("\n Root locks serialize concurrent writers before the outbox insert;");
        System.out.println(" domain and outbox rows commit together. The relay delivers to Kafka.");
        System.out.println(" Event revisions and JPA versions are demonstrated on separate streams.");
        System.out.println(" Application code: OrderService. Infrastructure and this runner: demo-only.\n");
    }

    private static void await(CountDownLatch latch) {
        try {
            require(latch.await(30, TimeUnit.SECONDS), "Timed out waiting to commit the demo transaction");
        } catch (InterruptedException ex) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while waiting to commit", ex);
        }
    }

    private static void require(boolean condition, String message) {
        if (!condition) throw new IllegalStateException(message);
    }
}
