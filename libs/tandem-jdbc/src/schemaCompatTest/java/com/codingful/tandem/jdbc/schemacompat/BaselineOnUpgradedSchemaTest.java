package com.codingful.tandem.jdbc.schemacompat;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import com.codingful.tandem.core.AggregateId;
import com.codingful.tandem.core.BucketHash;
import com.codingful.tandem.core.BucketStatusView;
import com.codingful.tandem.core.LagSnapshot;
import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.core.OutboxRecord;
import com.codingful.tandem.core.OutboxRowDetail;
import com.codingful.tandem.core.OutboxRowView;
import com.codingful.tandem.core.OutboxSearchCriteria;
import com.codingful.tandem.core.OutboxStatus;
import com.codingful.tandem.core.RelayCoordinationMode;
import com.codingful.tandem.core.RelayStatusView;
import com.codingful.tandem.core.ReplayCriteria;
import com.codingful.tandem.core.ReplayResult;
import com.codingful.tandem.core.SeqSource;
import com.codingful.tandem.core.TandemHeaders;
import com.codingful.tandem.core.exception.DuplicateSeqException;
import com.codingful.tandem.core.exception.TandemConfigurationException;
import com.codingful.tandem.core.port.TandemMetrics;
import com.codingful.tandem.jdbc.BucketCountGuard;
import com.codingful.tandem.jdbc.BucketSource;
import com.codingful.tandem.jdbc.Coordination;
import com.codingful.tandem.jdbc.JdbcDiscardService;
import com.codingful.tandem.jdbc.JdbcOutboxQuery;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import com.codingful.tandem.jdbc.JdbcOutboxStore;
import com.codingful.tandem.jdbc.JdbcRelayControl;
import com.codingful.tandem.jdbc.JdbcRelayControlSource;
import com.codingful.tandem.jdbc.JdbcRelayQuery;
import com.codingful.tandem.jdbc.JdbcReplayService;
import com.codingful.tandem.jdbc.RelayConfig;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import javax.sql.DataSource;
import liquibase.Scope;
import liquibase.analytics.configuration.AnalyticsArgs;
import liquibase.command.CommandScope;
import liquibase.command.core.UpdateCommandStep;
import liquibase.command.core.helpers.DbUrlConnectionArgumentsCommandStep;
import liquibase.database.Database;
import liquibase.database.DatabaseFactory;
import liquibase.database.jvm.JdbcConnection;
import liquibase.resource.ClassLoaderResourceAccessor;
import liquibase.resource.DirectoryResourceAccessor;
import liquibase.resource.ResourceAccessor;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestMethodOrder;
import org.postgresql.ds.PGSimpleDataSource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * The schema compatibility scenario (LLD-api-stability §5.2), compiled and run against the baseline release
 * of {@code tandem-jdbc}: does the current changelog apply to a database the baseline populated, and does
 * the baseline still work once it has.
 *
 * <p>The setup, run once before the first step, starts PostgreSQL, applies the changelog the baseline jar
 * ships (phase 1) and has the baseline adapter write, claim and complete rows on it (phase 2). Every test
 * method is one step of phase 3 (the upgrade) or phase 4 (the baseline on the upgraded schema), run in
 * order. {@code schemaCompatTest} tells the two apart: a declared break accepts the failure of a step, never
 * one of the setup, which JUnit reports against the class.
 *
 * <p>Only API the baseline has may appear here. Calls go straight to the adapter's operations, so the
 * statements run in a fixed order with no worker thread to wait on. Each step works on aggregates of its
 * own, every one in a bucket of its own, so a claim reaches only the rows of the step that makes it.
 */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class BaselineOnUpgradedSchemaTest {

    /** The changelog's path inside the {@code tandem-jdbc} jar, the one operators point Liquibase at. */
    private static final String CHANGELOG = "tandem/schema/postgres/changelog/db.changelog-master.xml";

    /** Where the task hands over the module's processed resources, which hold the current changelog. */
    private static final String CURRENT_RESOURCES_PROPERTY = "tandem.schema-compat.current-resources";

    private static final String POSTGRES_IMAGE_PROPERTY = "tandem.test.postgres.image";
    private static final String POSTGRES_IMAGE_ENV = "TANDEM_TEST_POSTGRES_IMAGE";

    /**
     * The image {@code TandemTestContainer.postgresImage()} starts unless one is named, restated because no
     * project of the build is on this source set's classpath.
     */
    private static final String DEFAULT_POSTGRES_IMAGE = "postgres:16-alpine";

    /** The bucket count the shipped changelog seeds {@code tandem_bucket_lease} with. */
    private static final int BUCKET_COUNT = 256;

    private static final int MAX_ATTEMPTS = 3;
    private static final int BATCH = 100;
    private static final Duration LEASE = Duration.ofMinutes(10);
    private static final Duration ALREADY_EXPIRED = Duration.ofSeconds(-1);
    private static final Duration HEARTBEAT_INTERVAL = Duration.ofMinutes(1);
    private static final String RELAY = "relay-before-upgrade";
    private static final String JOINING_RELAY = "relay-joining";
    private static final String WORKER = "worker-1";
    private static final String AGGREGATE_TYPE = "Order";
    private static final String EVENT_TYPE = "com.acme.order.placed";
    private static final String CONTENT_TYPE = "application/json";
    private static final String HEADER = "tenant";
    private static final String HEADER_VALUE = "acme";
    private static final String CORRELATION_ID = "incident-4711";
    private static final String DELIVERY_ERROR = "broker unavailable";
    private static final String POISON = "poison message";
    private static final String DISCARD_REASON = "payload rejected by every consumer";
    /** A payload as {@code jsonb} renders it back, so the bytes read equal the bytes written. */
    private static final byte[] PAYLOAD = "{\"total\": 42}".getBytes(StandardCharsets.UTF_8);
    private static final System.Logger LOG = System.getLogger(BaselineOnUpgradedSchemaTest.class.getName());

    private final Set<Integer> usedBuckets = new HashSet<>();
    private final List<Case> writtenBefore = cases("before");
    private final List<Case> writtenAfter = cases("after");
    private final String retried = aggregate("retried");
    private final String quarantined = aggregate("quarantined");
    private final String reclaimed = aggregate("reclaimed");
    private final String searched = aggregate("searched");
    private final String operated = aggregate("operated");
    private final String observed = aggregate("observed");

    private PostgreSQLContainer<?> postgres;
    private DataSource dataSource;
    private JdbcOutboxRepository repository;
    private JdbcOutboxStore store;
    private final Map<String, Long> firstSeqBefore = new HashMap<>();
    private final List<OutboxRecord> inFlightAtUpgrade = new ArrayList<>();
    private boolean upgraded;

    /** An aggregate written in one {@code seq} mode, with or without the write-side lock. */
    private record Case(String aggregate, SeqSource mode, boolean locked) {
    }

    @BeforeAll
    void startPostgresWithTheBaselineSchemaAndRows() throws Exception {
        postgres = new PostgreSQLContainer<>(DockerImageName.parse(postgresImage()).asCompatibleSubstituteFor("postgres"));
        postgres.start();
        PGSimpleDataSource source = new PGSimpleDataSource();
        source.setUrl(postgres.getJdbcUrl());
        source.setUser(postgres.getUsername());
        source.setPassword(postgres.getPassword());
        dataSource = source;
        repository = new JdbcOutboxRepository(dataSource, BUCKET_COUNT);
        store = new JdbcOutboxStore(dataSource, MAX_ATTEMPTS);

        // Phase 1: the changelog the baseline jar ships, at the path operators reference it by.
        update(new ClassLoaderResourceAccessor(getClass().getClassLoader()));

        // Phase 2. First the startup steps a relay runs, so every tandem_* table holds rows at the upgrade.
        BucketCountGuard.check(dataSource, BUCKET_COUNT);
        new JdbcRelayControlSource(dataSource, Coordination.LEASE, HEARTBEAT_INTERVAL).onStart();
        BucketSource relay = BucketSource.forCoordination(leaseConfig(RELAY), dataSource);
        relay.validateOnStart(TandemMetrics.NOOP, LOG);
        relay.heartbeat();
        // Then two rows per case, the first written alone, the second in one batch across the cases.
        writtenBefore.forEach(written -> repository.insert(message(written, 1)));
        repository.insertAll(writtenBefore.stream().map(written -> message(written, 2)).toList());
        // The relay claims the head of each case and completes half of them: the other half is still in
        // flight when the upgrade runs, as a rolling upgrade leaves it.
        List<OutboxRecord> claimed = store.claimBatch(bucketsOf(writtenBefore), WORKER, LEASE, BATCH);
        assertThat(claimed).hasSameSizeAs(writtenBefore);
        claimed.forEach(record -> firstSeqBefore.put(record.aggregateId().value(), record.hasSeq() ? record.seq() : 0));
        for (int i = 0; i < claimed.size(); i++) {
            if (i % 2 == 0) {
                store.markDone(claimed.get(i));
            } else {
                inFlightAtUpgrade.add(claimed.get(i));
            }
        }
    }

    @AfterAll
    void stopPostgres() {
        if (postgres != null) {
            postgres.stop();
        }
    }

    @Test
    @Order(1)
    void GIVEN_a_database_the_baseline_populated_WHEN_the_current_changelog_is_applied_THEN_it_upgrades_it_and_keeps_every_row()
            throws Exception {
        List<String> rowsBefore = rowsWithStatus();
        Set<String> appliedBefore = appliedChangesets();

        update(new DirectoryResourceAccessor(currentResources()));

        assertThat(appliedChangesets()).containsAll(appliedBefore);
        assertThat(rowsWithStatus()).isEqualTo(rowsBefore);
        upgraded = true;
    }

    @Test
    @Order(2)
    void GIVEN_rows_claimed_before_the_upgrade_WHEN_the_baseline_completes_them_THEN_the_rows_behind_them_are_delivered_as_written() {
        requireUpgrade();

        inFlightAtUpgrade.forEach(store::markDone);
        assertThat(inFlightAtUpgrade).allSatisfy(record -> assertThat(row(record.id()).status()).isEqualTo(OutboxStatus.DONE));

        List<OutboxRecord> behind = store.claimBatch(bucketsOf(writtenBefore), WORKER, LEASE, BATCH);
        assertSecondRows(behind, writtenBefore, firstSeqBefore);
        store.markDoneBatch(behind);
        assertThat(countWhere("aggregate_id = ANY(?) AND status <> ?",
                aggregatesOf(writtenBefore).toArray(String[]::new), OutboxStatus.DONE.code())).isZero();
    }

    @Test
    @Order(3)
    void GIVEN_the_upgraded_schema_WHEN_the_baseline_writes_in_every_seq_mode_THEN_each_row_is_claimed_as_written() {
        requireUpgrade();
        long pendingBefore = store.lag().orElseThrow().pending();

        writtenAfter.forEach(written -> repository.insert(message(written, 1)));
        repository.insertAll(writtenAfter.stream().map(written -> message(written, 2)).toList());

        LagSnapshot lag = store.lag().orElseThrow();
        assertThat(lag.pending()).isEqualTo(pendingBefore + 2L * writtenAfter.size());
        assertThat(lag.oldestSince()).isPresent();
        List<OutboxRecord> heads = store.claimBatch(bucketsOf(writtenAfter), WORKER, LEASE, BATCH);
        assertThat(heads).extracting(record -> record.aggregateId().value())
                .containsExactlyInAnyOrderElementsOf(aggregatesOf(writtenAfter));
        Map<String, Long> firstSeq = new HashMap<>();
        for (OutboxRecord head : heads) {
            Case written = caseOf(writtenAfter, head);
            assertWrittenAs(head, written, 1);
            firstSeq.put(written.aggregate(), head.hasSeq() ? head.seq() : 0);
        }
        store.markDoneBatch(heads);
        assertSecondRows(store.claimBatch(bucketsOf(writtenAfter), WORKER, LEASE, BATCH), writtenAfter, firstSeq);

        Case application = writtenAfter.stream().filter(written -> written.mode() == SeqSource.APPLICATION)
                .findFirst().orElseThrow();
        assertThatThrownBy(() -> repository.insert(message(application, 1))).isInstanceOf(DuplicateSeqException.class);
    }

    @Test
    @Order(4)
    void GIVEN_the_upgraded_schema_WHEN_a_delivery_is_retried_THEN_the_row_is_claimed_again_once_due() {
        requireUpgrade();
        repository.insert(message(retried, 1));
        OutboxRecord claimed = claimOne(retried, LEASE);

        store.markForRetry(claimed, DELIVERY_ERROR, null);
        OutboxRecord again = claimOne(retried, LEASE);
        assertThat(again.id()).isEqualTo(claimed.id());
        assertThat(again.attempts()).isEqualTo(1);
        assertThat(again.lastError()).isEqualTo(DELIVERY_ERROR);

        store.markForRetry(again, DELIVERY_ERROR, Duration.ofHours(1));
        assertThat(claim(retried, LEASE)).isEmpty();
        Row row = row(again.id());
        assertThat(row.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.attempts()).isEqualTo(2);
        assertThat(row.lockedBy()).isNull();
        assertThat(row.due()).isFalse();
    }

    @Test
    @Order(5)
    void GIVEN_the_upgraded_schema_WHEN_a_delivery_fails_for_good_THEN_the_row_is_quarantined_and_holds_back_its_aggregate() {
        requireUpgrade();
        long failedBefore = store.failedCount().orElseThrow();
        long blockedBefore = store.blockedCount().orElseThrow();
        for (long seq = 1; seq <= 3; seq++) {
            repository.insert(message(quarantined, seq));
        }
        OutboxRecord head = claimOne(quarantined, LEASE);

        store.markFailed(head, POISON);

        Row row = row(head.id());
        assertThat(row.status()).isEqualTo(OutboxStatus.FAILED);
        assertThat(row.attempts()).isEqualTo(1);
        assertThat(row.lastError()).isEqualTo(POISON);
        assertThat(row.lockedBy()).isNull();
        assertThat(claim(quarantined, LEASE)).isEmpty();
        assertThat(store.failedCount()).hasValue(failedBefore + 1);
        assertThat(store.blockedCount()).hasValue(blockedBefore + 2);
    }

    @Test
    @Order(6)
    void GIVEN_the_upgraded_schema_WHEN_leases_expire_THEN_the_rows_return_to_pending_until_the_last_attempt_quarantines_them() {
        requireUpgrade();
        repository.insert(message(reclaimed, 1));
        long id = 0;
        for (int attempt = 1; attempt <= MAX_ATTEMPTS; attempt++) {
            id = claimOne(reclaimed, ALREADY_EXPIRED).id();

            assertThat(store.reclaimExpiredLeases()).isEqualTo(1);

            Row row = row(id);
            assertThat(row.status()).isEqualTo(attempt < MAX_ATTEMPTS ? OutboxStatus.PENDING : OutboxStatus.FAILED);
            assertThat(row.attempts()).isEqualTo(attempt);
            assertThat(row.lockedBy()).isNull();
            assertThat(row.lastError()).isNotBlank();
        }
        assertThat(claim(reclaimed, LEASE)).isEmpty();
        assertThat(row(id).status()).isEqualTo(OutboxStatus.FAILED);
    }

    @Test
    @Order(7)
    void GIVEN_the_upgraded_schema_WHEN_cleanup_runs_THEN_it_deletes_only_finished_rows_older_than_the_cutoff_by_batches() {
        requireUpgrade();
        Object[] finished = {OutboxStatus.DONE.code(), OutboxStatus.DISCARDED.code()};
        long finishedRows = countWhere("status IN (?, ?)", finished);
        long otherRows = countWhere("status NOT IN (?, ?)", finished);
        assertThat(finishedRows).isGreaterThan(1);
        Instant cutoff = Instant.now().plus(Duration.ofDays(1));

        assertThat(store.cleanup(Instant.now().minus(Duration.ofDays(1)), BATCH)).isZero();
        assertThat(store.cleanup(cutoff, 1)).isEqualTo(1);
        assertThat(store.cleanup(cutoff, (int) finishedRows)).isEqualTo(finishedRows - 1);

        assertThat(countWhere("status IN (?, ?)", finished)).isZero();
        assertThat(countWhere("status NOT IN (?, ?)", finished)).isEqualTo(otherRows);
    }

    @Test
    @Order(8)
    void GIVEN_the_upgraded_schema_WHEN_a_second_relay_joins_and_leaves_THEN_the_buckets_are_shared_and_taken_back() {
        requireUpgrade();
        BucketSource relay = BucketSource.forCoordination(leaseConfig(RELAY), dataSource);
        BucketSource joining = BucketSource.forCoordination(leaseConfig(JOINING_RELAY), dataSource);
        relay.validateOnStart(TandemMetrics.NOOP, LOG);
        relay.heartbeat();
        assertThat(relay.ownedBuckets()).hasSize(BUCKET_COUNT);

        joining.heartbeat();
        assertThat(joining.ownedBuckets()).isEmpty();
        relay.heartbeat();
        joining.heartbeat();

        Set<Integer> relayBuckets = relay.ownedBuckets();
        Set<Integer> joiningBuckets = joining.ownedBuckets();
        assertThat(relayBuckets).hasSize(BUCKET_COUNT / 2);
        assertThat(joiningBuckets).hasSize(BUCKET_COUNT / 2).doesNotContainAnyElementsOf(relayBuckets);
        assertThat(relay.uncoveredBuckets()).hasValue(0);

        joining.release();
        assertThat(joining.ownedBuckets()).isEmpty();
        relay.heartbeat();
        assertThat(relay.ownedBuckets()).hasSize(BUCKET_COUNT);
    }

    @Test
    @Order(9)
    void GIVEN_the_upgraded_schema_WHEN_an_operator_pauses_and_resumes_THEN_the_relay_reads_it_on_refresh() {
        requireUpgrade();
        JdbcRelayControlSource controlSource = new JdbcRelayControlSource(dataSource, Coordination.LEASE,
                HEARTBEAT_INTERVAL);
        JdbcRelayControl control = new JdbcRelayControl(dataSource);
        int paused = bucketOf(operated);
        int other = bucketOf(observed);
        controlSource.onStart();
        controlSource.heartbeat();
        controlSource.refresh();
        assertThat(controlSource.wholeRelayPaused()).isFalse();
        assertThat(controlSource.bucketPaused(paused)).isFalse();

        control.pauseAll();
        assertThat(control.pauseBucket(paused)).isTrue();
        controlSource.refresh();
        assertThat(controlSource.wholeRelayPaused()).isTrue();
        assertThat(controlSource.bucketPaused(paused)).isTrue();
        assertThat(controlSource.bucketPaused(other)).isFalse();

        control.resumeAll();
        assertThat(control.resumeBucket(paused)).isTrue();
        controlSource.refresh();
        assertThat(controlSource.wholeRelayPaused()).isFalse();
        assertThat(controlSource.bucketPaused(paused)).isFalse();
    }

    @Test
    @Order(10)
    void GIVEN_the_upgraded_schema_WHEN_the_bucket_count_is_checked_THEN_the_stored_count_passes_and_another_is_refused() {
        requireUpgrade();

        assertThatCode(() -> BucketCountGuard.check(dataSource, BUCKET_COUNT)).doesNotThrowAnyException();
        assertThatThrownBy(() -> BucketCountGuard.check(dataSource, BUCKET_COUNT / 2))
                .isInstanceOf(TandemConfigurationException.class);
    }

    @Test
    @Order(11)
    void GIVEN_the_upgraded_schema_WHEN_an_operator_searches_the_outbox_THEN_counts_rows_and_details_read_as_written() {
        requireUpgrade();
        JdbcOutboxQuery query = new JdbcOutboxQuery(dataSource);
        Map<OutboxStatus, Long> countsBefore = query.statusCounts();
        List<Long> ids = writeDoneFailedPending(searched);

        Map<OutboxStatus, Long> counts = query.statusCounts();
        for (OutboxStatus status : countsBefore.keySet()) {
            long added = status == OutboxStatus.DONE || status == OutboxStatus.FAILED || status == OutboxStatus.PENDING
                    ? 1 : 0;
            assertThat(counts.get(status)).as("%s rows", status).isEqualTo(countsBefore.get(status) + added);
        }
        AggregateId aggregate = AggregateId.of(searched);
        List<OutboxRowView> rows = query.search(OutboxSearchCriteria.builder().aggregateId(aggregate).build());
        assertThat(rows).extracting(OutboxRowView::id).containsExactlyElementsOf(ids);
        assertThat(rows).extracting(OutboxRowView::status)
                .containsExactly(OutboxStatus.DONE, OutboxStatus.FAILED, OutboxStatus.PENDING);
        assertThat(rows).extracting(OutboxRowView::seq).containsExactly(1L, 2L, 3L);
        assertThat(rows).extracting(OutboxRowView::attempts).containsExactly(0, 1, 0);
        assertThat(rows).extracting(OutboxRowView::lastError).containsExactly(null, POISON, null);
        assertThat(rows).allSatisfy(view -> {
            assertThat(view.aggregateType()).isEqualTo(AGGREGATE_TYPE);
            assertThat(view.type()).isEqualTo(EVENT_TYPE);
            assertThat(view.correlationId()).isEqualTo(CORRELATION_ID);
            assertThat(view.replays()).isZero();
        });
        assertThat(query.search(OutboxSearchCriteria.builder().correlationId(CORRELATION_ID).build()))
                .extracting(OutboxRowView::id).containsExactlyElementsOf(ids);
        assertThat(query.search(OutboxSearchCriteria.builder().aggregateId(aggregate).status(OutboxStatus.FAILED).build()))
                .extracting(OutboxRowView::id).containsExactly(ids.get(1));
        assertThat(query.search(OutboxSearchCriteria.builder().aggregateType(AGGREGATE_TYPE).type(EVENT_TYPE)
                .afterId(ids.get(0)).createdFrom(Instant.now().minus(Duration.ofHours(1)))
                .createdTo(Instant.now().plus(Duration.ofHours(1))).limit(1).build()))
                .extracting(OutboxRowView::id).containsExactly(ids.get(1));

        OutboxRowDetail detail = query.findById(ids.get(1)).orElseThrow();
        assertThat(detail.view().id()).isEqualTo(ids.get(1));
        assertThat(detail.payload()).isEqualTo(PAYLOAD);
        assertThat(detail.headers()).containsEntry(HEADER, HEADER_VALUE)
                .containsEntry(TandemHeaders.CONTENT_TYPE, CONTENT_TYPE)
                .containsEntry(TandemHeaders.CORRELATION_ID, CORRELATION_ID);
        assertThat(query.findById(-1)).isEmpty();
    }

    @Test
    @Order(12)
    void GIVEN_the_upgraded_schema_WHEN_an_operator_discards_and_replays_THEN_only_the_eligible_rows_change() {
        requireUpgrade();
        JdbcDiscardService discard = new JdbcDiscardService(dataSource);
        JdbcReplayService replay = new JdbcReplayService(dataSource);
        List<Long> ids = writeDoneFailedPending(operated);
        long done = ids.get(0);
        long failed = ids.get(1);
        long pending = ids.get(2);

        assertThat(discard.discard(done, DISCARD_REASON)).isFalse();
        assertThat(discard.discard(failed, DISCARD_REASON)).isTrue();
        assertThat(row(failed).status()).isEqualTo(OutboxStatus.DISCARDED);
        assertThat(discardReasonOf(failed)).isEqualTo(DISCARD_REASON);

        AggregateId aggregate = AggregateId.of(operated);
        ReplayResult dryRun = replay.replay(new ReplayCriteria(aggregate, null, null, null, Set.of(), true));
        assertThat(dryRun.matched()).isEqualTo(1);
        assertThat(dryRun.replayed()).isZero();
        assertThat(row(done).status()).isEqualTo(OutboxStatus.DONE);

        ReplayResult replayed = replay.replay(new ReplayCriteria(aggregate, AGGREGATE_TYPE, done, pending,
                Set.of(OutboxStatus.DONE, OutboxStatus.FAILED), false));
        assertThat(replayed.matched()).isEqualTo(1);
        assertThat(replayed.replayed()).isEqualTo(1);
        Row row = row(done);
        assertThat(row.status()).isEqualTo(OutboxStatus.PENDING);
        assertThat(row.attempts()).isZero();
        assertThat(row.due()).isTrue();
        assertThat(store.replaysOf(done)).hasValue(1);
        assertThat(store.replaysOf(pending)).hasValue(0);
        assertThat(row(failed).status()).isEqualTo(OutboxStatus.DISCARDED);
    }

    @Test
    @Order(13)
    void GIVEN_the_upgraded_schema_WHEN_an_operator_inspects_and_steers_the_relay_THEN_its_views_follow_every_change() {
        requireUpgrade();
        JdbcRelayQuery relayQuery = new JdbcRelayQuery(dataSource);
        JdbcRelayControl control = new JdbcRelayControl(dataSource);
        JdbcRelayControlSource controlSource = new JdbcRelayControlSource(dataSource, Coordination.LEASE,
                HEARTBEAT_INTERVAL);
        BucketSource relay = BucketSource.forCoordination(leaseConfig(RELAY), dataSource);
        controlSource.onStart();
        controlSource.heartbeat();
        relay.heartbeat();
        repository.insert(message(observed, 1));
        int bucket = bucketOf(observed);

        assertThat(relayQuery.coordinationMode()).isEqualTo(RelayCoordinationMode.LEASE);
        RelayStatusView status = relayQuery.status();
        assertThat(status.alive()).isTrue();
        assertThat(status.paused()).isFalse();
        assertThat(status.bucketCount()).isEqualTo(BUCKET_COUNT);
        assertThat(status.uncoveredBuckets()).isZero();
        assertThat(status.workers()).isEqualTo(1);
        assertThat(relayQuery.workers()).singleElement().satisfies(worker -> {
            assertThat(worker.instanceId()).isEqualTo(RELAY);
            assertThat(worker.bucketCount()).isEqualTo(BUCKET_COUNT);
            assertThat(worker.lastHeartbeat()).isNotNull();
        });
        List<BucketStatusView> buckets = relayQuery.buckets(false);
        assertThat(buckets).extracting(BucketStatusView::bucket)
                .containsExactlyElementsOf(IntStream.range(0, BUCKET_COUNT).boxed().toList());
        assertThat(buckets).allSatisfy(view -> {
            assertThat(view.owner()).isEqualTo(RELAY);
            assertThat(view.covered()).isTrue();
        });
        BucketStatusView observedBucket = relayQuery.bucket(bucket).orElseThrow();
        assertThat(observedBucket.pendingCount()).isEqualTo(1);
        assertThat(observedBucket.lagAgeSeconds()).isNotNull();
        assertThat(relayQuery.bucket(BUCKET_COUNT)).isEmpty();

        assertThat(control.pauseBucket(bucket)).isTrue();
        assertThat(relayQuery.bucket(bucket).orElseThrow().paused()).isTrue();
        assertThat(control.resumeBucket(bucket)).isTrue();
        assertThat(relayQuery.bucket(bucket).orElseThrow().paused()).isFalse();
        assertThat(control.pauseBucket(BUCKET_COUNT)).isFalse();

        assertThat(control.releaseBucket(bucket)).isTrue();
        BucketStatusView released = relayQuery.bucket(bucket).orElseThrow();
        assertThat(released.owner()).isNull();
        assertThat(released.covered()).isFalse();
        assertThat(relayQuery.buckets(true)).extracting(BucketStatusView::bucket).containsExactly(bucket);
        assertThat(relayQuery.status().uncoveredBuckets()).isEqualTo(1);
        relay.heartbeat();
        assertThat(relayQuery.buckets(true)).isEmpty();

        control.pauseAll();
        assertThat(relayQuery.status().paused()).isTrue();
        control.resumeAll();
        assertThat(relayQuery.status().paused()).isFalse();
    }

    // ---------------------------------------------------------------------------------------------------
    // The scenario's own reads: plain SQL on the columns the step's own statements write, so a step observes
    // the database without going through another step's statements, and fails only on what it exercises.
    // ---------------------------------------------------------------------------------------------------

    /** The delivery state of one row, as the store's transitions leave it. */
    private record Row(OutboxStatus status, int attempts, String lastError, String lockedBy, boolean due) {
    }

    private Row row(long id) {
        return query("SELECT status, attempts, last_error, locked_by,"
                + " (next_attempt_at IS NULL OR next_attempt_at <= now()) AS due"
                + " FROM tandem_outbox WHERE id = ?", List.of(id), rs -> {
            assertThat(rs.next()).as("row %d exists", id).isTrue();
            return new Row(OutboxStatus.fromCode(rs.getInt("status")), rs.getInt("attempts"), rs.getString("last_error"),
                    rs.getString("locked_by"), rs.getBoolean("due"));
        });
    }

    private String discardReasonOf(long id) {
        return query("SELECT discard_reason FROM tandem_outbox WHERE id = ?", List.of(id), rs -> {
            assertThat(rs.next()).as("row %d exists", id).isTrue();
            return rs.getString(1);
        });
    }

    private List<String> rowsWithStatus() {
        return query("SELECT id, status FROM tandem_outbox ORDER BY id", List.of(), rs -> {
            List<String> rows = new ArrayList<>();
            while (rs.next()) {
                rows.add(rs.getLong("id") + ":" + rs.getInt("status"));
            }
            return rows;
        });
    }

    private Set<String> appliedChangesets() {
        return query("SELECT id, author, filename FROM databasechangelog", List.of(), rs -> {
            Set<String> applied = new LinkedHashSet<>();
            while (rs.next()) {
                applied.add(rs.getString("filename") + "::" + rs.getString("id") + "::" + rs.getString("author"));
            }
            return applied;
        });
    }

    private long countWhere(String predicate, Object... parameters) {
        return query("SELECT count(*) FROM tandem_outbox WHERE " + predicate, List.of(parameters), rs -> {
            rs.next();
            return rs.getLong(1);
        });
    }

    private interface ResultReader<T> {
        T read(ResultSet rs) throws SQLException;
    }

    private <T> T query(String sql, List<Object> parameters, ResultReader<T> reader) {
        try (Connection connection = dataSource.getConnection();
             PreparedStatement statement = connection.prepareStatement(sql)) {
            for (int i = 0; i < parameters.size(); i++) {
                Object parameter = parameters.get(i);
                if (parameter instanceof String[] array) {
                    statement.setArray(i + 1, connection.createArrayOf("varchar", array));
                } else {
                    statement.setObject(i + 1, parameter);
                }
            }
            try (ResultSet rs = statement.executeQuery()) {
                return reader.read(rs);
            }
        } catch (SQLException e) {
            throw new IllegalStateException(sql, e);
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Writing and claiming through the baseline adapter.
    // ---------------------------------------------------------------------------------------------------

    /** Three rows of one aggregate, left DONE, FAILED and PENDING in that order. */
    private List<Long> writeDoneFailedPending(String aggregate) {
        for (long seq = 1; seq <= 3; seq++) {
            repository.insert(OutboxMessage.builder().aggregateId(aggregate).aggregateType(AGGREGATE_TYPE)
                    .type(EVENT_TYPE).seq(seq).payload(PAYLOAD).contentType(CONTENT_TYPE).header(HEADER, HEADER_VALUE)
                    .header(TandemHeaders.CORRELATION_ID, CORRELATION_ID).build());
        }
        OutboxRecord done = claimOne(aggregate, LEASE);
        store.markDone(done);
        OutboxRecord failed = claimOne(aggregate, LEASE);
        store.markFailed(failed, POISON);
        long pending = query("SELECT max(id) FROM tandem_outbox WHERE aggregate_id = ?", List.of(aggregate), rs -> {
            rs.next();
            return rs.getLong(1);
        });
        return List.of(done.id(), failed.id(), pending);
    }

    private OutboxMessage message(Case written, long seq) {
        OutboxMessage.Builder builder = OutboxMessage.builder().aggregateId(written.aggregate())
                .aggregateType(AGGREGATE_TYPE).type(EVENT_TYPE).payload(PAYLOAD).header(HEADER, HEADER_VALUE);
        switch (written.mode()) {
            case APPLICATION -> builder.seq(seq);
            case MANAGED -> builder.managedSeq();
            case NONE -> builder.unsequenced();
            default -> throw new IllegalArgumentException("No writer for seq mode " + written.mode());
        }
        if (written.locked()) {
            builder.lockedWrite();
        }
        return builder.build();
    }

    private OutboxMessage message(String aggregate, long seq) {
        return message(new Case(aggregate, SeqSource.APPLICATION, false), seq);
    }

    private List<OutboxRecord> claim(String aggregate, Duration lease) {
        return store.claimBatch(Set.of(bucketOf(aggregate)), WORKER, lease, BATCH);
    }

    private OutboxRecord claimOne(String aggregate, Duration lease) {
        List<OutboxRecord> claimed = claim(aggregate, lease);
        assertThat(claimed).as("rows claimed for %s", aggregate).hasSize(1);
        return claimed.get(0);
    }

    /** A row of {@code written} as the claim returns it: its {@code n}th, carrying what was written. */
    private static void assertWrittenAs(OutboxRecord record, Case written, long n) {
        assertThat(record.status()).isEqualTo(OutboxStatus.IN_FLIGHT);
        assertThat(record.lockedBy()).isEqualTo(WORKER);
        assertThat(record.seqSource()).isEqualTo(written.mode());
        assertThat(record.aggregateType()).isEqualTo(AGGREGATE_TYPE);
        assertThat(record.type()).isEqualTo(EVENT_TYPE);
        assertThat(record.payload()).isEqualTo(PAYLOAD);
        assertThat(record.headers()).containsEntry(HEADER, HEADER_VALUE);
        assertThat(record.attempts()).isZero();
        switch (written.mode()) {
            case APPLICATION -> assertThat(record.seq()).isEqualTo(n);
            case MANAGED -> assertThat(record.hasSeq()).isTrue();
            case NONE -> assertThat(record.hasSeq()).isFalse();
            default -> throw new IllegalArgumentException("No expectation for seq mode " + written.mode());
        }
    }

    /** The second row of every case, claimed once the first is done; a managed number grows. */
    private static void assertSecondRows(List<OutboxRecord> claimed, List<Case> cases, Map<String, Long> firstSeq) {
        assertThat(claimed).extracting(record -> record.aggregateId().value())
                .containsExactlyInAnyOrderElementsOf(aggregatesOf(cases));
        for (OutboxRecord record : claimed) {
            Case written = caseOf(cases, record);
            assertWrittenAs(record, written, 2);
            if (written.mode() == SeqSource.MANAGED) {
                assertThat(record.seq()).isGreaterThan(firstSeq.get(written.aggregate()));
            }
        }
    }

    // ---------------------------------------------------------------------------------------------------
    // Setup helpers.
    // ---------------------------------------------------------------------------------------------------

    /** Applies the changelog at {@link #CHANGELOG}, as {@code resources} hold it, then closes them. */
    private void update(ResourceAccessor resources) throws Exception {
        try (Connection connection = dataSource.getConnection()) {
            Database database = DatabaseFactory.getInstance()
                    .findCorrectDatabaseImplementation(new JdbcConnection(connection));
            Map<String, Object> scope = Map.of(Scope.Attr.resourceAccessor.name(), resources,
                    AnalyticsArgs.ENABLED.getKey(), false);
            Scope.child(scope, () -> {
                new CommandScope(UpdateCommandStep.COMMAND_NAME)
                        .addArgumentValue(DbUrlConnectionArgumentsCommandStep.DATABASE_ARG, database)
                        .addArgumentValue(UpdateCommandStep.CHANGELOG_FILE_ARG, CHANGELOG)
                        .execute();
            });
        } finally {
            resources.close();
        }
    }

    private static Path currentResources() {
        String directory = System.getProperty(CURRENT_RESOURCES_PROPERTY);
        assertThat(directory).as("system property %s", CURRENT_RESOURCES_PROPERTY).isNotBlank();
        return Path.of(directory);
    }

    private static String postgresImage() {
        String configured = System.getProperty(POSTGRES_IMAGE_PROPERTY);
        if (configured == null || configured.isBlank()) {
            configured = System.getenv(POSTGRES_IMAGE_ENV);
        }
        return configured == null || configured.isBlank() ? DEFAULT_POSTGRES_IMAGE : configured;
    }

    private static RelayConfig leaseConfig(String instanceId) {
        return RelayConfig.builder().bucketCount(BUCKET_COUNT).coordination(Coordination.LEASE).instanceId(instanceId)
                .bucketLease(LEASE).build();
    }

    private void requireUpgrade() {
        Assumptions.assumeTrue(upgraded, "the current changelog did not apply to the baseline's database");
    }

    /** One case per {@code seq} mode, each with and without the write-side lock. */
    private List<Case> cases(String prefix) {
        List<Case> cases = new ArrayList<>();
        for (SeqSource mode : List.of(SeqSource.APPLICATION, SeqSource.MANAGED, SeqSource.NONE)) {
            for (boolean locked : List.of(false, true)) {
                String name = prefix + "-" + mode.name().toLowerCase(Locale.ROOT) + (locked ? "-locked" : "");
                cases.add(new Case(aggregate(name), mode, locked));
            }
        }
        return List.copyOf(cases);
    }

    /** An aggregate id in a bucket no other aggregate of the scenario uses. */
    private String aggregate(String name) {
        for (int suffix = 0; ; suffix++) {
            String id = name + "-" + suffix;
            if (usedBuckets.add(bucketOf(id))) {
                return id;
            }
        }
    }

    private static int bucketOf(String aggregate) {
        return BucketHash.bucketFor(aggregate, BUCKET_COUNT);
    }

    private static Set<Integer> bucketsOf(List<Case> cases) {
        return cases.stream().map(written -> bucketOf(written.aggregate())).collect(Collectors.toSet());
    }

    private static List<String> aggregatesOf(List<Case> cases) {
        return cases.stream().map(Case::aggregate).toList();
    }

    private static Case caseOf(List<Case> cases, OutboxRecord record) {
        return cases.stream().filter(written -> written.aggregate().equals(record.aggregateId().value()))
                .findFirst().orElseThrow();
    }
}
