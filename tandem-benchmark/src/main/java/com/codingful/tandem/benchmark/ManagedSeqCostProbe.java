package com.codingful.tandem.benchmark;

import com.codingful.tandem.core.OutboxMessage;
import com.codingful.tandem.jdbc.JdbcOutboxRepository;
import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;
import javax.sql.DataSource;
import org.HdrHistogram.Histogram;
import org.HdrHistogram.Recorder;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.utility.DockerImageName;

/**
 * Measures what {@link OutboxMessage.Builder#managedSeq()} actually costs the caller's transaction,
 * against {@code seq(long)} and {@link OutboxMessage.Builder#unsequenced()} on the same write path
 * (LLD-benchmark §6.5; the results it produced are HLD-managed-seq §3.4).
 *
 * <p>HLD-managed-seq §4.1/§5 states the managed number costs "no extra statement, no round trip and
 * no lock" — true of the <i>statement</i>, since the {@code tandem_seq} DEFAULT fires inside the
 * INSERT. What the statement claim does not cover is the open question this probe answers:
 * {@code tandem_seq} is a <b>single global sequence, pinned to {@code CACHE 1}</b> and touched by
 * every managed insert of every aggregate, so each {@code nextval} takes an exclusive lock on the
 * same sequence page — a contention point shared by every writer. Cheaper than a row lock, but not
 * automatically "none".
 *
 * <p>Four measurements, all against a real PostgreSQL through the real {@link JdbcOutboxRepository}:
 * <ol>
 *   <li><b>Saturation sweep</b> — every writer inserting flat out, per arm, over a sweep of writer
 *       counts. Contention is a function of concurrent writers, so a sweep is what can show the
 *       managed arm degrading faster than the other two. Run with the container's default
 *       {@code fsync=off}: the commit fsync is a large constant that would mask a small per-insert
 *       cost, so removing it is the <i>sensitive</i> setting, not a flattering one.</li>
 *   <li><b>Durable</b> — the same contrast once more with {@code fsync=on}, the picture a real
 *       deployment sees.</li>
 *   <li><b>Paced</b> — every arm at one offered rate well below saturation, durable: the honest
 *       comparison of write latency and its tail, since at saturation a tail is queueing, not cost.</li>
 *   <li><b>Sequence ceiling</b> — {@code nextval} driven server-side over {@code generate_series}, so
 *       no client round trip is in the way: the raw ops/s the {@code CACHE 1} sequence sustains, next
 *       to a {@code CACHE 32} sequence (the mitigation the wire contract forbids, HLD-managed-seq §7)
 *       and a loop of the same shape calling nothing.</li>
 * </ol>
 *
 * <p>Kafka is deliberately not started: nothing measured here leaves the write side, and an idle
 * broker on the same host is only noise. Report output is {@code System.out} — a tool's product, not
 * logging (AGENTS.md §Logging 6).
 *
 * <p>{@code ./gradlew :tandem-benchmark:managedSeqCostProbe} — requires Docker.
 */
public final class ManagedSeqCostProbe {

    private static final String AGGREGATE_TYPE = "SeqProbe";
    private static final String CACHED_SEQUENCE = "probe_seq_cache32";
    private static final int SIGNIFICANT_DIGITS = 3;
    private static final long HIGHEST_TRACKABLE_NANOS = Duration.ofSeconds(60).toNanos();
    private static final Duration SAMPLE_INTERVAL = Duration.ofMillis(20);

    private ManagedSeqCostProbe() {
    }

    /** The three ways a caller states {@code seq} (HLD-managed-seq §4.6) — the arms contrasted here. */
    private enum Arm {
        APPLICATION("app", "seq(version)"),
        MANAGED("mgd", "managedSeq()"),
        UNSEQUENCED("non", "unsequenced()");

        private final String namespace;
        private final String label;

        Arm(String namespace, String label) {
            this.namespace = namespace;
            this.label = label;
        }
    }

    /** One arm's measurement window: what it achieved, how it was distributed, and what it waited on. */
    private record ArmResult(Arm arm, int writers, int round, double perSecond, Histogram latency, long failures,
                             long missedSubmissions, Map<String, Long> waits, String rows) {
    }

    private record Settings(int concurrency, List<Integer> concurrencySweep, List<Integer> sequenceSweep, int rounds,
                            Duration warmup, Duration duration, int aggregates, int payloadBytes, int bucketCount,
                            int seriesLength, int batchSize, List<Integer> phases) {

        static Settings parse(String[] args) {
            Map<String, String> kv = new LinkedHashMap<>();
            for (String arg : args) {
                int eq = arg.indexOf('=');
                kv.put(eq < 0 ? arg : arg.substring(0, eq), eq < 0 ? "" : arg.substring(eq + 1));
            }
            return new Settings(
                    Integer.parseInt(kv.getOrDefault("--concurrency", "32")),
                    ints(kv.getOrDefault("--concurrency-sweep", "8,32,64")),
                    ints(kv.getOrDefault("--sequence-sweep", "8,32,64,96")),
                    Integer.parseInt(kv.getOrDefault("--rounds", "2")),
                    Duration.ofSeconds(Long.parseLong(kv.getOrDefault("--warmup-seconds", "4"))),
                    Duration.ofSeconds(Long.parseLong(kv.getOrDefault("--duration-seconds", "15"))),
                    Integer.parseInt(kv.getOrDefault("--aggregates", "200")),
                    Integer.parseInt(kv.getOrDefault("--payload-bytes", "256")),
                    Integer.parseInt(kv.getOrDefault("--buckets", "64")),
                    Integer.parseInt(kv.getOrDefault("--series-length", "10000")),
                    Integer.parseInt(kv.getOrDefault("--batch-size", "50")),
                    ints(kv.getOrDefault("--phases", "1,2,3,4,5,6,7,8")));
        }

        private static List<Integer> ints(String csv) {
            return List.of(csv.split(",")).stream().map(String::trim).map(Integer::parseInt).toList();
        }
    }

    public static void main(String[] args) throws Exception {
        Locale.setDefault(Locale.ROOT);   // the report is read next to numbers from other hosts
        Settings settings = Settings.parse(args);
        try (PostgreSQLContainer<?> postgres =
                     new PostgreSQLContainer<>(DockerImageName.parse("postgres:16-alpine"))
                             // Testcontainers' default command is `postgres -c fsync=off`, and a
                             // command-line setting outranks postgresql.auto.conf — so ALTER SYSTEM
                             // could never switch fsync back on. Start it with no setting of its own
                             // and let this probe own the durability knob.
                             .withCommand("postgres")) {
            postgres.start();
            prepareSchema(postgres);
            printHeader(postgres, settings);

            List<ArmResult> durable = List.of();
            if (settings.phases().contains(1)) {
                fsync(postgres, "off");
                for (int writers : settings.concurrencySweep()) {
                    List<ArmResult> results = saturationPhase(postgres, settings, writers);
                    report("PHASE 1 — saturation, fsync=off (commit cost removed: the setting most likely to expose"
                            + " a per-insert cost), " + writers + " writers", results, settings);
                }
            }
            if (settings.phases().contains(2)) {
                fsync(postgres, "on");
                durable = saturationPhase(postgres, settings, settings.concurrency());
                report("PHASE 2 — saturation, fsync=on (a durable deployment), " + settings.concurrency() + " writers",
                        durable, settings);
            }
            if (settings.phases().contains(3)) {
                fsync(postgres, "on");
                double rate = pacedRate(durable, postgres, settings);
                List<ArmResult> paced = pacedPhase(postgres, settings, rate);
                report(String.format("PHASE 3 — paced at %,.0f writes/s offered, fsync=on, up to %d concurrent",
                        rate, settings.concurrency()), paced, settings);
            }
            if (settings.phases().contains(4)) {
                sequenceCeilingPhase(postgres, settings);
            }
            if (settings.phases().contains(5)) {
                fsync(postgres, "off");
                for (int writers : settings.concurrencySweep()) {
                    report("PHASE 5 — interleaved (every writer alternates the three modes op by op), saturated,"
                            + " fsync=off, " + writers + " writers",
                            interleavedPhase(postgres, settings, writers, 0), settings);
                }
            }
            if (settings.phases().contains(6)) {
                fsync(postgres, "on");
                report("PHASE 6 — interleaved, saturated, fsync=on (a durable deployment), "
                        + settings.concurrency() + " writers",
                        interleavedPhase(postgres, settings, settings.concurrency(), 0), settings);
            }
            if (settings.phases().contains(8)) {
                fsync(postgres, "on");
                report("PHASE 8 — interleaved batches of " + settings.batchSize()
                        + " rows per transaction (insertAll), saturated, fsync=on, "
                        + settings.concurrency() + " writers",
                        interleavedPhase(postgres, settings, settings.concurrency(), 0, settings.batchSize()),
                        settings);
            }
            if (settings.phases().contains(7)) {
                fsync(postgres, "on");
                // Half of what the saturated durable mix just sustained: below the ceiling, so the
                // latency being compared is service time rather than queueing.
                double rate = Math.max(1.0, interleavedPhase(postgres, settings, settings.concurrency(), 0).stream()
                        .mapToDouble(ArmResult::perSecond).sum() * 0.5);
                report(String.format("PHASE 7 — interleaved, paced at %,.0f writes/s offered, fsync=on", rate),
                        interleavedPhase(postgres, settings, settings.concurrency(), rate), settings);
            }
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Saturation: every writer flat out
    // ---------------------------------------------------------------------------------------------

    private static List<ArmResult> saturationPhase(PostgreSQLContainer<?> postgres, Settings s, int writers)
            throws Exception {
        List<ArmResult> results = new ArrayList<>();
        try (HikariDataSource dataSource = pool(postgres, writers + 4)) {
            for (int round = 1; round <= s.rounds(); round++) {
                // Rotated per round so no arm keeps the same slot: a warming JVM, a growing table and
                // host drift all favour a fixed position, and rotation cancels that.
                for (Arm arm : rotated(round)) {
                    results.add(saturationRun(dataSource, arm, s, writers, round));
                }
            }
        }
        return results;
    }

    private static ArmResult saturationRun(DataSource dataSource, Arm arm, Settings s, int writers, int round)
            throws Exception {
        truncate(dataSource);
        TransactionalUnitOfWork unitOfWork = new TransactionalUnitOfWork(dataSource);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(unitOfWork.transactionAware(), s.bucketCount());
        Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
        byte[] payload = payload(s.payloadBytes());

        Recorder recorder = new Recorder(HIGHEST_TRACKABLE_NANOS, SIGNIFICANT_DIGITS);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong failures = new AtomicLong();

        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < writers; i++) {
            Thread writer = new Thread(() -> {
                while (running.get()) {
                    writeOne(unitOfWork, repository, arm, s, payload, counters, recorder, failures);
                }
            }, "probe-" + arm.namespace + "-" + i);
            writer.setDaemon(true);
            writer.start();
            threads.add(writer);
        }

        park(s.warmup());
        recorder.getIntervalHistogram();   // discard the warm-up window
        WaitSampler sampler = WaitSampler.start(dataSource);
        long startedAt = System.nanoTime();
        park(s.duration());
        Histogram measured = recorder.getIntervalHistogram();
        double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
        Map<String, Long> waits = sampler.stop();

        running.set(false);
        for (Thread writer : threads) {
            writer.join(TimeUnit.SECONDS.toMillis(30));
        }
        return new ArmResult(arm, writers, round, measured.getTotalCount() / elapsedSeconds, measured,
                failures.get(), 0, waits, describeRows(dataSource));
    }

    // ---------------------------------------------------------------------------------------------
    // Paced: one offered rate, every arm
    // ---------------------------------------------------------------------------------------------

    /** Half the slowest arm's durable saturation throughput: below every arm's ceiling, so no arm queues. */
    private static double pacedRate(List<ArmResult> durable, PostgreSQLContainer<?> postgres, Settings s)
            throws Exception {
        if (!durable.isEmpty()) {
            return Math.max(1.0, durable.stream().mapToDouble(ArmResult::perSecond).min().orElseThrow() * 0.5);
        }
        try (HikariDataSource dataSource = pool(postgres, s.concurrency() + 4)) {
            return Math.max(1.0, saturationRun(dataSource, Arm.MANAGED, s, s.concurrency(), 0).perSecond() * 0.5);
        }
    }

    private static List<ArmResult> pacedPhase(PostgreSQLContainer<?> postgres, Settings s, double ratePerSecond)
            throws Exception {
        List<ArmResult> results = new ArrayList<>();
        try (HikariDataSource dataSource = pool(postgres, s.concurrency() + 4)) {
            for (int round = 1; round <= s.rounds(); round++) {
                for (Arm arm : rotated(round)) {
                    results.add(pacedRun(dataSource, arm, s, round, ratePerSecond));
                }
            }
        }
        return results;
    }

    private static ArmResult pacedRun(DataSource dataSource, Arm arm, Settings s, int round, double ratePerSecond)
            throws Exception {
        truncate(dataSource);
        TransactionalUnitOfWork unitOfWork = new TransactionalUnitOfWork(dataSource);
        JdbcOutboxRepository repository = new JdbcOutboxRepository(unitOfWork.transactionAware(), s.bucketCount());
        Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
        byte[] payload = payload(s.payloadBytes());

        Recorder recorder = new Recorder(HIGHEST_TRACKABLE_NANOS, SIGNIFICANT_DIGITS);
        AtomicBoolean running = new AtomicBoolean(true);
        AtomicLong failures = new AtomicLong();
        AtomicLong missed = new AtomicLong();
        Semaphore inFlight = new Semaphore(s.concurrency());

        try (ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor()) {
            Thread pacer = new Thread(() -> {
                long intervalNanos = Math.max(1, (long) (1_000_000_000.0 / ratePerSecond));
                long nextAt = System.nanoTime();
                while (running.get()) {
                    long now = System.nanoTime();
                    if (now < nextAt) {
                        LockSupport.parkNanos(nextAt - now);
                    }
                    if (inFlight.tryAcquire()) {
                        executor.submit(() -> {
                            try {
                                writeOne(unitOfWork, repository, arm, s, payload, counters, recorder, failures);
                            } finally {
                                inFlight.release();
                            }
                        });
                    } else {
                        missed.incrementAndGet();   // the arm cannot hold the offered rate
                    }
                    nextAt += intervalNanos;
                    if (nextAt < System.nanoTime() - intervalNanos) {
                        nextAt = System.nanoTime();   // fell behind; resync rather than burst
                    }
                }
            }, "probe-pacer-" + arm.namespace);
            pacer.setDaemon(true);
            pacer.start();

            park(s.warmup());
            recorder.getIntervalHistogram();
            missed.set(0);
            WaitSampler sampler = WaitSampler.start(dataSource);
            long startedAt = System.nanoTime();
            park(s.duration());
            Histogram measured = recorder.getIntervalHistogram();
            double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
            Map<String, Long> waits = sampler.stop();
            running.set(false);
            pacer.join(TimeUnit.SECONDS.toMillis(30));
            return new ArmResult(arm, s.concurrency(), round, measured.getTotalCount() / elapsedSeconds, measured,
                    failures.get(), missed.get(), waits, describeRows(dataSource));
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Interleaved: the three modes side by side under identical conditions
    // ---------------------------------------------------------------------------------------------

    /**
     * The sharpest form of the contrast, and the answer to the one weakness of the phases above: on a
     * developer machine the drift between two consecutive 15s windows is larger than the effect being
     * looked for, so a per-run A/B cannot resolve a small per-insert cost however many rounds it takes.
     * Here every writer thread rotates through the three modes operation by operation, into one
     * histogram per mode — so all three see the same host, the same table, the same instant, and their
     * latency distributions can be compared directly. Rate is equal by construction and carries no
     * information here; the latency columns are the measurement.
     */
    private static List<ArmResult> interleavedPhase(PostgreSQLContainer<?> postgres, Settings s, int writers,
                                                    double ratePerSecond) throws Exception {
        return interleavedPhase(postgres, s, writers, ratePerSecond, 1);
    }

    private static List<ArmResult> interleavedPhase(PostgreSQLContainer<?> postgres, Settings s, int writers,
                                                    double ratePerSecond, int batchSize) throws Exception {
        List<ArmResult> results = new ArrayList<>();
        try (HikariDataSource dataSource = pool(postgres, writers + 4)) {
            truncate(dataSource);
            TransactionalUnitOfWork unitOfWork = new TransactionalUnitOfWork(dataSource);
            JdbcOutboxRepository repository = new JdbcOutboxRepository(unitOfWork.transactionAware(), s.bucketCount());
            Map<String, AtomicLong> counters = new ConcurrentHashMap<>();
            byte[] payload = payload(s.payloadBytes());

            Map<Arm, Recorder> recorders = new EnumMap<>(Arm.class);
            Map<Arm, AtomicLong> failures = new EnumMap<>(Arm.class);
            for (Arm arm : Arm.values()) {
                recorders.put(arm, new Recorder(HIGHEST_TRACKABLE_NANOS, SIGNIFICANT_DIGITS));
                failures.put(arm, new AtomicLong());
            }
            AtomicBoolean running = new AtomicBoolean(true);
            AtomicLong missed = new AtomicLong();

            List<Thread> threads = new ArrayList<>();
            ExecutorService executor = ratePerSecond > 0 ? Executors.newVirtualThreadPerTaskExecutor() : null;
            if (executor != null) {
                Semaphore inFlight = new Semaphore(writers);
                AtomicLong op = new AtomicLong();
                Thread pacer = new Thread(() -> {
                    long intervalNanos = Math.max(1, (long) (1_000_000_000.0 / ratePerSecond));
                    long nextAt = System.nanoTime();
                    while (running.get()) {
                        long now = System.nanoTime();
                        if (now < nextAt) {
                            LockSupport.parkNanos(nextAt - now);
                        }
                        if (inFlight.tryAcquire()) {
                            Arm arm = Arm.values()[Math.floorMod(op.getAndIncrement(), Arm.values().length)];
                            executor.submit(() -> {
                                try {
                                    writeOne(unitOfWork, repository, arm, s, payload, counters,
                                            recorders.get(arm), failures.get(arm), batchSize);
                                } finally {
                                    inFlight.release();
                                }
                            });
                        } else {
                            missed.incrementAndGet();
                        }
                        nextAt += intervalNanos;
                        if (nextAt < System.nanoTime() - intervalNanos) {
                            nextAt = System.nanoTime();
                        }
                    }
                }, "probe-mixed-pacer");
                pacer.setDaemon(true);
                pacer.start();
                threads.add(pacer);
            } else {
                for (int i = 0; i < writers; i++) {
                    int offset = i;   // staggered, so threads do not march through the modes in lockstep
                    Thread writer = new Thread(() -> {
                        int op = offset;
                        while (running.get()) {
                            Arm arm = Arm.values()[Math.floorMod(op++, Arm.values().length)];
                            writeOne(unitOfWork, repository, arm, s, payload, counters,
                                    recorders.get(arm), failures.get(arm), batchSize);
                        }
                    }, "probe-mixed-" + i);
                    writer.setDaemon(true);
                    writer.start();
                    threads.add(writer);
                }
            }

            park(s.warmup());
            recorders.values().forEach(Recorder::getIntervalHistogram);   // discard the warm-up window
            missed.set(0);
            WaitSampler sampler = WaitSampler.start(dataSource);
            long startedAt = System.nanoTime();
            park(s.duration().multipliedBy(s.rounds()));   // one long window instead of repeated rounds
            Map<Arm, Histogram> measured = new EnumMap<>(Arm.class);
            for (Arm arm : Arm.values()) {
                measured.put(arm, recorders.get(arm).getIntervalHistogram());
            }
            double elapsedSeconds = (System.nanoTime() - startedAt) / 1_000_000_000.0;
            Map<String, Long> waits = sampler.stop();
            running.set(false);
            for (Thread writer : threads) {
                writer.join(TimeUnit.SECONDS.toMillis(30));
            }
            if (executor != null) {
                executor.close();
            }
            String rows = describeRows(dataSource);
            for (Arm arm : Arm.values()) {
                results.add(new ArmResult(arm, writers, 1, measured.get(arm).getTotalCount() / elapsedSeconds,
                        measured.get(arm), failures.get(arm).get(), missed.get() / Arm.values().length,
                        waits, rows));
            }
        }
        return results;
    }

    // ---------------------------------------------------------------------------------------------
    // The sequence on its own
    // ---------------------------------------------------------------------------------------------

    private static void sequenceCeilingPhase(PostgreSQLContainer<?> postgres, Settings s) throws Exception {
        System.out.println();
        System.out.println("PHASE 4 — nextval driven server-side over generate_series (no client round trip per value)");
        System.out.printf("  %,d values per statement, %ds windows%n", s.seriesLength(), s.duration().toSeconds());
        Map<String, String> arms = new LinkedHashMap<>();
        arms.put("loop only, no nextval", "SELECT count(g) FROM generate_series(1, ?) AS g");
        arms.put("tandem_seq, CACHE 1", "SELECT count(nextval('tandem_seq')) FROM generate_series(1, ?)");
        arms.put("a CACHE 32 sequence",
                "SELECT count(nextval('" + CACHED_SEQUENCE + "')) FROM generate_series(1, ?)");

        System.out.printf("  %-24s %s%n", "sessions",
                s.sequenceSweep().stream().map(c -> String.format("%14d", c)).reduce("", String::concat));
        for (Map.Entry<String, String> arm : arms.entrySet()) {
            StringBuilder row = new StringBuilder(String.format("  %-24s", arm.getKey()));
            for (int sessions : s.sequenceSweep()) {
                try (HikariDataSource dataSource = pool(postgres, sessions + 2)) {
                    row.append(String.format("%14s", String.format("%,.0f/s",
                            callsPerSecond(dataSource, arm.getValue(), s, sessions))));
                }
            }
            System.out.println(row);
        }
    }

    private static double callsPerSecond(DataSource dataSource, String sql, Settings s, int sessions) throws Exception {
        AtomicLong calls = new AtomicLong();
        AtomicBoolean running = new AtomicBoolean(true);
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < sessions; i++) {
            Thread thread = new Thread(() -> {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(sql)) {
                    ps.setInt(1, s.seriesLength());
                    while (running.get()) {
                        try (ResultSet rs = ps.executeQuery()) {
                            rs.next();
                        }
                        calls.addAndGet(s.seriesLength());
                    }
                } catch (SQLException e) {
                    throw new IllegalStateException("sequence ceiling probe failed", e);
                }
            }, "probe-seq-" + i);
            thread.setDaemon(true);
            thread.start();
            threads.add(thread);
        }
        park(Duration.ofSeconds(3));   // warm up
        calls.set(0);
        long startedAt = System.nanoTime();
        park(s.duration());
        double perSecond = calls.get() / ((System.nanoTime() - startedAt) / 1_000_000_000.0);
        running.set(false);
        for (Thread thread : threads) {
            thread.join(TimeUnit.SECONDS.toMillis(30));
        }
        return perSecond;
    }

    // ---------------------------------------------------------------------------------------------
    // The write itself
    // ---------------------------------------------------------------------------------------------

    private static void writeOne(TransactionalUnitOfWork unitOfWork, JdbcOutboxRepository repository, Arm arm,
                                 Settings s, byte[] payload, Map<String, AtomicLong> counters,
                                 Recorder recorder, AtomicLong failures) {
        writeOne(unitOfWork, repository, arm, s, payload, counters, recorder, failures, 1);
    }

    /**
     * {@code batchSize > 1} takes the {@code insertAll} path the Spring collector flushes a transaction
     * through, which is where a per-row cost weighs most: batching drives the per-row cost of the INSERT
     * itself down while a {@code nextval} per row stays exactly what it was.
     */
    private static void writeOne(TransactionalUnitOfWork unitOfWork, JdbcOutboxRepository repository, Arm arm,
                                 Settings s, byte[] payload, Map<String, AtomicLong> counters,
                                 Recorder recorder, AtomicLong failures, int batchSize) {
        String aggregateId = arm.namespace + "-" + ThreadLocalRandom.current().nextInt(s.aggregates());
        long startedAt = System.nanoTime();
        try {
            unitOfWork.runInTransaction(conn -> {
                if (batchSize == 1) {
                    repository.insert(message(arm, aggregateId, payload, counters));
                } else {
                    List<OutboxMessage> batch = new ArrayList<>(batchSize);
                    for (int i = 0; i < batchSize; i++) {
                        batch.add(message(arm, aggregateId, payload, counters));
                    }
                    repository.insertAll(batch);
                }
                return null;
            });
            recorder.recordValue(Math.clamp(System.nanoTime() - startedAt, 1, HIGHEST_TRACKABLE_NANOS));
        } catch (RuntimeException | SQLException e) {
            failures.incrementAndGet();
        }
    }

    /**
     * The one difference between the arms. The application-assigned arm keeps its counter in memory
     * rather than reading a domain row: a {@code SELECT ... FOR UPDATE} would add a row lock to that
     * arm alone and drown the effect being measured. Aggregate ids are namespaced per arm so a managed
     * number (drawn from one global sequence) can never collide with an app-assigned one under
     * {@code UNIQUE (aggregate_id, seq)}.
     */
    private static OutboxMessage message(Arm arm, String aggregateId, byte[] payload, Map<String, AtomicLong> counters) {
        OutboxMessage.Builder builder = OutboxMessage.builder()
                .aggregateId(aggregateId)
                .aggregateType(AGGREGATE_TYPE)
                .type("probe.event")
                .payload(payload)
                .contentType("application/json");
        switch (arm) {
            case APPLICATION -> builder.seq(counters.computeIfAbsent(aggregateId, k -> new AtomicLong())
                    .incrementAndGet());
            case MANAGED -> builder.managedSeq();
            case UNSEQUENCED -> builder.unsequenced();
        }
        return builder.build();
    }

    // ---------------------------------------------------------------------------------------------
    // What the backends are waiting on while an arm runs
    // ---------------------------------------------------------------------------------------------

    /**
     * Samples {@code pg_stat_activity} while an arm runs. Direct evidence for or against the concern:
     * sequence-page contention surfaces as an {@code LWLock:BufferContent} wait on the sequence
     * buffer, so if the managed arm shows no more of those than the arms that never call
     * {@code nextval}, the shared page is not where its writers spend time.
     */
    private static final class WaitSampler {

        private static final String SQL =
                "SELECT coalesce(wait_event_type || ':' || wait_event, 'running (no wait)') AS w, count(*) AS n "
                        + "FROM pg_stat_activity "
                        + "WHERE state = 'active' AND backend_type = 'client backend' AND pid <> pg_backend_pid() "
                        + "GROUP BY 1";

        private final Map<String, Long> counts = new ConcurrentHashMap<>();
        private final AtomicBoolean running = new AtomicBoolean(true);
        private final Thread thread;

        private WaitSampler(DataSource dataSource) {
            this.thread = new Thread(() -> {
                try (Connection conn = dataSource.getConnection();
                     PreparedStatement ps = conn.prepareStatement(SQL)) {
                    while (running.get()) {
                        try (ResultSet rs = ps.executeQuery()) {
                            while (rs.next()) {
                                counts.merge(rs.getString("w"), rs.getLong("n"), Long::sum);
                            }
                        }
                        park(SAMPLE_INTERVAL);
                    }
                } catch (SQLException e) {
                    counts.put("sampler failed: " + e.getMessage(), 0L);
                }
            }, "probe-wait-sampler");
            this.thread.setDaemon(true);
        }

        static WaitSampler start(DataSource dataSource) {
            WaitSampler sampler = new WaitSampler(dataSource);
            sampler.thread.start();
            return sampler;
        }

        Map<String, Long> stop() throws InterruptedException {
            running.set(false);
            thread.join(TimeUnit.SECONDS.toMillis(10));
            return Map.copyOf(counts);
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Reporting
    // ---------------------------------------------------------------------------------------------

    private static void report(String title, List<ArmResult> results, Settings s) {
        System.out.println();
        System.out.println(title);
        System.out.printf("  %d aggregates, %d-byte payload, %ds windows, %d rounds%n",
                s.aggregates(), s.payloadBytes(), s.duration().toSeconds(), s.rounds());
        System.out.printf("  %-14s %-6s %11s %9s %9s %9s %9s %9s %9s %7s%n",
                "arm", "round", "writes/s", "mean", "p50", "p95", "p99", "p99.9", "max", "failed");
        for (ArmResult r : results) {
            System.out.printf("  %-14s %-6d %11s %9s %9s %9s %9s %9s %9s %7d%n",
                    r.arm().label, r.round(), String.format("%,.0f", r.perSecond()),
                    micros(r.latency().getMean()), percentile(r.latency(), 50), percentile(r.latency(), 95),
                    percentile(r.latency(), 99), percentile(r.latency(), 99.9),
                    micros(r.latency().getMaxValue()), r.failures());
        }
        System.out.println("  --- median across rounds ---");
        for (Arm arm : Arm.values()) {
            List<ArmResult> armResults = results.stream().filter(r -> r.arm() == arm).toList();
            if (armResults.isEmpty()) {
                continue;
            }
            long missed = armResults.stream().mapToLong(ArmResult::missedSubmissions).sum();
            System.out.printf("  %-14s %-6s %11s %9s %9s %9s %9s%s%n", arm.label, "med",
                    String.format("%,.0f", medianOf(armResults, r -> r.perSecond())),
                    micros(medianOf(armResults, r -> r.latency().getMean())),
                    micros(medianOf(armResults, r -> r.latency().getValueAtPercentile(50))),
                    micros(medianOf(armResults, r -> r.latency().getValueAtPercentile(95))),
                    micros(medianOf(armResults, r -> r.latency().getValueAtPercentile(99))),
                    missed > 0 ? "   offered-rate misses:" + missed : "");
        }
        Map<String, String> rows = new LinkedHashMap<>();
        results.forEach(r -> rows.putIfAbsent(r.rows(), r.arm().label));
        System.out.println("  --- what was written (last round, 0=APPLICATION 1=MANAGED 2=NONE) ---");
        rows.forEach((row, arm) -> System.out.printf("  %-14s %s%n", rows.size() == 1 ? "mixed" : arm, row));
        Map<String, Long> waits = new LinkedHashMap<>();
        for (ArmResult r : results) {
            r.waits().forEach((k, v) -> waits.merge(String.format("%-14s %s", r.arm().label, k), v, Long::sum));
        }
        System.out.println("  --- pg_stat_activity samples of active client backends, summed over rounds ---");
        waits.entrySet().stream()
                .sorted(Map.Entry.<String, Long>comparingByValue().reversed())
                .limit(15)
                .forEach(e -> System.out.printf("  %-50s %7d%n", e.getKey(), e.getValue()));
    }

    private static double medianOf(List<ArmResult> results, java.util.function.ToDoubleFunction<ArmResult> value) {
        double[] sorted = results.stream().mapToDouble(value).sorted().toArray();
        if (sorted.length == 0) {
            return Double.NaN;
        }
        return sorted.length % 2 == 1
                ? sorted[sorted.length / 2]
                : (sorted[sorted.length / 2 - 1] + sorted[sorted.length / 2]) / 2.0;
    }

    private static String percentile(Histogram histogram, double percentile) {
        return micros(histogram.getValueAtPercentile(percentile));
    }

    /** Latencies read next to each other, so one unit throughout — microseconds, from nanos. */
    private static String micros(double nanos) {
        return String.format("%,.0fus", nanos / 1000.0);
    }

    private static void printHeader(PostgreSQLContainer<?> postgres, Settings s) throws SQLException {
        System.out.println("=".repeat(118));
        System.out.println("Managed seq cost probe — what HLD-managed-seq §4.1's mechanism costs the write path");
        System.out.println("=".repeat(118));
        try (HikariDataSource dataSource = pool(postgres, 2);
             Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            for (String setting : List.of("server_version", "shared_buffers", "wal_level", "wal_buffers",
                    "synchronous_commit", "max_connections")) {
                try (ResultSet rs = stmt.executeQuery("SHOW " + setting)) {
                    rs.next();
                    System.out.printf("  %-20s %s%n", setting, rs.getString(1));
                }
            }
        }
        System.out.printf("  %-20s %s, %d cores visible to the JVM%n", "host",
                System.getProperty("os.name"), Runtime.getRuntime().availableProcessors());
        System.out.printf("  %-20s %s%n", "settings", s);
    }

    /** Proves each arm wrote what it claims to write, rather than trusting the builder call. */
    private static String describeRows(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement();
             ResultSet rs = stmt.executeQuery(
                     "SELECT seq_source, count(*) AS n, count(seq) AS with_seq, min(seq) AS lo, max(seq) AS hi "
                             + "FROM tandem_outbox GROUP BY seq_source ORDER BY seq_source")) {
            StringBuilder summary = new StringBuilder();
            while (rs.next()) {
                if (!summary.isEmpty()) {
                    summary.append("  |  ");
                }
                summary.append(String.format("seq_source:%d rows:%,d withSeq:%,d minSeq:%s maxSeq:%s",
                        rs.getInt("seq_source"), rs.getLong("n"), rs.getLong("with_seq"),
                        rs.getString("lo"), rs.getString("hi")));
            }
            return summary.toString();
        }
    }

    // ---------------------------------------------------------------------------------------------
    // Environment
    // ---------------------------------------------------------------------------------------------

    private static void prepareSchema(PostgreSQLContainer<?> postgres) throws SQLException {
        try (HikariDataSource dataSource = pool(postgres, 2);
             Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute(readBaseline());
            // The mitigation the wire contract forbids (HLD-managed-seq §7), created only so the
            // ceiling phase can price what raising CACHE would actually buy.
            stmt.execute("CREATE SEQUENCE " + CACHED_SEQUENCE + " AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 32");
        }
    }

    /**
     * Testcontainers starts PostgreSQL with {@code fsync=off}, which is the sensitive setting for this
     * measurement but not what a deployment runs. {@code fsync} is {@code sighup}-scoped, so a reload
     * switches it without restarting the container or losing the schema.
     */
    private static void fsync(PostgreSQLContainer<?> postgres, String value) throws SQLException {
        try (HikariDataSource dataSource = pool(postgres, 2);
             Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("ALTER SYSTEM SET fsync = " + value);
            stmt.execute("SELECT pg_reload_conf()");
            park(Duration.ofSeconds(1));
            try (ResultSet rs = stmt.executeQuery("SHOW fsync")) {
                rs.next();
                if (!value.equals(rs.getString(1))) {
                    throw new IllegalStateException("fsync did not take effect, wanted:" + value
                            + " got:" + rs.getString(1));
                }
            }
        }
    }

    private static HikariDataSource pool(PostgreSQLContainer<?> postgres, int size) {
        HikariConfig config = new HikariConfig();
        config.setJdbcUrl(postgres.getJdbcUrl());
        config.setUsername(postgres.getUsername());
        config.setPassword(postgres.getPassword());
        config.setMaximumPoolSize(size);
        config.setMinimumIdle(size);
        config.setAutoCommit(true);
        config.setPoolName("probe-" + size);
        return new HikariDataSource(config);
    }

    private static void truncate(DataSource dataSource) throws SQLException {
        try (Connection conn = dataSource.getConnection();
             Statement stmt = conn.createStatement()) {
            stmt.execute("TRUNCATE tandem_outbox");
        }
    }

    private static byte[] payload(int payloadBytes) {
        StringBuilder json = new StringBuilder(payloadBytes + 16);
        json.append("{\"pad\":\"");
        while (json.length() < payloadBytes - 2) {
            json.append('x');
        }
        json.append("\"}");
        return json.toString().getBytes(StandardCharsets.UTF_8);
    }

    private static List<Arm> rotated(int round) {
        List<Arm> arms = new ArrayList<>(List.of(Arm.values()));
        Collections.rotate(arms, round);
        return arms;
    }

    private static void park(Duration duration) {
        long deadline = System.nanoTime() + duration.toNanos();
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            LockSupport.parkNanos(remaining);
        }
    }

    /** Same walk-up lookup {@code TandemTestContainer} uses — the committed DDL, not a copy of it. */
    private static String readBaseline() {
        Path dir = Path.of("").toAbsolutePath();
        while (dir != null) {
            Path candidate = dir.resolve("schema/postgres/tandem-baseline.sql");
            if (Files.exists(candidate)) {
                try {
                    return Files.readString(candidate, StandardCharsets.UTF_8);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
            dir = dir.getParent();
        }
        throw new IllegalStateException("could not locate schema/postgres/tandem-baseline.sql");
    }
}
