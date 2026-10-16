package com.codingful.tandem.jdbc;

import com.codingful.tandem.core.port.OutboxDispatcher;
import com.codingful.tandem.core.port.OutboxStore;
import com.codingful.tandem.core.port.TandemMetrics;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.HashSet;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.OptionalLong;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLongArray;

/**
 * The relay engine (LLD-jdbc §3.1): a pool of supervised worker threads each owning a slice of the
 * instance's buckets and running the continuous claim-while-busy loop, plus periodic lease-reclaim
 * and cleanup jobs. Depends only on the {@code OutboxStore} + {@code OutboxDispatcher} ports — never
 * on {@code tandem-kafka}.
 */
public final class WorkerPool {

    private static final Logger LOG = System.getLogger(WorkerPool.class.getName());

    private final OutboxStore store;
    private final OutboxDispatcher dispatcher;
    private final RelayConfig cfg;
    private final TandemMetrics metrics;
    private final Clock clock;
    private final BackoffStrategy backoff;
    private final BucketSource bucketSource;
    private final RelayControlSource controlSource;
    private final WakeupSource wakeupSource;
    private final String instanceId;

    private final int workerCount;
    private final RelayWorker[] workers;
    private final Thread[] threads;
    /**
     * Per-worker instant of the last completed claim cycle, for {@link #status()}. An
     * {@link AtomicLongArray} rather than a plain {@code long[]}: array <i>elements</i> are never
     * volatile, so a plain array would let a reader see an arbitrarily stale value — and a status
     * reading whose staleness is itself unbounded cannot answer the question it exists for.
     */
    private final AtomicLongArray lastCycleAtMillis;
    /** Where {@link #wakeupSource} signals land, and what a worker's idle wait ends on. */
    private final WorkerWakeups wakeups;
    /**
     * The buckets this instance owned as of the last heartbeat, and the filter every incoming signal
     * passes ({@link #isWakeable}). A snapshot rather than a live read because {@code LISTEN} is a
     * broadcast: under {@code LEASE} every instance is told about every bucket, so the filter runs once
     * per notification, and {@link BucketSource#ownedBuckets()} is a query (§3.2) — asking it that
     * often would cost more than the wakes it saves. Left empty, and never refreshed, when no
     * {@link WakeupSource} is wired: nothing reads it then.
     */
    private volatile Set<Integer> ownedBuckets = Set.of();
    private volatile boolean running;
    // Which start() the worker threads belong to. A stop() whose join times out (a worker blocked in I/O
    // that ignores the interrupt) leaves that thread alive; when it unblocks after a later start(), running
    // is true again, and only this tells it the run it was started for is over (§3.1).
    private volatile long run;
    private boolean stopping;   // guarded by this: a shutdown is transitioning; start() refuses meanwhile
    private ScheduledExecutorService scheduler;

    /**
     * Embedded topology with the basic-round defaults: no-op metrics, system clock, full-jitter backoff,
     * every bucket owned by this instance, no Admin-API pause, no wakeup. {@link #builder} overrides any
     * of them.
     *
     * @param store      relay-side persistence (poll/claim/update/cleanup)
     * @param dispatcher the publish port (e.g. {@code KafkaRelay})
     * @param cfg        relay engine configuration
     * @throws NullPointerException if any argument is {@code null}
     */
    public WorkerPool(OutboxStore store, OutboxDispatcher dispatcher, RelayConfig cfg) {
        this(builder(store, dispatcher, cfg));
    }

    /**
     * A pool with optional collaborators the three-argument constructor leaves at their defaults.
     *
     * @param store      relay-side persistence (poll/claim/update/cleanup)
     * @param dispatcher the publish port (e.g. {@code KafkaRelay})
     * @param cfg        relay engine configuration
     * @return a builder holding the three-argument constructor's defaults
     * @throws NullPointerException if any argument is {@code null}
     */
    public static Builder builder(OutboxStore store, OutboxDispatcher dispatcher, RelayConfig cfg) {
        return new Builder(store, dispatcher, cfg);
    }

    private WorkerPool(Builder builder) {
        this.store = builder.store;
        this.dispatcher = builder.dispatcher;
        this.cfg = builder.cfg;
        this.metrics = builder.metrics;
        this.clock = builder.clock;
        this.backoff = builder.backoff;
        this.bucketSource = builder.bucketSource;
        this.controlSource = builder.controlSource;
        this.wakeupSource = builder.wakeupSource;
        // Single instance identity, shared with the LEASE bucket owner (RelayConfig#instanceId), so a
        // worker/thread-name/log line correlates directly to tandem_bucket_lease.owner (LLD-jdbc §3.2).
        this.instanceId = cfg.instanceId();
        this.workerCount = cfg.workersPerInstance();
        this.workers = new RelayWorker[workerCount];
        this.threads = new Thread[workerCount];
        this.lastCycleAtMillis = new AtomicLongArray(workerCount);
        this.wakeups = new WorkerWakeups(workerCount);
    }

    /**
     * Validate config (fail-fast), then start the worker threads and the maintenance jobs. Idempotent
     * guard. All or nothing: a start that fails after its first thread is running stops what it started
     * before rethrowing, so the pool is left stopped and a later {@code start()} begins afresh.
     */
    public synchronized void start() {
        if (running) {
            return;
        }
        if (stopping) {
            // A shutdown is mid-join on another thread; silently interleaving a fresh start would race
            // it, so the call is refused — visibly, since the caller's relay will simply not be running.
            LOG.log(Level.WARNING, "start() ignored: a stop() is still in progress instanceId:" + instanceId);
            return;
        }
        // hard invariant rowLease > delivery.timeout.ms (§3.5), validated against the dispatcher's own
        // effective timeout when it reports one — so it cannot pass against a stale configured default.
        // The warning is for a value somebody set and the dispatcher overrides. The default differing
        // from what the dispatcher reports is the ordinary case (a producer setting, or just a client
        // whose own defaults moved), and a healthy start has nothing to warn about there.
        OptionalLong dispatcherTimeout = dispatcher.deliveryTimeoutMillis();
        if (dispatcherTimeout.isPresent() && cfg.deliveryTimeoutSet()
                && dispatcherTimeout.getAsLong() != cfg.deliveryTimeoutMs()) {
            LOG.log(Level.WARNING, "Validating rowLease against the dispatcher's reported delivery timeout,"
                    + " not the configured value dispatcherDeliveryTimeoutMs:" + dispatcherTimeout.getAsLong()
                    + ", configuredDeliveryTimeoutMs:" + cfg.deliveryTimeoutMs());
        }
        cfg.checkRowLeaseSafe(dispatcherTimeout.orElse(cfg.deliveryTimeoutMs()), metrics, LOG);
        // Checked here, before any thread starts: the scheduler rejects a non-positive period only when
        // the job is scheduled, by which time the workers would already be running.
        requirePositive(cfg.reclaimInterval(), "reclaimInterval");
        requirePositive(cfg.cleanupInterval(), "cleanupInterval");
        if (metrics.isEnabled()) {
            requirePositive(cfg.metricsInterval(), "metricsInterval");
        }
        bucketSource.validateOnStart(metrics, LOG);   // LEASE precondition: lease table seeded (§3.2)
        controlSource.onStart();   // publishes this instance's coordination mode (HLD-admin-api §4.1)
        controlSource.refresh();   // known pause state from the first cycle, not just after the first tick
        run++;
        running = true;
        try {
            startThreadsAndJobs();
        } catch (Throwable failure) {
            LOG.log(Level.ERROR, "Relay startup failed; stopping what it started instanceId:" + instanceId, failure);
            stop();
            throw failure;
        }
    }

    private static void requirePositive(Duration interval, String name) {
        if (interval.isNegative() || interval.isZero()) {
            throw new IllegalArgumentException(name + " must be positive");
        }
    }

    private void startThreadsAndJobs() {
        LOG.log(Level.INFO, "Starting relay instanceId:" + instanceId + ", workers:" + workerCount
                + ", coordination:" + cfg.coordination() + ", bucketCount:" + cfg.bucketCount()
                + ", batchSize:" + cfg.batchSize() + ", pollIntervalMs:" + cfg.pollInterval().toMillis()
                + ", pollIntervalFloorMs:" + cfg.pollIntervalFloor().toMillis()
                + ", pollBackoffFactor:" + cfg.pollBackoffFactor()
                + ", wakeup:" + wakeupName()
                + ", maxAttempts:" + cfg.maxAttempts() + ", rowLeaseMs:" + cfg.rowLease().toMillis());
        for (int i = 0; i < workerCount; i++) {
            int index = i;
            String workerId = "tandem-relay-" + instanceId + "-w" + index;
            workers[i] = new RelayWorker(store, dispatcher, cfg, backoff, metrics, clock, workerId,
                    () -> sliceFor(index));
            startWorkerThread(index, run);
        }
        startWakeups();
        scheduler = Executors.newSingleThreadScheduledExecutor(r -> daemon(r, "tandem-relay-" + instanceId + "-jobs"));
        long reclaimMs = cfg.reclaimInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::reclaimTick, reclaimMs, reclaimMs, TimeUnit.MILLISECONDS);
        long cleanupMs = cfg.cleanupInterval().toMillis();
        scheduler.scheduleWithFixedDelay(this::cleanupTick, cleanupMs, cleanupMs, TimeUnit.MILLISECONDS);
        // Initial delay 0, unlike reclaimTick/controlTick: under LEASE this is the first bucket claim, and
        // a relay that waited a full reclaimInterval to own any bucket would sit idle for no reason — same
        // rationale as metricsTick's immediate first read below.
        scheduler.scheduleWithFixedDelay(this::heartbeatTick, 0, reclaimMs, TimeUnit.MILLISECONDS);
        scheduler.scheduleWithFixedDelay(this::controlTick, reclaimMs, reclaimMs, TimeUnit.MILLISECONDS);
        if (metrics.isEnabled()) {
            // Scheduled only when an adapter is wired: with the no-op default the lag query never runs,
            // so an outbox nobody is watching pays nothing for the gauges (§4). The first reading is
            // taken immediately rather than one interval in: a relay that just started may be inheriting
            // a backlog from the instance it replaced, and that is the moment worth seeing.
            long metricsMs = cfg.metricsInterval().toMillis();
            scheduler.scheduleWithFixedDelay(this::metricsTick, 0, metricsMs, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * This worker's slice of the instance's currently-owned, not-paused buckets:
     * {@code bucket % workerCount == index}. Ownership is queried fresh every call (as it always was);
     * pause is a cached, in-memory read (HLD-admin-api §4.1) — never a query on this hot path.
     */
    private Set<Integer> sliceFor(int index) {
        if (controlSource.wholeRelayPaused()) {
            return Set.of();
        }
        Set<Integer> owned = bucketSource.ownedBuckets();
        Set<Integer> mine = new HashSet<>();
        for (int bucket : owned) {
            if (Math.floorMod(bucket, workerCount) == index && !controlSource.bucketPaused(bucket)) {
                mine.add(bucket);
            }
        }
        return mine;
    }

    /** Whether a worker thread started for {@code workerRun} should keep working. */
    private boolean isCurrent(long workerRun) {
        return running && run == workerRun;
    }

    private void startWorkerThread(int index, long workerRun) {
        // Starting counts as this worker's first cycle, so a worker that hangs on its very first claim
        // shows an ageing timestamp rather than none at all — and a restarted one does not inherit the
        // dead thread's staleness (§3.8).
        lastCycleAtMillis.set(index, clock.millis());
        Thread t = new Thread(() -> runWorker(index, workerRun), "tandem-relay-" + instanceId + "-w" + index);
        t.setDaemon(true);
        // Supervised: an Error that escapes the loop kills the thread; restart it so its buckets are
        // not abandoned (critical for the embedded topology, which has no lease table to self-heal, §3.1).
        // The restart takes this monitor and re-checks running, so every threads[] write/read is under
        // the lock (visibility) and a restart cannot slip in after shutdown began. beginHalt() holds the
        // lock only for the brief transition, not the join, so this handler never stalls behind a join;
        // if it does restart just as shutdown starts, that worker is a daemon that exits on running=false.
        // A worker of an earlier run is not restarted: its slot now belongs to the current run's thread.
        t.setUncaughtExceptionHandler((thread, error) -> {
            LOG.log(Level.ERROR, "Relay worker died; restarting workerIndex:" + index, error);
            synchronized (this) {
                if (isCurrent(workerRun)) {
                    startWorkerThread(index, workerRun);
                }
            }
        });
        threads[index] = t;
        t.start();
    }

    private void runWorker(int index, long workerRun) {
        RelayWorker worker = workers[index];
        // A local, so the failure counter is this thread's by construction — never shared, never
        // contended, and reset with the thread when a supervised restart replaces it (§3.1).
        PollBackoff backoff = new PollBackoff(cfg.pollIntervalFloor(), cfg.pollInterval(),
                cfg.pollBackoffFactor(), cfg.reclaimInterval());
        while (isCurrent(workerRun)) {
            try {
                int claimed = worker.claimAndDispatch();
                worker.flushDone();
                worker.flushFailures();
                // Progress, deliberately marked only on a cycle that completed without throwing: a
                // worker whose every iteration fails is not making progress, and status() must say so
                // (§3.8). Claiming nothing is still progress — an idle relay is a working relay.
                if (!isCurrent(workerRun)) {
                    break;   // before marking progress: a worker of an earlier run must not freshen the current one's
                }
                lastCycleAtMillis.set(index, clock.millis());
                if (claimed > 0 && LOG.isLoggable(Level.DEBUG)) {
                    LOG.log(Level.DEBUG, "Worker claimed rows workerIndex:" + index + ", claimed:" + claimed);
                }
                // The loop reports what the cycle did and is told how long to wait; which of the three
                // waits that is, and how the idle one grows, belongs to PollBackoff (§3.1).
                long wait = backoff.waitAfterCycle(claimed, worker.inFlight());
                if (wait > 0) {
                    // A signal ends this wait early; it does not reset the ramp. The claim that follows
                    // does, but only if it finds something (waitAfterCycle above), which is the whole
                    // difference: a wake that claims nothing leaves the worker where it was instead of
                    // pinning it at the floor, so a burst of signals for rows this worker has already
                    // taken cannot turn its backoff off. With no WakeupSource wired nothing ever
                    // signals, and this is the plain sleep it has always been.
                    awaitWakeup(index, wait);
                }
            } catch (Exception perIteration) {
                LOG.log(Level.ERROR, "Relay worker iteration failed workerIndex:" + index
                        + ", instanceId:" + instanceId, perIteration);
                // Growing wait, not a fixed pollInterval: a database that is down would otherwise have
                // every worker re-querying and logging a stack trace ten times a second (§3.1).
                sleep(backoff.waitAfterFailedCycle());
            }
        }
        worker.flushDone();       // best-effort drain of any acked ids on shutdown
        worker.flushFailures();   // and of any captured failures, so no outcome is lost to the lease
    }

    private void reclaimTick() {
        try {
            int reclaimed = store.reclaimExpiredLeases();
            if (reclaimed > 0) {
                if (metrics.isEnabled()) {
                    metrics.incrementLeaseExpired(reclaimed);
                }
                if (LOG.isLoggable(Level.DEBUG)) {
                    LOG.log(Level.DEBUG, "Reclaimed expired leases count:" + reclaimed);
                }
            }
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Lease reclaim failed", e);
        }
    }

    /**
     * One pass deletes everything past retention, chunk after chunk (§3.7): one chunk per interval would
     * cap the cleanup rate (1000 rows every 15 minutes by default, about one row a second), and any outbox
     * busier than that would grow without bound. Each chunk is its own short statement, so the pass still
     * holds no long lock, and it ends because the cutoff is fixed for the whole pass.
     */
    private void cleanupTick() {
        try {
            Instant doneBefore = clock.instant().minus(cfg.retention());
            int chunk = cfg.cleanupBatchSize();
            int deleted;
            do {
                deleted = store.cleanup(doneBefore, chunk);
            } while (running && deleted >= chunk);
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Cleanup failed", e);
        }
    }

    /**
     * Read the lag and failed-count gauges and report how many workers are actually alive and how long
     * the slowest of them has gone without progress (§4, §3.8). Failing here must never disturb
     * delivery — an unreadable gauge is a monitoring problem, not a relay problem — so, like the other
     * maintenance jobs, it swallows the failure after logging it.
     */
    private void metricsTick() {
        try {
            store.lag().ifPresent(lag -> {
                metrics.recordLag(lag.pending());
                metrics.recordLagAgeSeconds(lag.ageSecondsAt(clock.instant()));
            });
            store.failedCount().ifPresent(metrics::recordFailed);
            store.blockedCount().ifPresent(metrics::recordBlocked);
            bucketSource.uncoveredBuckets().ifPresent(metrics::recordUncoveredBuckets);
            // One status() call for both gauges below: it is already an in-process, database-free
            // snapshot (§3.8), so there is no cost split to weigh — only two readings of one moment
            // to keep consistent with each other, exactly like the single lag() query above.
            RelayStatus status = status();
            metrics.recordActiveWorkers(status.workersAlive());
            metrics.recordWorkerCycleAgeSeconds(status.oldestWorkerCycleAgeSecondsAt(clock.instant()));
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Reading relay metrics failed", e);
        }
    }

    /**
     * This instance's current in-process state (§3.8) — what an application's own readiness/liveness
     * probe reads. <b>Never queries the database</b>, by contract, so it is safe to call at probe
     * frequency; see {@link RelayStatus} for what that deliberately excludes and why Tandem draws no
     * health verdict of its own.
     *
     * <p>Alive, not configured: a worker whose thread died and has not been restarted yet must not
     * count, and its stale cycle timestamp must not drag {@code oldestWorkerCycle} down either.
     *
     * @return an immutable reading, taken under the same monitor the worker-thread bookkeeping uses so
     *         the counts and the timestamp describe one consistent moment
     */
    public synchronized RelayStatus status() {
        RelayStatus.State state = running
                ? RelayStatus.State.RUNNING
                : (stopping ? RelayStatus.State.STOPPING : RelayStatus.State.STOPPED);
        int alive = 0;
        long oldest = Long.MAX_VALUE;
        for (int i = 0; i < workerCount; i++) {
            Thread t = threads[i];
            if (t != null && t.isAlive()) {
                alive++;
                oldest = Math.min(oldest, lastCycleAtMillis.get(i));
            }
        }
        return new RelayStatus(instanceId, state, cfg.coordination(), workerCount, alive,
                alive == 0 ? Optional.empty() : Optional.of(Instant.ofEpochMilli(oldest)));
    }

    private void heartbeatTick() {
        try {
            bucketSource.heartbeat();
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Bucket heartbeat failed", e);
        }
        refreshOwnedBuckets();
    }

    /**
     * Re-reads {@link #ownedBuckets} on the same cadence ownership itself changes on. A failed read
     * <b>keeps the previous snapshot</b>: emptying it would silence every wakeup until the next tick,
     * which is a worse answer than acting on ownership that is a few seconds old — and stale ownership
     * costs nothing here, since a signal for a bucket this instance no longer owns wakes a worker that
     * claims nothing, and one for a bucket it has just acquired is simply dropped and found by the
     * next poll (§3.10).
     *
     * <p>With no source wired it does not run at all. The snapshot exists for the filter and nothing
     * else, so maintaining it there would be a lease query every {@code reclaimInterval} — and, on a
     * bad day, a warning naming a feature that is off — for a relay that can never consult it.
     */
    private void refreshOwnedBuckets() {
        if (wakeupSource == WakeupSource.NONE) {
            return;
        }
        try {
            ownedBuckets = Set.copyOf(bucketSource.ownedBuckets());
        } catch (Exception e) {
            LOG.log(Level.WARNING, "Refreshing the wakeup bucket filter failed; keeping the previous"
                    + " snapshot instanceId:" + instanceId, e);
        }
    }

    /**
     * Whether a signal for {@code bucket} is worth waking a worker for: this instance must own it and
     * it must not be paused. Under {@code SINGLE} every bucket is owned, so this only ever filters the
     * paused ones.
     */
    private boolean isWakeable(int bucket) {
        return ownedBuckets.contains(bucket)
                && !controlSource.wholeRelayPaused()
                && !controlSource.bucketPaused(bucket);
    }

    /**
     * Refreshes the cached Admin-API pause state and marks this instance alive for
     * {@code RelayStatus.state == DOWN} (HLD-admin-api §4.1). Never disturbs delivery on failure.
     */
    private void controlTick() {
        try {
            controlSource.refresh();
            controlSource.heartbeat();
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Relay control refresh failed", e);
        }
    }

    /** Graceful shutdown: stop polling, let workers finish their cycle, release buckets. In-flight rows are recovered by lease reclaim (§3.1/§3.5). */
    public void stop() {
        Thread[] toJoin = beginHalt();
        if (toJoin == null) {
            return;
        }
        stopWakeups();   // outside the lock too: it joins a thread of its own, and briefly blocks
        try {
            joinAll(toJoin);   // outside the lock, so a worker's uncaught-handler restart never stalls on it
            try {
                bucketSource.release();
            } catch (Exception e) {
                LOG.log(Level.ERROR, "Releasing buckets failed", e);
            }
            LOG.log(Level.INFO, "Relay stopped instanceId:" + instanceId);
        } finally {
            finishHalt();
        }
    }

    /**
     * Ungraceful stop: halts the worker threads and scheduler exactly like {@link #stop()}, but
     * deliberately <b>skips</b> {@code bucketSource.release()} — simulating an instance that crashed
     * rather than shut down cleanly, so under {@code LEASE} its bucket ownership and presence
     * ({@code tandem_relay_member}) are discovered stale only once their leases expire (§3.2), not
     * released immediately as a graceful {@link #stop()} would. Real crashes need no explicit call at
     * all (the process is simply gone); this exists so tests can exercise the lease-expiry self-heal
     * path against a live, running instance rather than driving {@code BucketSource} heartbeats by
     * hand (contrast {@code BucketLeaseManagerIT}, which does exactly that at a lower level). Under
     * {@code SINGLE}, where {@code release()} is a no-op, {@code kill()} and {@link #stop()} are
     * equivalent.
     */
    public void kill() {
        Thread[] toJoin = beginHalt();
        if (toJoin == null) {
            return;
        }
        stopWakeups();
        try {
            joinAll(toJoin);
        } finally {
            finishHalt();
        }
    }

    /**
     * Locked transition step: clear {@code running}, stop the scheduler and interrupt the workers,
     * and return a snapshot of the threads to join. Returns {@code null} if already stopped. The
     * <b>join happens outside</b> this critical section (see {@link #stop()}) so a worker whose
     * uncaught-exception handler is trying to restart — which also needs this monitor — never blocks
     * behind a 10s join. {@code stopping} keeps {@link #start()} from interleaving until the join is done.
     */
    private synchronized Thread[] beginHalt() {
        if (!running) {
            return null;
        }
        running = false;
        stopping = true;
        if (scheduler != null) {
            scheduler.shutdownNow();
        }
        Thread[] snapshot = threads.clone();
        for (Thread t : snapshot) {
            if (t != null) {
                t.interrupt();
            }
        }
        return snapshot;
    }

    private synchronized void finishHalt() {
        stopping = false;
    }

    private static void joinAll(Thread[] toJoin) {
        for (Thread t : toJoin) {
            if (t != null) {
                joinQuietly(t);
            }
        }
    }

    private static Thread daemon(Runnable r, String name) {
        Thread t = new Thread(r, name);
        t.setDaemon(true);
        return t;
    }

    /** What the startup line reports for the wakeup source: {@code none}, or the adapter's own name. */
    private String wakeupName() {
        if (wakeupSource == WakeupSource.NONE) {
            return "none";
        }
        String simple = wakeupSource.getClass().getSimpleName();
        return simple.isEmpty() ? wakeupSource.getClass().getName() : simple;   // an anonymous source has none
    }

    /**
     * Starts delivering wakeups into {@link #wakeups}, through the ownership filter. A signal names a
     * bucket, not an instance, and {@code LISTEN} delivers it to every listener, so under {@code LEASE}
     * roughly {@code (N-1)/N} of what arrives here is about buckets some other instance owns. Waking a
     * worker for those costs a claim that finds nothing — twice over, since the claim re-reads
     * ownership itself — and it is the largest part of what the wakeup adds to the database
     * (dispatch-latency §3.4).
     *
     * <p>A source that cannot start must not stop the
     * relay: discovery falls back to the poll interval, which bounds it in every configuration anyway
     * (dispatch-latency §2, C2).
     */
    private void startWakeups() {
        try {
            // Populated before the source can deliver anything: the ownership tick would fill it a
            // moment later anyway, and until then every signal would be dropped as unowned.
            refreshOwnedBuckets();
            wakeupSource.start(new WakeupSource.Listener() {
                @Override
                public void wake(int bucket) {
                    if (isWakeable(bucket)) {
                        wakeups.wake(bucket);
                    }
                }

                @Override
                public void wakeAll() {
                    // Deliberately unfiltered: a sweep means "something was missed and I cannot say
                    // which bucket", so every worker checks its own slice, which is where ownership is
                    // decided anyway.
                    wakeups.wakeAll();
                }
            });
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Starting the wakeup source failed; discovery is the poll loop only"
                    + " instanceId:" + instanceId, e);
        }
    }

    private void stopWakeups() {
        try {
            wakeupSource.stop();
        } catch (Exception e) {
            LOG.log(Level.ERROR, "Stopping the wakeup source failed instanceId:" + instanceId, e);
        }
    }

    /**
     * This worker's idle wait, ended early by a wakeup for one of its buckets. An interrupt ends it
     * too, which a shutdown does and which the loop's own {@code running} check handles next.
     */
    private void awaitWakeup(int index, long millis) {
        try {
            wakeups.await(index, millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void sleep(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    private static void joinQuietly(Thread t) {
        try {
            t.join(TimeUnit.SECONDS.toMillis(10));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    /** Builds a {@link WorkerPool} with optional collaborators (HLD-api-stability §4.3). */
    public static final class Builder {
        private final OutboxStore store;
        private final OutboxDispatcher dispatcher;
        private final RelayConfig cfg;
        private TandemMetrics metrics = TandemMetrics.NOOP;
        private Clock clock = Clock.systemUTC();
        private BackoffStrategy backoff = BackoffStrategy.fullJitter();
        private BucketSource bucketSource;
        private RelayControlSource controlSource = RelayControlSource.NOOP;
        private WakeupSource wakeupSource = WakeupSource.NONE;

        private Builder(OutboxStore store, OutboxDispatcher dispatcher, RelayConfig cfg) {
            this.store = Objects.requireNonNull(store, "store");
            this.dispatcher = Objects.requireNonNull(dispatcher, "dispatcher");
            this.cfg = Objects.requireNonNull(cfg, "cfg");
            this.bucketSource = BucketSource.embedded(cfg.bucketCount());
        }

        /** Metrics sink. Default {@link TandemMetrics#NOOP}. */
        public Builder metrics(TandemMetrics metrics) {
            this.metrics = Objects.requireNonNull(metrics, "metrics");
            return this;
        }

        /**
         * Used for cleanup's {@code doneBefore} cutoff, so a test can fix it. Default
         * {@link Clock#systemUTC()}.
         */
        public Builder clock(Clock clock) {
            this.clock = Objects.requireNonNull(clock, "clock");
            return this;
        }

        /** Retry delay for retriable dispatch failures. Default {@link BackoffStrategy#fullJitter()}. */
        public Builder backoff(BackoffStrategy backoff) {
            this.backoff = Objects.requireNonNull(backoff, "backoff");
            return this;
        }

        /**
         * Which virtual buckets this instance owns: {@link BucketSource#forCoordination} for several
         * instances (LLD-jdbc §3.2). Default {@link BucketSource#embedded}, every bucket.
         */
        public Builder bucketSource(BucketSource bucketSource) {
            this.bucketSource = Objects.requireNonNull(bucketSource, "bucketSource");
            return this;
        }

        /**
         * The Admin API's desired pause state, refreshed on {@code reclaimInterval} (HLD-admin-api §4.1).
         * Default {@link RelayControlSource#NOOP}: no pause support.
         */
        public Builder controlSource(RelayControlSource controlSource) {
            this.controlSource = Objects.requireNonNull(controlSource, "controlSource");
            return this;
        }

        /**
         * Where the write side's post-commit signals arrive, shortening discovery for a row written into
         * a quiet bucket (dispatch-latency §3.4). Default {@link WakeupSource#NONE}: the poll loop alone,
         * which bounds discovery in every configuration anyway.
         */
        public Builder wakeupSource(WakeupSource wakeupSource) {
            this.wakeupSource = Objects.requireNonNull(wakeupSource, "wakeupSource");
            return this;
        }

        public WorkerPool build() {
            return new WorkerPool(this);
        }
    }
}
