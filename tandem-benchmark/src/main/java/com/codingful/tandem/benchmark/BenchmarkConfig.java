package com.codingful.tandem.benchmark;

import java.time.Duration;
import java.util.Objects;

/**
 * The harness sizing knobs (LLD-benchmark §10). Immutable; built via {@link #builder()} or
 * {@link #defaults()}. Scenario-specific tuning (offered rate, ramp step, search budget) is decided
 * by each {@link com.codingful.tandem.benchmark.scenario.Scenario}, not baked in here.
 */
public final class BenchmarkConfig {

    /** How COMMIT→ack latency's {@code t0} is captured (HLD-load-testing.md §2.2, LLD-benchmark §5.1). */
    public enum LatencyMode {
        /** Insert-time {@code nanoTime} header; always available, over-estimates by the INSERT→COMMIT gap. */
        PROXY,
        /** Post-COMMIT timestamp via the in-process side-channel; removes the INSERT→COMMIT skew. */
        ACCURATE
    }

    private final int bucketCount;
    private final int workers;
    private final Duration pollInterval;
    private final int batchSize;
    private final Duration rowLease;
    private final long deliveryTimeoutMs;
    private final int maxAttempts;
    private final int maxConnections;
    private final int payloadBytes;
    private final int aggregateCardinality;
    private final Duration warmup;
    private final Duration duration;
    private final Duration window;
    private final double offeredRate;
    private final Duration retention;
    private final Duration cleanupInterval;
    private final int cleanupBatchSize;
    private final LatencyMode latencyMode;

    private BenchmarkConfig(Builder b) {
        this.bucketCount = b.bucketCount;
        this.workers = b.workers;
        this.pollInterval = b.pollInterval;
        this.batchSize = b.batchSize;
        this.rowLease = b.rowLease;
        this.deliveryTimeoutMs = b.deliveryTimeoutMs;
        this.maxAttempts = b.maxAttempts;
        this.maxConnections = b.maxConnections;
        this.payloadBytes = b.payloadBytes;
        this.aggregateCardinality = b.aggregateCardinality;
        this.warmup = b.warmup;
        this.duration = b.duration;
        this.window = b.window;
        this.offeredRate = b.offeredRate;
        this.retention = b.retention;
        this.cleanupInterval = b.cleanupInterval;
        this.cleanupBatchSize = b.cleanupBatchSize;
        this.latencyMode = b.latencyMode;
    }

    /** Must match the value baked into every row by the write-side (LLD-jdbc §2.1). Default 256. */
    public int bucketCount() {
        return bucketCount;
    }

    /** Relay {@code workersPerInstance}. Default 8. */
    public int workers() {
        return workers;
    }

    /**
     * Relay {@code pollInterval} — the <b>idle</b> backoff between claim attempts, not a per-batch
     * delay (LLD-jdbc §3.1), so it bounds discovery latency for a drained bucket and sets the idle
     * query load at {@code workers / pollInterval}. Default 100 ms, matching {@code RelayConfig}'s.
     */
    public Duration pollInterval() {
        return pollInterval;
    }

    /** Relay claim batch size — the per-shard in-flight window (LLD-jdbc §3.4). Default 100. */
    public int batchSize() {
        return batchSize;
    }

    /** Relay row lease. Must be {@code > deliveryTimeoutMs} (enforced at relay startup). Default 60s. */
    public Duration rowLease() {
        return rowLease;
    }

    /** The Kafka producer's {@code delivery.timeout.ms}, wired into the producer config (not just the row-lease check). Default 30000. */
    public long deliveryTimeoutMs() {
        return deliveryTimeoutMs;
    }

    /** Retriable failures allowed before a row is quarantined to {@code FAILED}. Default 10. */
    public int maxAttempts() {
        return maxAttempts;
    }

    /** The bounded connection-pool size — the true in-flight-transaction limit (LLD-benchmark §4.2). Default 32. */
    public int maxConnections() {
        return maxConnections;
    }

    /** The reference payload size (HLD-load-testing.md §5). Default 1024 (1 KB). */
    public int payloadBytes() {
        return payloadBytes;
    }

    /** Number of distinct synthetic aggregates the {@link AggregateSelector} spreads load over. Default 1024. */
    public int aggregateCardinality() {
        return aggregateCardinality;
    }

    /** Discarded before recording (HLD-load-testing.md §3). Default 30s. */
    public Duration warmup() {
        return warmup;
    }

    /** The steady-state measurement window (the S1 sustain gate, S2's hold duration, …). Default 10 min. */
    public Duration duration() {
        return duration;
    }

    /**
     * The reporting window a long run is sliced into (S9) — the unit a drift comparison is made
     * <b>between</b>, where {@link #duration()} is the whole run. Default 20 minutes; a scenario shrinks
     * it when the run is too short to hold two.
     */
    public Duration window() {
        return window;
    }

    /**
     * A fixed offered rate in events/s, or {@code 0} to let the scenario find one. Only an endurance
     * run reads it: every other scenario decides its own rate, by ramp or by design. Holding it fixed is
     * what makes two windows hours apart comparable at all — and the alternative, a ramp scaled to the
     * run, would spend hours at the ceiling, which is the one place a burstable or thermally-limited
     * host stops being a measuring instrument.
     */
    public double offeredRate() {
        return offeredRate;
    }

    /**
     * How long a {@code DONE} row is kept before the relay's cleanup deletes it — {@code RelayConfig}'s
     * own default of 14 days, which for any run shorter than that means <b>cleanup never deletes
     * anything</b> and the outbox only grows. That is the right default for a scenario measured in
     * minutes and the wrong shape for one measured in hours: a production outbox is a churn table, and
     * an endurance run that never deletes measures a table getting longer rather than the
     * {@code PENDING → IN_FLIGHT → DONE → deleted} cycle a real deployment lives in (and fills the
     * host's disk while doing it). Default 14 days.
     */
    public Duration retention() {
        return retention;
    }

    /** How often the relay's cleanup pass runs. Default 15 min, matching {@code RelayConfig}'s. */
    public Duration cleanupInterval() {
        return cleanupInterval;
    }

    /**
     * Rows deleted per cleanup pass. Sized against the write rate for a long run: at {@code r} events/s
     * a pass every {@code i} seconds must delete at least {@code r × i} rows, or the backlog of
     * deletable rows grows even though cleanup is running. Default 1000.
     */
    public int cleanupBatchSize() {
        return cleanupBatchSize;
    }

    /** Default {@link LatencyMode#PROXY}. */
    public LatencyMode latencyMode() {
        return latencyMode;
    }

    public static BenchmarkConfig defaults() {
        return builder().build();
    }

    public static Builder builder() {
        return new Builder();
    }

    /**
     * A builder pre-loaded with every field of this config, for the derived configs below and for the
     * runner's command-line overrides. Deliberately not a field-by-field copy at each call site: the
     * derivations are full copies with a few overrides, and enumerating the fields in each of them is
     * how a newly-added knob silently reverts to its default in three places at once.
     */
    public Builder toBuilder() {
        return builder()
                .bucketCount(bucketCount)
                .workers(workers)
                .pollInterval(pollInterval)
                .batchSize(batchSize)
                .rowLease(rowLease)
                .deliveryTimeoutMs(deliveryTimeoutMs)
                .maxAttempts(maxAttempts)
                .maxConnections(maxConnections)
                .payloadBytes(payloadBytes)
                .aggregateCardinality(aggregateCardinality)
                .warmup(warmup)
                .duration(duration)
                .window(window)
                .offeredRate(offeredRate)
                .retention(retention)
                .cleanupInterval(cleanupInterval)
                .cleanupBatchSize(cleanupBatchSize)
                .latencyMode(latencyMode);
    }

    /**
     * A copy of this config with a much smaller warmup/duration/rowLease — the CI smoke variant
     * (HLD-load-testing.md §7). {@code deliveryTimeoutMs} shrinks together with {@code rowLease} so
     * the relay-startup invariant {@code rowLease > deliveryTimeoutMs} still holds (LLD-jdbc §3.5) —
     * shrinking only rowLease would fail fast at {@code WorkerPool.start()}.
     */
    public BenchmarkConfig toSmoke() {
        return toBuilder()
                .workers(Math.min(workers, 2))
                .batchSize(Math.min(batchSize, 20))
                .deliveryTimeoutMs(4_000)
                .rowLease(Duration.ofSeconds(9))
                .maxConnections(Math.min(maxConnections, 8))
                .aggregateCardinality(Math.min(aggregateCardinality, 32))
                .warmup(Duration.ofSeconds(1))
                .duration(Duration.ofSeconds(3))
                .build();
    }

    /**
     * A copy of this config with real relay concurrency (default {@code workers}/{@code batchSize}/
     * {@code bucketCount}) but a much shorter {@code duration} than the full-run default — for
     * demonstrating the harness against real Docker containers without an hours-long run
     * (HLD-load-testing.md §5.1: not a KPI number on a developer machine either way, only faster to
     * look at). {@code deliveryTimeoutMs}/{@code rowLease} shrink together, same reason as {@link #toSmoke()}.
     */
    public BenchmarkConfig toDemo() {
        return toBuilder()
                .deliveryTimeoutMs(8_000)
                .rowLease(Duration.ofSeconds(20))
                .maxConnections(Math.min(maxConnections, 16))
                .aggregateCardinality(Math.min(aggregateCardinality, 256))
                .warmup(Duration.ofSeconds(3))
                .duration(Duration.ofSeconds(20))
                .build();
    }

    /** A copy of this config with only {@code duration} changed — everything else carries over as-is. */
    public BenchmarkConfig withDuration(Duration duration) {
        return toBuilder().duration(duration).build();
    }

    public static final class Builder {
        private int bucketCount = 256;
        private int workers = 8;
        private Duration pollInterval = Duration.ofMillis(100);   // RelayConfig's own default
        private int batchSize = 100;
        private Duration rowLease = Duration.ofSeconds(60);
        private long deliveryTimeoutMs = 30_000;   // Kafka producer default (LLD-kafka §1)
        private int maxAttempts = 10;
        private int maxConnections = 32;
        private int payloadBytes = 1024;
        private int aggregateCardinality = 1024;
        private Duration warmup = Duration.ofSeconds(30);
        private Duration duration = Duration.ofMinutes(10);
        private Duration window = Duration.ofMinutes(20);
        private double offeredRate = 0;   // 0 = the scenario finds its own
        private Duration retention = Duration.ofDays(14);        // RelayConfig's own defaults
        private Duration cleanupInterval = Duration.ofMinutes(15);
        private int cleanupBatchSize = 1000;
        private LatencyMode latencyMode = LatencyMode.PROXY;

        private Builder() {
        }

        public Builder bucketCount(int bucketCount) {
            this.bucketCount = positive(bucketCount, "bucketCount");
            return this;
        }

        public Builder workers(int workers) {
            this.workers = positive(workers, "workers");
            return this;
        }

        public Builder pollInterval(Duration pollInterval) {
            Objects.requireNonNull(pollInterval, "pollInterval");
            if (pollInterval.isNegative() || pollInterval.isZero()) {
                throw new IllegalArgumentException("pollInterval must be positive");
            }
            this.pollInterval = pollInterval;
            return this;
        }

        public Builder batchSize(int batchSize) {
            this.batchSize = positive(batchSize, "batchSize");
            return this;
        }

        public Builder rowLease(Duration rowLease) {
            this.rowLease = Objects.requireNonNull(rowLease, "rowLease");
            return this;
        }

        public Builder deliveryTimeoutMs(long deliveryTimeoutMs) {
            if (deliveryTimeoutMs <= 0) {
                throw new IllegalArgumentException("deliveryTimeoutMs must be positive");
            }
            this.deliveryTimeoutMs = deliveryTimeoutMs;
            return this;
        }

        public Builder maxAttempts(int maxAttempts) {
            this.maxAttempts = positive(maxAttempts, "maxAttempts");
            return this;
        }

        public Builder maxConnections(int maxConnections) {
            this.maxConnections = positive(maxConnections, "maxConnections");
            return this;
        }

        public Builder payloadBytes(int payloadBytes) {
            this.payloadBytes = positive(payloadBytes, "payloadBytes");
            return this;
        }

        public Builder aggregateCardinality(int aggregateCardinality) {
            if (aggregateCardinality < 2) {
                throw new IllegalArgumentException("aggregateCardinality must be at least 2");
            }
            this.aggregateCardinality = aggregateCardinality;
            return this;
        }

        public Builder warmup(Duration warmup) {
            this.warmup = Objects.requireNonNull(warmup, "warmup");
            return this;
        }

        public Builder duration(Duration duration) {
            this.duration = Objects.requireNonNull(duration, "duration");
            return this;
        }

        /** The endurance reporting window; must be positive. Default 20 minutes. */
        public Builder window(Duration window) {
            Objects.requireNonNull(window, "window");
            if (window.isNegative() || window.isZero()) {
                throw new IllegalArgumentException("window must be positive");
            }
            this.window = window;
            return this;
        }

        /** Fixed offered rate in events/s; {@code 0} leaves the scenario to find one. */
        public Builder offeredRate(double offeredRate) {
            if (offeredRate < 0) {
                throw new IllegalArgumentException("offeredRate must not be negative");
            }
            this.offeredRate = offeredRate;
            return this;
        }

        /** How long a {@code DONE} row is kept; must be positive. Default 14 days. */
        public Builder retention(Duration retention) {
            this.retention = Objects.requireNonNull(retention, "retention");
            return this;
        }

        public Builder cleanupInterval(Duration cleanupInterval) {
            this.cleanupInterval = Objects.requireNonNull(cleanupInterval, "cleanupInterval");
            return this;
        }

        public Builder cleanupBatchSize(int cleanupBatchSize) {
            this.cleanupBatchSize = positive(cleanupBatchSize, "cleanupBatchSize");
            return this;
        }

        public Builder latencyMode(LatencyMode latencyMode) {
            this.latencyMode = Objects.requireNonNull(latencyMode, "latencyMode");
            return this;
        }

        public BenchmarkConfig build() {
            return new BenchmarkConfig(this);
        }

        private static int positive(int value, String field) {
            if (value <= 0) {
                throw new IllegalArgumentException(field + " must be positive");
            }
            return value;
        }
    }
}
