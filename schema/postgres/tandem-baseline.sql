-- Tandem — baseline schema (PostgreSQL)
--
-- GENERATED FILE — do not edit. Regenerate with `./gradlew generateBaselineSql`.
-- The source of truth is changelog/db.changelog-master.xml; CI fails if this file drifts from it.
--
-- This is the ready-to-apply baseline DDL for an operator who does not run Liquibase (LLD-jdbc §6):
-- apply it to an empty database and you have the complete current schema. Liquibase users point
-- `liquibase update` at the changelog instead and get the same schema plus change tracking. Either
-- way the library does NOT run migrations itself.
--
-- The schema is a long-lived contract shared by the client write-side, the relay, and the Admin API
-- (possibly at different Tandem versions on the same DB), so it MUST evolve ADDITIVELY only: new
-- optional/nullable columns, new indexes or tables — never a removal, rename, type change, or newly
-- required column (HLD §1.4). Optional features (attempt archive, causal-ordering clock) ship their
-- own separate DDL and are NOT part of this baseline.
--
-- Targets PostgreSQL 13+. For MySQL, see schema/mysql (pending Q28: partial-index workaround,
-- type mappings). The `bucket` value is computed in Java by tandem-jdbc (engine-independent),
-- so there is no DB-specific bucket/hash function to port.

-- Changeset v1-baseline.sql::v1-create-tandem-outbox::tandem
-- ---------------------------------------------------------------------------
-- Core (always required)
-- ---------------------------------------------------------------------------
--
-- The outbox itself: the transactional handoff point between the client write-side and the relay.
CREATE TABLE tandem_outbox (
    id              BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    aggregate_id    VARCHAR(255) NOT NULL,
    aggregate_type  VARCHAR(255) NOT NULL,
    type            VARCHAR(255),            -- CloudEvents `type`, e.g. com.acme.order.placed; nullable (Q20)
    bucket          SMALLINT     NOT NULL,   -- virtual bucket = Math.floorMod(fnv1a64(aggregate_id), B); computed in Java by tandem-jdbc at insert (HLD §4.3)
    seq             BIGINT       NOT NULL,
    payload         JSONB        NOT NULL,   -- JSONB by default; switch to BYTEA only if a binary serializer (Avro/Protobuf) is used (HLD §5.2)
    headers         JSONB,
    status          SMALLINT     NOT NULL DEFAULT 0,
    -- 0 = PENDING, 1 = IN_FLIGHT, 2 = DONE, 3 = FAILED, 4 = DISCARDED
    locked_by       VARCHAR(64),
    locked_until    TIMESTAMPTZ,
    attempts        INT          NOT NULL DEFAULT 0,
    last_error      TEXT,
    next_attempt_at TIMESTAMPTZ,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    discard_reason  TEXT,                    -- operator-supplied reason when the Admin API discards a FAILED row (HLD-admin-api §6.1); distinct from last_error, which stays the original delivery failure
    correlation_id  VARCHAR(255),            -- searchable copy of headers['correlation-id'] (HLD-tracing §4); headers stay the source of truth for what reaches Kafka. Bounded length: the value typically arrives from OUTSIDE this application (an inbound HTTP header, a consumed message), so it is untrusted input and must not widen an index without limit

    UNIQUE (aggregate_id, seq)               -- per-aggregate ordering safety net (HLD §4.2)
);

-- Changeset v1-baseline.sql::v1-create-tandem-meta::tandem
-- Cross-cutting Tandem metadata, keyed by name (LLD-bucket-count-guard §5). Three keys today:
--   bucket_count  — the single value the write-side and the relay must agree on.
--   relay_paused  — the whole-relay desired state the Admin API writes and every relay instance
--                   re-reads on its control-refresh tick (HLD-admin-api §4.1).
--   coordination  — SINGLE or LEASE, written by the relay at startup so the Admin API knows which
--                   per-bucket endpoints this deployment can actually answer (HLD-admin-api §4.1);
--                   without it the admin would query LEASE-only tables that either do not exist or
--                   are present but unmaintained, and report a healthy relay as stalled.
-- All three live here rather than in dedicated tables because this is the one key/value table
-- present in EVERY deployment — `tandem_bucket_lease` below exists only under LEASE coordination,
-- so a pause stored there could not reach a SINGLE-mode relay. Deliberately NOT seeded here — the
-- guard seeds it on first startup with whatever bucketCount the operator configured, so a fresh
-- database with a non-default bucketCount is correct without editing this file (unlike the lease
-- table below, whose row count must equal B and so is seeded). A database created before this table
-- existed simply has the guard seed the row on first startup under the new version (backward- and
-- forward-compatible, HLD §1.4). `value` is stored as text and parsed by the adapter, so the table
-- is not typed to this one setting.
CREATE TABLE tandem_meta (
    key         TEXT         PRIMARY KEY,
    value       TEXT         NOT NULL,
    updated_at  TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Changeset v1-baseline.sql::v1-create-index-dispatch::tandem
-- Partial index driving the bucket poll (only PENDING rows), by bucket then id.
CREATE INDEX idx_tandem_outbox_dispatch
    ON tandem_outbox (bucket, id)
    WHERE status = 0;

-- Changeset v1-baseline.sql::v1-create-index-aggregate::tandem
-- Supports the head-of-chain / poison-gate NOT EXISTS check per aggregate (HLD §6; LLD-jdbc §3.3).
CREATE INDEX idx_tandem_outbox_aggregate
    ON tandem_outbox (aggregate_id, id)
    WHERE status IN (0, 1, 3);

-- Changeset v1-baseline.sql::v1-create-index-inflight::tandem
-- Supports the periodic lease-reclaim (LLD-jdbc §3.5): "status = 1 AND locked_until < now()", run
-- every reclaimInterval (~5s) on every instance. Partial on IN_FLIGHT only, so it stays tiny (in-flight
-- rows are transient and few) and the reclaim scans expired leases, not the whole table — which matters
-- once DONE rows accumulate between cleanup passes on a large, high-throughput outbox.
CREATE INDEX idx_tandem_outbox_inflight
    ON tandem_outbox (locked_until)
    WHERE status = 1;

-- Changeset v1-baseline.sql::v1-create-index-failed::tandem
-- Supports the two metrics-tick readings over FAILED rows (LLD-jdbc §4): `failed.count`, and the
-- `blocked.count` subquery that groups failures by aggregate. Both otherwise seq-scan the whole outbox
-- on every tick, because no other index can answer "status = 3" alone. Partial on FAILED only, so it is
-- normally empty and stays negligible: measured on a 500k-row outbox with three failed rows it was 16 kB
-- against a 47 MB table, and took failed.count from 30.7ms/6075 buffers to 0.06ms/3.
CREATE INDEX idx_tandem_outbox_failed
    ON tandem_outbox (aggregate_id, id)
    WHERE status = 3;

-- Changeset v1-baseline.sql::v1-create-index-correlation::tandem
-- Supports the Admin API's search by correlation id (HLD-admin-api §4) — the incident-time lookup, where
-- the correlation id is often the ONLY identifier an operator has (it comes from a log line, an alert or a
-- customer ticket; the aggregate id is not known yet). A plain, non-partial B-tree on a real column rather
-- than an expression/GIN index over `headers`: portable to MySQL 8 unchanged, where expression and partial
-- indexes are unavailable and would need a generated-column workaround (LLD-jdbc §5). Non-unique by nature —
-- one correlation id spans many rows, typically across several aggregates (HLD-tracing §2). NULL for every
-- row written with tracing off, and Postgres B-trees do index NULLs, so `(correlation_id) WHERE
-- correlation_id IS NOT NULL` would be smaller — kept non-partial deliberately, for the same MySQL
-- portability reason.
CREATE INDEX idx_tandem_outbox_correlation
    ON tandem_outbox (correlation_id);

-- Changeset v1-baseline.sql::v1-create-tandem-bucket-lease::tandem
-- ---------------------------------------------------------------------------
-- LEASE coordination mode only (multi-instance)
--
-- The SINGLE coordination mode (one relay instance owns all buckets in-process) does NOT need
-- these tables. Create them only under LEASE — a horizontally-scaled client with an embedded
-- relay, or one or more standalone relay processes (LLD-jdbc §1/§3.2).
--
-- B (virtual bucket count) is fixed at first deploy and IMMUTABLE thereafter (B5): changing it
-- re-maps aggregates across buckets and would split an aggregate's events across workers. The
-- seed below matches the default B = 256 (buckets 0..255); if you configure a different B, seed
-- exactly that many rows and keep the config and seed in sync.
-- ---------------------------------------------------------------------------
CREATE TABLE tandem_bucket_lease (
    bucket       SMALLINT     PRIMARY KEY,   -- 0 .. B-1
    owner        VARCHAR(64),                -- worker id; NULL = free
    lease_until  TIMESTAMPTZ,                -- ownership expiry; renewed on heartbeat
    -- Set by the Admin API's POST /relay/pause with a bucket selector (HLD-admin-api §4.1). The owning
    -- worker keeps renewing the lease while paused and simply stops dispatching for this bucket, so a
    -- deliberately-idle bucket stays distinguishable from an uncovered (stalled) one.
    paused       BOOLEAN      NOT NULL DEFAULT false,
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Changeset v1-baseline.sql::v1-seed-tandem-bucket-lease::tandem
-- Seed one row per virtual bucket (default B = 256 → 0..255).
INSERT INTO tandem_bucket_lease (bucket)
SELECT generate_series(0, 255)::smallint;

-- Changeset v1-baseline.sql::v1-create-tandem-relay-member::tandem
-- Relay-instance membership (presence), decoupled from bucket ownership (LLD-jdbc §3.2). One row per
-- live relay instance, renewed each heartbeat. Its purpose is fair-share correctness: an instance that
-- currently owns ZERO buckets has no row in tandem_bucket_lease and would otherwise be invisible to
-- peers' live-owner count, so an incumbent holding every bucket would never learn a newcomer exists and
-- never release its fair share (a stable scale-up starvation). Counting live members here instead makes
-- a zero-owned joiner visible, so the incumbent releases and the fleet rebalances. A dead instance's row
-- simply expires (and is pruned on the next heartbeat). Not seeded — instances self-register at runtime.
CREATE TABLE tandem_relay_member (
    owner        VARCHAR(64)  PRIMARY KEY,   -- matches tandem_bucket_lease.owner
    lease_until  TIMESTAMPTZ  NOT NULL,      -- presence expiry; renewed on heartbeat
    updated_at   TIMESTAMPTZ  NOT NULL DEFAULT now()
);

-- Changeset v2-replays.sql::v2-add-tandem-outbox-replays::tandem
-- A replay rewrites the row's delivery state (status, attempts, lease columns) and leaves nothing
-- behind, so a replayed row has been indistinguishable from one that never was. That mattered twice:
-- the Admin API's own justification is that replay mutates delivery state and therefore needs an
-- audit trail (HLD-admin-api §4), yet the only trace was a log line in a different process; and the
-- relay cannot otherwise tell a legitimate replay from a genuine write-side ordering violation, since
-- the two present identically on the row (HLD §8).
--
-- Counted, not a boolean: "replayed twice" is a materially different story from "replayed once" when
-- reconstructing an incident. Distinct from `attempts`, which a replay resets because it is the
-- delivery budget of the current round — this survives for the lifetime of the row.
--
-- NOT NULL DEFAULT 0 so existing rows read as never-replayed without a backfill, and additive per
-- HLD §1.4: the relay's claim query does not select it, so a relay older than this column keeps
-- working against a migrated database unchanged.
ALTER TABLE tandem_outbox
    ADD COLUMN replays INT NOT NULL DEFAULT 0;

-- Changeset v3-managed-seq.sql::v3-create-tandem-seq::tandem
-- An aggregate with no version field has no source for `seq`, so adopting Tandem otherwise reaches
-- into the application's own domain schema rather than only adding the `tandem_*` tables
-- (HLD-managed-seq §1). This sequence is that source: in managed mode the write side omits the
-- column and the DEFAULT below supplies the value, so there is no extra statement, no round trip and
-- no lock on the caller's hot path.
--
-- CACHE 1 is a correctness constraint, not a performance default, and is stated explicitly rather
-- than inherited: with a per-session cache two sessions pre-allocate disjoint ranges (1-100,
-- 101-200), so a later-committing session can emit a LOWER `seq` for the same aggregate — which the
-- relay's seq-regression detector (HLD §7, §8) reports as a write-side ordering violation that never
-- happened.
--
-- The sequence is deliberately NOT tied to the column with OWNED BY: its lifecycle stays independent
-- of `tandem_outbox`.
--
-- START WITH 1 is right for a database whose aggregate types are all new to managed mode. Switching
-- an aggregate type that has ALREADY emitted app-assigned values needs the sequence moved above that
-- type's maximum (`ALTER SEQUENCE tandem_seq RESTART WITH ...`) or the next event collides with
-- UNIQUE (aggregate_id, seq). That belongs to enabling the feature, not to this migration, which
-- cannot know which types will ever be converted.
CREATE SEQUENCE tandem_seq AS BIGINT START WITH 1 INCREMENT BY 1 CACHE 1;

-- Changeset v3-managed-seq.sql::v3-default-tandem-outbox-seq::tandem
-- Strictly additive (HLD §1.4): `seq` is NOT NULL with no default today, so no existing writer can
-- omit the column, and a DEFAULT changes the behaviour of none of them. An adopter who never opts in
-- carries a sequence that is never advanced and a default that never fires.
ALTER TABLE tandem_outbox
    ALTER COLUMN seq SET DEFAULT nextval('tandem_seq');

-- Changeset v4-optional-seq.sql::v4-drop-tandem-outbox-seq-not-null::tandem
-- A message may now be written with no sequence number at all (HLD-managed-seq §4.5): consumers
-- deduplicate on the event id, which is unique by construction and always present. The write side
-- binds an explicit NULL for such a row, so the `DEFAULT nextval('tandem_seq')` added in v3 still
-- fires only for the managed mode, which omits the column.
--
-- `UNIQUE (aggregate_id, seq)` is left exactly as v1 created it and becomes inert for a row with no
-- `seq`: PostgreSQL treats NULLs as distinct, so such rows never collide there — and cannot, since
-- the duplicate it guards against is a stale application version.
ALTER TABLE tandem_outbox
    ALTER COLUMN seq DROP NOT NULL;

-- Changeset v4-optional-seq.sql::v4-add-tandem-outbox-seq-source::tandem
-- Where `seq` came from, which decides what the relay's ordering detector can check with it (HLD §8).
-- 0 = APPLICATION, 1 = MANAGED, 2 = NONE
--
-- Only an APPLICATION value carries information the row's `id` does not: it is the order the
-- application DECLARED, so publishing out of that order is a violation of the write-side contract
-- even when `id` order was respected. A MANAGED value comes from tandem_seq at INSERT and therefore
-- IS insert order, and a NONE row declares nothing — for both, the detector keys on `id`. The column
-- exists because a persisted row cannot otherwise tell the three apart: an APPLICATION and a MANAGED
-- `seq` are both just a BIGINT.
--
-- Deliberately NOT range-constrained to 0-2: a CHECK would make adding a fourth source a BREAKING
-- schema change, against the additive-only rule (HLD §1.4). Readers must instead tolerate a value
-- they do not know, and degrade to the `id` key — which under-reports and never invents a violation.
--
-- NOT NULL with no DEFAULT, deliberately: a default would let an INSERT that forgets the column
-- silently claim APPLICATION. Hence the three steps — the column arrives nullable, existing rows are
-- backfilled, and only then does the constraint go on.
--
-- **Existing rows are backfilled as APPLICATION**, because their real provenance is unrecoverable —
-- that is the very reason this column exists — and APPLICATION is overwhelmingly the likelier one:
-- `managedSeq()` first shipped in v0.7.0, so a database being migrated from it has had days rather
-- than its lifetime to write a managed row. The cost of guessing wrong is bounded and temporary: a
-- genuinely managed row is then judged on its number instead of on `id`, and because `id` and
-- `tandem_seq` are separate sequences that concurrent inserts can interleave, such a row could raise
-- an ordering violation that never happened. It applies only to rows still awaiting delivery — the
-- detector reads only what it publishes — so it drains with the existing backlog.
ALTER TABLE tandem_outbox
    ADD COLUMN seq_source SMALLINT;

UPDATE tandem_outbox
    SET seq_source = 0
    WHERE seq_source IS NULL;

ALTER TABLE tandem_outbox
    ALTER COLUMN seq_source SET NOT NULL;

-- Changeset v4-optional-seq.sql::v4-add-tandem-outbox-seq-source-check::tandem
-- seq_source = NONE and a NULL seq are the same fact stated twice, so the database enforces the
-- agreement rather than trusting every writer to keep it. This is the one thing the reader-tolerance
-- rule above cannot recover on its own: whether the row has a `seq` at all.
ALTER TABLE tandem_outbox
    ADD CONSTRAINT tandem_outbox_seq_source_agrees CHECK ((seq IS NULL) = (seq_source = 2));

-- Changeset v5-claim-under-backlog.sql::v5-create-index-pending-id::tandem
-- The claim orders by `id` and takes the first `batchSize` rows it can have. With only the v1 indexes
-- present, PostgreSQL satisfies that order by walking `tandem_outbox_pkey`, which visits DONE rows
-- too, and a busy outbox holds far more of those than pending ones: they accumulate for the whole
-- retention window before cleanup removes them. Under a backlog the claim therefore spends its time
-- crossing already-delivered rows to reach the few it wants, and the cost grows with the table rather
-- than with the work.
--
-- Measured on a 400 events/s outbox recovering from a 15-minute relay outage (281k pending, 85k
-- delivered rows awaiting cleanup): the primary key walk discarded 84 195 rows and took 877 ms, and
-- with this index it discarded 404 and took 15 ms. The gap widens with the number of delivered rows,
-- because that is exactly what this index does not contain.
--
-- Why not simply reuse `idx_tandem_outbox_dispatch (bucket, id) WHERE status = 0`: it orders by
-- bucket first, so satisfying `ORDER BY id` from it needs a sort, and the planner reasonably declines.
-- Ordering the claim by `(bucket, id)` instead was measured and is worse, not better: `id` order puts
-- each aggregate's OLDEST row first, which is the one the head-of-chain gate admits, so a bucket-major
-- order burns through rows the gate then rejects.
--
-- It costs a write: `status` changes twice per row, so every row inserts and removes an entry here,
-- and this index cannot be HOT-updated any more than the existing partial ones can.
CREATE INDEX idx_tandem_outbox_pending_id
    ON tandem_outbox (id)
    WHERE status = 0;

