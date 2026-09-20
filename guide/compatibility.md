# Tandem Database Compatibility

---

## 0. The question this page answers

Tandem's documentation says "PostgreSQL". You are on Aurora, AlloyDB, Neon, Supabase,
CloudNativePG or a managed instance from a cloud provider, and you need to know whether that
counts.

!!! question "Short answer"
    If it is **PostgreSQL** (any distribution, managed or self-hosted, version 14 or later),
    Tandem runs on it with no new SQL and no configuration. If it is **PostgreSQL-compatible**
    (it speaks the wire protocol but is a different engine underneath), the answer depends on one
    property, and §1 is how you check it yourself.

This page also states, deliberately, what is **not** supported and why: an engine that cannot
express Tandem's claim is a permanent exclusion, not a feature request.

---

## 1. What Tandem actually requires of a database

The filter is not the SQL dialect. Tandem asks a database for five things, and an engine either
has them or does not.

1. **A transactional `INSERT` on the caller's own connection.** The outbox row and the business
   state change commit together or not at all. Every SQL database qualifies; it is listed because
   it is the property the whole pattern rests on.
2. **A non-blocking claim primitive: `SELECT … FOR UPDATE SKIP LOCKED`.** This is the discriminant.
   Tandem's relay claims work with one statement (a `SKIP LOCKED` select inside a CTE, then
   `UPDATE … RETURNING`), which is what lets N workers take disjoint row sets without blocking each
   other. An engine with no pessimistic row locking cannot express it, and no amount of adapter code
   recovers that.
3. **`id` assigned in insert order, from a sequence that hands out numbers one at a time.**
   Per-aggregate ordering is established at write time and the relay dispatches in `id` order
   ([HLD §4.2](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/docs/HLD.md)). An engine whose identity
   values are node-local, cached per session or deliberately non-monotonic breaks the ordering
   guarantee silently, which is the one failure mode worse than an outright error.
4. **No aborts the application is expected to retry.** Tandem runs at `READ COMMITTED` and handles
   exactly one SQLSTATE, the unique violation on `(aggregate_id, seq)`. An engine that raises
   serialization failures (`40001`) under normal concurrency needs retry handling Tandem does not
   have.
5. **A JDBC driver, plus a way to run the suite against it.** Compatibility that nobody has
   executed is a claim, not a fact. §2 is how this page keeps the two apart.

Everything else Tandem uses (`jsonb`, `ON CONFLICT`, `RETURNING`, array binds, partial indexes,
`now() + interval`, `TIMESTAMPTZ`) is ordinary PostgreSQL and comes along with the wire protocol in
practice. Two optional features need more than that, and both **degrade instead of failing**:

- **`Wakeup.PG_NOTIFY`** needs `LISTEN`/`NOTIFY` and PostgreSQL's own JDBC driver. Without it the
  relay finds the row on its next poll, so the cost is latency, not delivery.
- **`lockedWrite()`** needs `pg_advisory_xact_lock()` and `hashtext()`. `hashtext()` is an internal
  PostgreSQL function, so it is the first thing to check on a derived engine. Without it you keep
  whatever serialization your own write path already provides
  ([§3.2 of the adoption guide](adoption.md#32-where-seq-comes-from-and-whether-you-need-tandems-lock)).

---

## 2. What "supported" means here: three levels, never blurred

| Level | What stands behind it |
|---|---|
| **CI-verified** | Tandem's full Testcontainers integration and end-to-end suites run against this engine in CI: on every change to the schema, the JDBC adapter or the test harness, and again every week on a schedule. A regression fails the build. |
| **Verified once** | A recorded manual run of the same suites against a real instance, with the date and the exact version. It says the engine worked then, not that it is watched. |
| **Expected compatible** | Nothing Tandem needs is known to be missing, and nobody has run it. Useful, and not the same thing as the two rows above. |

A matrix that blurs these is worse than no matrix, so a row never carries a level it has not earned.

---

## 3. PostgreSQL versions

**Supported: PostgreSQL 14 and later.** The floor tracks the versions the PostgreSQL community
still patches; 13 reached end of life in November 2025 and is no longer covered. Nothing in
Tandem's SQL needs a recent major (the constructs it uses date back to PostgreSQL 10), so a 13
instance will very likely work, with the security posture of an unpatched database and no test run
behind it.

| Version | Level |
|---|---|
| PostgreSQL 16 | **CI-verified**: the default image, exercised by every build |
| PostgreSQL 14 and 18 | **CI-verified**: the two ends of the supported range |
| PostgreSQL 15 and 17 | Supported. They sit between two verified majors, and are not exercised separately |
| PostgreSQL 13 and earlier | Not supported; end of life upstream |

The matrix lives in
[`.github/workflows/postgres-majors.yml`](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/.github/workflows/postgres-majors.yml).
It runs the same suites the default build runs, on a change to `schema/`, `tandem-jdbc/` or
`tandem-test/`, weekly on a schedule, and on demand. A commit that cannot affect the SQL does not
re-run it, which is why the schedule is there: it keeps the claim from ageing quietly during a month
of documentation commits.

Only the ends of the range, deliberately: nothing in Tandem's SQL is version-sensitive today, so what
a matrix can still catch is a *future* construct needing a newer major, or one an older major drops.
Both ends catch that, and a middle major would earn its run only by failing where neither neighbour
does.

---

## 4. Managed PostgreSQL and PostgreSQL-derived engines

| Engine | Claim | Schema | `PG_NOTIFY` wakeup | `lockedWrite()` | Level |
|---|---|---|---|---|---|
| Amazon RDS for PostgreSQL | yes | yes | yes | yes | Expected compatible |
| Google Cloud SQL for PostgreSQL | yes | yes | yes | yes | Expected compatible |
| Azure Database for PostgreSQL (Flexible Server) | yes | yes | yes | yes | Expected compatible |
| CloudNativePG, Zalando/Crunchy operators | yes | yes | yes | yes | Expected compatible |
| Amazon Aurora PostgreSQL | yes | yes | writer only, see §5 | yes | Expected compatible |
| Google AlloyDB | yes | yes | yes | yes | Expected compatible; engine exercised, see below |
| Neon | yes | yes | see §5 | yes | Expected compatible |
| Supabase | yes | yes | direct connection only, see §5 | yes | Expected compatible |
| TimescaleDB | yes | yes | yes | yes | Expected compatible |
| Citus | only as a local or reference table, see §6 | yes | yes | yes | Not recommended |

The first four rows **are** PostgreSQL: same engine, same executor, same driver. The only honest
doubt about them is the version you run and the pooler you put in front, which is §3 and §5.
Aurora and AlloyDB replace the storage layer and keep the PostgreSQL executor, so every construct
Tandem uses exists; their caveats are operational. Neon and Supabase run PostgreSQL itself, and
their caveats are about connection handling.

No row says *Verified once* yet. When one does, it will carry the date and the engine version in
this table, and nowhere else.

### What running them actually showed

Two of these engines are distributed as container images, so they were put in front of Tandem's own
integration suites on **2026-09-18**. The results are why those rows still read *Expected compatible*
rather than something stronger.

**AlloyDB Omni 17** (`google/alloydbomni:17`, the downloadable engine) **ran Tandem's SQL without a
single failure**: 147 JDBC integration tests plus the admin and Kafka suites, 166 in total, covering
the claim, the lease coordination, the head-of-chain gate, cleanup, replay and the wakeup. Nothing
Tandem asks of a database was missing. It is not in CI for reasons that have nothing to do with the
engine's SQL: the image is 3.1 GB, it needs a startup timeout well past Testcontainers' default, and
the suites that start several PostgreSQL containers at once outrun even a generous one. And it would
still be the downloadable engine, not the managed service, which is the part an adopter is actually
asking about.

**Supabase's distribution** (`supabase/postgres`) could not be started by the harness at all. The
image pins its own bootstrap to its own roles: setting `POSTGRES_USER`, which Testcontainers always
does, makes its init fail (`role "supabase_admin" does not exist`) and the container exit before a
single statement runs. That is a fact about packaging, not about the engine, which is PostgreSQL and
behaves like it. Automating this row would take a bespoke container class.

**Aurora PostgreSQL and Neon** have no image at all. The only way to move them up a level is a run
against a real instance.

---

## 5. The operational caveats, which matter more than the SQL

These decide whether the **optional** parts work. None of them affects delivery: the relay keeps
polling and keeps publishing in order.

!!! warning "A connection pooler in transaction mode silently disables the wakeup"
    PgBouncer's default mode, RDS Proxy, AlloyDB's built-in pooler and Supabase's Supavisor all
    hand a session to another client between transactions, which breaks `LISTEN`. The relay logs
    the loss and falls back to polling. If you want `Wakeup.PG_NOTIFY`, give the relay a direct
    connection (session pooling is also fine) and size the pool knowing the listener holds one
    connection for the relay's lifetime.

**Aurora PostgreSQL.** Point the relay at the **writer endpoint**: it writes on every claim, and
notifications do not cross instances. A failover invalidates the listening connection, which
`PgNotifyWakeup` handles by reconnecting with backoff and re-waking every worker, so nothing
emitted during the gap is lost, it is simply found by the poll.

**Neon and other scale-to-zero platforms.** An idle compute suspends and takes the dedicated
listening connection with it. Same handling as above, same result. A relay that polls is also a
workload that keeps the compute awake, which is a cost question rather than a correctness one.

**Supabase.** The pooled connection string is Supavisor in transaction mode. Use the direct
connection for the relay if you want the wakeup.

**Read replicas.** Tandem never reads from a replica: the claim, the lease and the metrics queries
all write or need the latest committed state. Route everything to the primary.

---

## 6. Not supported, and why

### Permanently out: no pessimistic locking

**Amazon Aurora DSQL** and **Google Cloud Spanner (PostgreSQL interface)** have no `FOR UPDATE`
and no `SKIP LOCKED`; they resolve write conflicts optimistically, at commit. Tandem's claim is not
expressible on them, and an adapter cannot add what the engine does not have. This is an
architectural exclusion, not a backlog item. An outbox on those engines needs a different claim
model, which is a different product.

### Out for now: a behavioural port disguised as a free one

**CockroachDB** and **YugabyteDB** speak the PostgreSQL wire protocol, and most of Tandem's SQL
would parse. They are still not supported, for reasons that are about behaviour rather than syntax:

- They surface **serialization retries** (`40001`) to the application under normal concurrency.
  Tandem has no retry handling for them, so a relay would fail claims it should simply repeat.
- Their **id generation is not commit-ordered** in the way requirement 3 of §1 needs, and per-aggregate
  ordering is exactly what Tandem sells. The remedy on PostgreSQL is `lockedWrite()`, which needs
  advisory locks and `hashtext()`, and those are the least portable pieces of the write side.
- **No `LISTEN`/`NOTIFY`**, so the wakeup is unavailable (a degradation, not a blocker).
- Short leases plus one `UPDATE` per row is a workload that pays consensus latency on every step.

Supporting them means owning a retry strategy, a different ordering argument and a performance
profile, so it is a port, and a port has to be built and measured rather than claimed.

### Out: distributed extensions over PostgreSQL

**Citus** works only if `tandem_outbox` stays a **local or reference table**. Distributed, the
claim's `ORDER BY id` and its locking no longer mean across shards what the relay assumes.

### Not the right question: H2 and SQLite

These usually come up as *"can I test without a database?"*. That has a direct answer, and it is
not an embedded SQL engine: `InMemoryOutbox` in `tandem-test` is a real implementation of the
outbox port, so unit tests run with no Docker and no database at all
([LLD-test §2](https://github.com/alirux/tandem-transactional-outbox-kafka/blob/main/docs/LLD-test.md)). For the paths that
must touch SQL, `TandemTestContainer` starts the real thing.

---

## 7. Checking your own engine

Tandem's own integration suites are the check. Point them at any image that exposes PostgreSQL:

```bash
TANDEM_TEST_POSTGRES_IMAGE=postgres:17-alpine ./gradlew integrationTest
```

The variable (or the `tandem.test.postgres.image` system property) is read by
`TandemTestContainer.postgresImage()`, which both the end-to-end harness and the JDBC integration
tests take their image from. A green run covers the claim, the lease coordination, the head-of-chain
gate, cleanup and the full write-to-Kafka path against that engine.

For a managed instance there is no image to point at, so the equivalent is running the suites
against a throwaway database on it. If you do that, the result is worth a pull request: it is what
turns an *Expected compatible* row into a *Verified once* row, with your date and version on it.
