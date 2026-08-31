--liquibase formatted sql

-- Schema version v5 — an index that keeps the claim's cost bounded when the outbox is behind.
--
-- Everything here has shipped and is IMMUTABLE: an operator's DATABASECHANGELOG already records the
-- checksums, so a later schema change appends a new v<n> file instead of editing this one.

--changeset tandem:v5-create-index-pending-id stripComments:false
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
