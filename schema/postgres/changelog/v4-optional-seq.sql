--liquibase formatted sql

-- Schema version v4 — `seq` becomes optional, and the row records where its number came from.
--
-- Everything here has shipped and is IMMUTABLE: an operator's DATABASECHANGELOG already records the
-- checksums, so a later schema change appends a new v<n> file instead of editing this one.

--changeset tandem:v4-drop-tandem-outbox-seq-not-null stripComments:false
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

--changeset tandem:v4-add-tandem-outbox-seq-source stripComments:false
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

--changeset tandem:v4-add-tandem-outbox-seq-source-check stripComments:false
-- seq_source = NONE and a NULL seq are the same fact stated twice, so the database enforces the
-- agreement rather than trusting every writer to keep it. This is the one thing the reader-tolerance
-- rule above cannot recover on its own: whether the row has a `seq` at all.
ALTER TABLE tandem_outbox
    ADD CONSTRAINT tandem_outbox_seq_source_agrees CHECK ((seq IS NULL) = (seq_source = 2));
