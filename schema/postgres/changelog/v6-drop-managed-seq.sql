--liquibase formatted sql

-- Schema version v6: Tandem no longer assigns `seq`. A row carries the number the application gave
-- it, or none (HLD-seq §4.1, §4.5).
--
-- Everything here has shipped and is IMMUTABLE: an operator's DATABASECHANGELOG already records the
-- checksums, so a later schema change appends a new v<n> file instead of editing this one.
--
-- BREAKING: a writer from a release that still offers `managedSeq()` must be stopped before this
-- version is applied, and a relay from such a release cannot read the table once `seq_source` is gone.

--changeset tandem:v6-drop-tandem-outbox-seq-source-check stripComments:false
-- The agreement rule ties `seq_source` to whether `seq` is NULL. It goes first, because the next step
-- clears the number of the rows Tandem numbered, which that rule would refuse.
ALTER TABLE tandem_outbox
    DROP CONSTRAINT tandem_outbox_seq_source_agrees;

--changeset tandem:v6-unsequence-managed-rows stripComments:false
-- A row Tandem numbered (seq_source = 1) becomes a row with no number. Kept, its number would be read
-- as one the application declared, and the relay's ordering detector would judge it on a counter that
-- concurrent inserts can draw out of `id` order, reporting violations that never happened. A consumer
-- of such an aggregate stops receiving `ce_seq` from here on. A row Tandem numbered before v4 is not
-- among them: v4 recorded every existing row as the application's, so it keeps its number, as v4
-- already accepted.
UPDATE tandem_outbox
    SET seq = NULL
    WHERE seq_source = 1;

--changeset tandem:v6-drop-tandem-outbox-seq-source stripComments:false
-- With one source of numbers left, whether a row has a number is exactly whether `seq` is NULL.
ALTER TABLE tandem_outbox
    DROP COLUMN seq_source;

--changeset tandem:v6-drop-tandem-outbox-seq-default stripComments:false
-- Without a default, an INSERT that leaves out `seq` stores NULL, a row with no number, instead of
-- silently drawing one.
ALTER TABLE tandem_outbox
    ALTER COLUMN seq DROP DEFAULT;

DROP SEQUENCE tandem_seq;
