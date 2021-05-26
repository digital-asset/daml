-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V100: Append-only schema
--
-- This is a major redesign of the index database schema. Updates from the ReadService are
-- now written into the append-only table participant_events, and the set of active contracts is
-- reconstructed from the log of create and archive events.
---------------------------------------------------------------------------------------------------

-- Stores which events were touched by which migration.
-- This metadata is not used for normal indexing operations and exists only to simplify fixing data migration issues.
CREATE TABLE participant_migration_history_v100 (
    -- * last event inserted before the migration was run
    ledger_end_sequential_id_before bigint,

    -- * last event inserted after the migration was run
    ledger_end_sequential_id_after bigint
    -- NOTE: events between ledger_end_sequential_id_before and ledger_end_sequential_id_after
    -- were created by the migration itself.
);
INSERT INTO participant_migration_history_v100 VALUES (
    (SELECT max(event_sequential_id) FROM participant_events),
    NULL -- updated at the end of this script
);

---------------------------------------------------------------------------------------------------
-- Events tables
--
-- The events tables are modified in the following order:
--   1. Create new append-only tables for events
--   2. Copy data from the old mutable tables into the new tables
--   3. Drop the old mutable tables
--   4. Create a view that contains the union of all events
---------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------
-- TODO append-only: reorder small fields to the end to avoid unnecessary padding.
CREATE TABLE participant_events_divulgence (
    -- * event identification
    event_sequential_id bigint NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)
    event_offset text, -- offset of the transaction that divulged the contract

    -- * transaction metadata
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument bytea,

    -- * compression flags
    create_argument_compression SMALLINT
);


---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------
-- TODO append-only: reorder small fields to the end to avoid unnecessary padding.
CREATE TABLE participant_events_create (
    -- * event identification
    event_sequential_id bigint NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset text NOT NULL,

    -- * transaction metadata
    transaction_id text NOT NULL,
    ledger_effective_time timestamp without time zone NOT NULL,
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer NOT NULL,
    event_id text NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text NOT NULL,
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument bytea NOT NULL,

    -- * create events only
    create_signatories text[] NOT NULL,
    create_observers text[] NOT NULL,
    create_agreement_text text,
    create_key_value bytea,
    create_key_hash text,

    -- * compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT
);
-- disable compression for columns containing data that is generally incompressible, this reduces the CPU usage
-- text and bytea values are compressed by default, "STORAGE EXTERNAL" disables the compression
ALTER TABLE participant_events_create ALTER COLUMN create_key_hash SET STORAGE EXTERNAL;


---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------
-- TODO append-only: reorder small fields to the end to avoid unnecessary padding.
CREATE TABLE participant_events_consuming_exercise (
    -- * event identification
    event_sequential_id bigint NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset text NOT NULL,

    -- * transaction metadata
    transaction_id text NOT NULL,
    ledger_effective_time timestamp without time zone NOT NULL,
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer NOT NULL,
    event_id text NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text NOT NULL,
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * information about the corresponding create event
    create_key_value bytea,        -- used for the mutable state cache

    -- * exercise events (consuming and non_consuming)
    exercise_choice text NOT NULL,
    exercise_argument bytea NOT NULL,
    exercise_result bytea,
    exercise_actors text[] NOT NULL,
    exercise_child_event_ids text[] NOT NULL,

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);


---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------
-- TODO append-only: reorder small fields to the end to avoid unnecessary padding.
CREATE TABLE participant_events_non_consuming_exercise (
    -- * event identification
    event_sequential_id bigint NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset text NOT NULL,

    -- * transaction metadata
    transaction_id text NOT NULL,
    ledger_effective_time timestamp without time zone NOT NULL,
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer NOT NULL,
    event_id text NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text NOT NULL,
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * information about the corresponding create event
    create_key_value bytea,        -- used for the mutable state cache

    -- * exercise events (consuming and non_consuming)
    exercise_choice text NOT NULL,
    exercise_argument bytea NOT NULL,
    exercise_result bytea,
    exercise_actors text[] NOT NULL,
    exercise_child_event_ids text[] NOT NULL,

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);


---------------------------------------------------------------------------------------------------
-- Data migration
---------------------------------------------------------------------------------------------------

-- Insert all create events and use the participant_contracts table
-- to fill in the create_key_hash for _active_ contracts.
INSERT INTO participant_events_create
(
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    create_argument_compression,
    create_signatories,
    create_observers,
    create_agreement_text,
    create_key_value,
    create_key_value_compression,
    create_key_hash
)
SELECT
    participant_events.event_sequential_id,
    participant_events.event_offset,
    participant_events.transaction_id,
    participant_events.ledger_effective_time,
    participant_events.command_id,
    participant_events.workflow_id,
    participant_events.application_id,
    participant_events.submitters,
    participant_events.node_index,
    participant_events.event_id,
    participant_events.contract_id,
    participant_events.template_id,
    participant_events.flat_event_witnesses,
    participant_events.tree_event_witnesses,
    participant_events.create_argument,
    participant_events.create_argument_compression,
    participant_events.create_signatories,
    participant_events.create_observers,
    participant_events.create_agreement_text,
    participant_events.create_key_value,
    participant_events.create_key_value_compression,
    participant_contracts.create_key_hash -- only works for active contracts
FROM participant_events LEFT JOIN participant_contracts USING (contract_id)
WHERE participant_events.exercise_consuming IS NULL -- create events
ORDER BY participant_events.event_sequential_id;

-- Insert all consuming exercise events
INSERT INTO participant_events_consuming_exercise
(
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    create_key_value,
    create_key_value_compression,
    exercise_choice,
    exercise_argument,
    exercise_argument_compression,
    exercise_result,
    exercise_result_compression,
    exercise_actors,
    exercise_child_event_ids
)
SELECT
    participant_events.event_sequential_id,
    participant_events.event_offset,
    participant_events.transaction_id,
    participant_events.ledger_effective_time,
    participant_events.command_id,
    participant_events.workflow_id,
    participant_events.application_id,
    participant_events.submitters,
    participant_events.node_index,
    participant_events.event_id,
    participant_events.contract_id,
    participant_events.template_id,
    participant_events.flat_event_witnesses,
    participant_events.tree_event_witnesses,
    participant_events.create_key_value,
    participant_events.create_key_value_compression,
    participant_events.exercise_choice,
    participant_events.exercise_argument,
    participant_events.exercise_argument_compression,
    participant_events.exercise_result,
    participant_events.exercise_result_compression,
    participant_events.exercise_actors,
    participant_events.exercise_child_event_ids
FROM participant_events
WHERE participant_events.exercise_consuming = TRUE -- consuming exercise events
ORDER BY participant_events.event_sequential_id;

-- Insert all non-consuming exercise events
INSERT INTO participant_events_non_consuming_exercise
(
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    create_key_value,
    create_key_value_compression,
    exercise_choice,
    exercise_argument,
    exercise_argument_compression,
    exercise_result,
    exercise_result_compression,
    exercise_actors,
    exercise_child_event_ids
)
SELECT
    participant_events.event_sequential_id,
    participant_events.event_offset,
    participant_events.transaction_id,
    participant_events.ledger_effective_time,
    participant_events.command_id,
    participant_events.workflow_id,
    participant_events.application_id,
    participant_events.submitters,
    participant_events.node_index,
    participant_events.event_id,
    participant_events.contract_id,
    participant_events.template_id,
    participant_events.flat_event_witnesses,
    participant_events.tree_event_witnesses,
    participant_events.create_key_value,
    participant_events.create_key_value_compression,
    participant_events.exercise_choice,
    participant_events.exercise_argument,
    participant_events.exercise_argument_compression,
    participant_events.exercise_result,
    participant_events.exercise_result_compression,
    participant_events.exercise_actors,
    participant_events.exercise_child_event_ids
FROM participant_events
WHERE participant_events.exercise_consuming = FALSE -- non-consuming exercise events
ORDER BY participant_events.event_sequential_id;

-- Temporary sequence to generate sequential IDs that appear after
-- all other already existing sequential IDs.
CREATE SEQUENCE temp_divulgence_sequential_id START 1;

-- Divulgence events did not exist before, we need to assign a new sequential ID for them.
-- They will all be inserted after all other events, i.e., at a point later than the transaction
-- that actually lead to the divulgence. This is OK, as we only use them for lookups.
-- In addition, we want to avoid rewriting the event_sequential_id of other events
-- for data continuity reasons.
WITH divulged_contracts AS (
    SELECT nextval('temp_divulgence_sequential_id') +
           (SELECT coalesce(max(event_sequential_id), 0) FROM participant_events) as event_sequential_id,
           contract_id,
           array_agg(contract_witness) as divulgees
    FROM participant_contract_witnesses
             INNER JOIN participant_contracts USING (contract_id)
             LEFT JOIN participant_events USING (contract_id)
    WHERE (NOT create_stakeholders @> array [contract_witness])
      AND exercise_consuming IS NULL -- create events only
      AND (tree_event_witnesses IS NULL OR NOT tree_event_witnesses @> array [contract_witness])
    GROUP BY contract_id
) INSERT INTO participant_events_divulgence (
    event_sequential_id,
    event_offset,
    command_id,
    workflow_id,
    application_id,
    submitters,
    contract_id,
    template_id,
    tree_event_witnesses,
    create_argument,
    create_argument_compression
)
SELECT
    event_sequential_id,
    -- The following 5 fields are metadata of the transaction that lead to the divulgence.
    -- We can't reconstruct this information from the old schema.
    NULL,
    NULL,
    '',
    NULL,
    NULL,
    contract_id,
    template_id,
    divulgees,
    create_argument,
    create_argument_compression
FROM divulged_contracts INNER JOIN participant_contracts USING (contract_id);

-- Drop temporary objects
DROP SEQUENCE temp_divulgence_sequential_id;
-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V100.1: Append-only schema
--
-- This step drops the now unused tables and creates a view that transparently replaces the old
-- participant_events table.
---------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------
-- Drop old tables, at this point all data has been copied to the new tables
---------------------------------------------------------------------------------------------------

DROP TABLE participant_contracts CASCADE;
DROP TABLE participant_contract_witnesses CASCADE;
DROP TABLE participant_events CASCADE;


---------------------------------------------------------------------------------------------------
-- Events table: view of all events
---------------------------------------------------------------------------------------------------

-- This view is used to drive the transaction and transaction tree streams,
-- which will in the future also contain divulgence events.
-- The event_kind field defines the type of event (numbers allocated to leave some space for future additions):
--    0: divulgence event
--   10: create event
--   20: consuming exercise event
--   25: non-consuming exercise event
-- TODO append-only: EITHER only include columns that are used in queries that use this view OR verify that the query planning
-- is not negatively affected by a long list of columns that are never used.
CREATE VIEW participant_events
AS
SELECT
    0::smallint as event_kind,
    event_sequential_id,
    NULL::text as event_offset,
    NULL::text as transaction_id,
    NULL::timestamp without time zone as ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    NULL::integer as node_index,
    NULL::text as event_id,
    contract_id,
    template_id,
    NULL::text[] as flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    NULL::text[] as create_signatories,
    NULL::text[] as create_observers,
    NULL::text as create_agreement_text,
    NULL::bytea as create_key_value,
    NULL::text as create_key_hash,
    NULL::text as exercise_choice,
    NULL::bytea as exercise_argument,
    NULL::bytea as exercise_result,
    NULL::text[] as exercise_actors,
    NULL::text[] as exercise_child_event_ids,
    create_argument_compression,
    NULL::smallint as create_key_value_compression,
    NULL::smallint as exercise_argument_compression,
    NULL::smallint as exercise_result_compression
FROM participant_events_divulgence
UNION ALL
SELECT
    10::smallint as event_kind,
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    create_signatories,
    create_observers,
    create_agreement_text,
    create_key_value,
    create_key_hash,
    NULL::text as exercise_choice,
    NULL::bytea as exercise_argument,
    NULL::bytea as exercise_result,
    NULL::text[] as exercise_actors,
    NULL::text[] as exercise_child_event_ids,
    create_argument_compression,
    create_key_value_compression,
    NULL::smallint as exercise_argument_compression,
    NULL::smallint as exercise_result_compression
FROM participant_events_create
UNION ALL
SELECT
    20::smallint as event_kind,
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    NULL::bytea as create_argument,
    NULL::text[] as create_signatories,
    NULL::text[] as create_observers,
    NULL::text as create_agreement_text,
    create_key_value,
    NULL::text as create_key_hash,
    exercise_choice,
    exercise_argument,
    exercise_result,
    exercise_actors,
    exercise_child_event_ids,
    NULL::smallint as create_argument_compression,
    create_key_value_compression,
    exercise_argument_compression,
    exercise_result_compression
FROM participant_events_consuming_exercise
UNION ALL
SELECT
    25::smallint as event_kind,
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    NULL::bytea as create_argument,
    NULL::text[] as create_signatories,
    NULL::text[] as create_observers,
    NULL::text as create_agreement_text,
    create_key_value,
    NULL::text as create_key_hash,
    exercise_choice,
    exercise_argument,
    exercise_result,
    exercise_actors,
    exercise_child_event_ids,
    NULL::smallint as create_argument_compression,
    create_key_value_compression,
    exercise_argument_compression,
    exercise_result_compression
FROM participant_events_non_consuming_exercise
;


---------------------------------------------------------------------------------------------------
-- Parameters table
---------------------------------------------------------------------------------------------------

-- new field: the sequential_event_id up to which all events have been ingested
ALTER TABLE parameters ADD COLUMN ledger_end_sequential_id bigint;
UPDATE parameters SET ledger_end_sequential_id = (
    SELECT max(event_sequential_id) FROM participant_events
);

-- Note that ledger_end_sequential_id_before will not be equal to ledger_end_sequential_id_after,
-- as the append-only migration creates divulgence events.
UPDATE participant_migration_history_v100
SET ledger_end_sequential_id_after = (
    SELECT max(ledger_end_sequential_id) FROM parameters
);
-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V100.2: Append-only schema
--
-- This step creates indices for the new tables of the append-only schema
---------------------------------------------------------------------------------------------------

---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_divulgence_event_offset ON participant_events_divulgence USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_divulgence_event_sequential_id ON participant_events_divulgence USING btree (event_sequential_id);

-- filtering by template
CREATE INDEX participant_events_divulgence_template_id_idx ON participant_events_divulgence USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_divulgence_tree_event_witnesses_idx ON participant_events_divulgence USING gin (tree_event_witnesses);

-- lookup divulgance events, in order of ingestion
CREATE INDEX participant_events_divulgence_contract_id_idx ON participant_events_divulgence USING btree (contract_id, event_sequential_id);


---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_create_event_offset ON participant_events_create USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_create_event_sequential_id ON participant_events_create USING btree (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_create_event_id_idx ON participant_events_create USING btree (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_create_transaction_id_idx ON participant_events_create USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_create_template_id_idx ON participant_events_create USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_create_flat_event_witnesses_idx ON participant_events_create USING gin (flat_event_witnesses);
CREATE INDEX participant_events_create_tree_event_witnesses_idx ON participant_events_create USING gin (tree_event_witnesses);

-- lookup by contract id
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create USING hash (contract_id);

-- lookup by contract_key
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create USING btree (create_key_hash, event_sequential_id);


---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_consuming_exercise_event_offset ON participant_events_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_consuming_exercise_event_sequential_id ON participant_events_consuming_exercise USING btree (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_consuming_exercise_event_id_idx ON participant_events_consuming_exercise USING btree (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_consuming_exercise_transaction_id_idx ON participant_events_consuming_exercise USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_consuming_exercise_template_id_idx ON participant_events_consuming_exercise USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_consuming_exercise_flat_event_witnesses_idx ON participant_events_consuming_exercise USING gin (flat_event_witnesses);
CREATE INDEX participant_events_consuming_exercise_tree_event_witnesses_idx ON participant_events_consuming_exercise USING gin (tree_event_witnesses);

-- lookup by contract id
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise USING hash (contract_id);


---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_non_consuming_exercise_event_offset ON participant_events_non_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_non_consuming_exercise_event_sequential_id ON participant_events_non_consuming_exercise USING btree (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_non_consuming_exercise_event_id_idx ON participant_events_non_consuming_exercise USING btree (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_non_consuming_exercise_transaction_id_idx ON participant_events_non_consuming_exercise USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_non_consuming_exercise_template_id_idx ON participant_events_non_consuming_exercise USING btree (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
-- NOTE: index name truncated because the full name exceeds the 63 characters length limit
CREATE INDEX participant_events_non_consuming_exercise_flat_event_witnes_idx ON participant_events_non_consuming_exercise USING gin (flat_event_witnesses);
CREATE INDEX participant_events_non_consuming_exercise_tree_event_witnes_idx ON participant_events_non_consuming_exercise USING gin (tree_event_witnesses);


---------------------------------------------------------------------------------------------------
-- Completions table
---------------------------------------------------------------------------------------------------

CREATE INDEX participant_command_completion_offset_application_idx ON participant_command_completions USING btree (completion_offset, application_id);
-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V100.3: Vacuum
--
-- This is a maintenance task run after the big migration to the append-only schema.
-- It is run in a separate migration because Flyway does not allow mixing transactional and
-- non-transactional statements in a single migration script.
---------------------------------------------------------------------------------------------------

VACUUM ANALYZE;
