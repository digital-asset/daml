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
--
-- Note on event sequential IDs:
-- These IDs must be assigned sequentially by the indexer such that for all events ev1, ev2 it holds that
-- (ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)
---------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_divulgence (
    -- * fixed-size columns first to avoid padding
   event_sequential_id bigint NOT NULL, -- event identification: same ordering as event_offset

    -- * event identification
    event_offset text, -- offset of the transaction that divulged the contract

    -- * transaction metadata
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,
    application_id text,
    submitters text[],

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL, -- informees

    -- * contract data
    create_argument bytea,

    -- * compression flags
    create_argument_compression SMALLINT
);

---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_create (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time timestamp NOT NULL, -- transaction metadata
    node_index integer NOT NULL,              -- event metadata

    -- * event identification
    event_offset text NOT NULL,

    -- * transaction metadata
    transaction_id text NOT NULL,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    event_id text NOT NULL,       -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text NOT NULL,
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL, -- stakeholders
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL, -- informees

    -- * contract data
    create_argument bytea NOT NULL,
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
CREATE TABLE participant_events_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time timestamp NOT NULL, -- transaction metadata
    node_index integer NOT NULL,              -- event metadata

    -- * event identification
    event_offset text NOT NULL,

    -- * transaction metadata
    transaction_id text NOT NULL,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    event_id text NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text NOT NULL,
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL, -- stakeholders
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,        -- used for the mutable state cache

    -- * choice data
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
CREATE TABLE participant_events_non_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time timestamp NOT NULL, -- transaction metadata
    node_index integer NOT NULL,              -- event metadata

    -- * event identification
    event_offset text NOT NULL,

    -- * transaction metadata
    transaction_id text NOT NULL,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    event_id text NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text NOT NULL,
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL, -- stakeholders
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,        -- used for the mutable state cache

    -- * choice data
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
