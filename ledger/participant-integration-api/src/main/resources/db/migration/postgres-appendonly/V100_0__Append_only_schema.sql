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
    submitters integer[],

    -- * shared event information
    contract_id text NOT NULL,
    template_id integer,
    tree_event_witnesses integer[] DEFAULT '{}'::integer[] NOT NULL, -- informees

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
    submitters integer[],

    -- * event metadata
    event_id text NOT NULL,       -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id integer NOT NULL,
    flat_event_witnesses integer[] DEFAULT '{}'::integer[] NOT NULL, -- stakeholders
    tree_event_witnesses integer[] DEFAULT '{}'::integer[] NOT NULL, -- informees

    -- * contract data
    create_argument bytea NOT NULL,
    create_signatories integer[] NOT NULL,
    create_observers integer[] NOT NULL,
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
    submitters integer[],

    -- * event metadata
    event_id text NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id integer NOT NULL,
    flat_event_witnesses integer[] DEFAULT '{}'::integer[] NOT NULL, -- stakeholders
    tree_event_witnesses integer[] DEFAULT '{}'::integer[] NOT NULL, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,        -- used for the mutable state cache

    -- * choice data
    exercise_choice text NOT NULL,
    exercise_argument bytea NOT NULL,
    exercise_result bytea,
    exercise_actors integer[] NOT NULL,
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
    submitters integer[],

    -- * event metadata
    event_id text NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id integer NOT NULL,
    flat_event_witnesses integer[] DEFAULT '{}'::integer[] NOT NULL, -- stakeholders
    tree_event_witnesses integer[] DEFAULT '{}'::integer[] NOT NULL, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,        -- used for the mutable state cache

    -- * choice data
    exercise_choice text NOT NULL,
    exercise_argument bytea NOT NULL,
    exercise_result bytea,
    exercise_actors integer[] NOT NULL,
    exercise_child_event_ids text[] NOT NULL,

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);



CREATE TABLE string_interning (
    id integer PRIMARY KEY NOT NULL,
    s text
);

ALTER TABLE participant_command_completions
  ALTER COLUMN submitters TYPE integer[] USING '{}'::integer[]; -- malicious: no migration PoC

ALTER TABLE party_entries
  ADD column party_id integer NOT NULL DEFAULT 0; -- malicious: no migration PoC

CREATE INDEX idx_party_entries_party_id_and_ledger_offset ON party_entries(party_id, ledger_offset);