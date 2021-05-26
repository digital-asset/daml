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
    ledger_end_sequential_id_before NUMBER,

    -- * last event inserted after the migration was run
    ledger_end_sequential_id_after NUMBER
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
    event_sequential_id NUMBER NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)
    event_offset VARCHAR2(4000), -- offset of the transaction that divulged the contract

    -- * transaction metadata
    command_id VARCHAR2(4000),
    workflow_id VARCHAR2(4000),
    application_id VARCHAR2(4000),
    submitters CLOB CONSTRAINT ensure_json_ped_submitters CHECK (submitters IS JSON),

    -- * shared event information
    contract_id VARCHAR2(4000) NOT NULL,
    template_id VARCHAR2(4000),
    tree_event_witnesses CLOB DEFAULT '{}' NOT NULL CONSTRAINT ensure_json_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument BLOB,

    -- * compression flags
    create_argument_compression SMALLINT
);


---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------
-- TODO append-only: reorder small fields to the end to avoid unnecessary padding.
CREATE TABLE participant_events_create (
    -- * event identification
    event_sequential_id NUMBER NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset VARCHAR2(4000) NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR2(4000) NOT NULL,
    ledger_effective_time TIMESTAMP NOT NULL,
    command_id VARCHAR2(4000),
    workflow_id VARCHAR2(4000),
    application_id VARCHAR2(4000),
    submitters CLOB CONSTRAINT ensure_json_pec_submitters CHECK (submitters IS JSON),

    -- * event metadata
    node_index INTEGER NOT NULL,
    event_id VARCHAR2(4000) NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR2(4000) NOT NULL,
    template_id VARCHAR2(4000) NOT NULL,
    flat_event_witnesses CLOB DEFAULT '{}' NOT NULL CONSTRAINT ensure_json_pec_flat_event_witnesses CHECK (flat_event_witnesses IS JSON),       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses CLOB DEFAULT '{}' NOT NULL CONSTRAINT ensure_json_pec_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument BLOB NOT NULL,

    -- * create events only
    create_signatories CLOB NOT NULL CONSTRAINT ensure_json_create_signatories CHECK (create_signatories IS JSON),
    create_observers CLOB NOT NULL CONSTRAINT ensure_json_create_observers CHECK (create_observers is JSON),
    create_agreement_text VARCHAR2(4000),
    create_key_value BLOB,
    create_key_hash VARCHAR2(4000),

    -- * compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT
);

---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_consuming_exercise (
    -- * event identification
    event_sequential_id NUMBER NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset VARCHAR2(4000) NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR2(4000) NOT NULL,
    ledger_effective_time TIMESTAMP NOT NULL,
    command_id VARCHAR2(4000),
    workflow_id VARCHAR2(4000),
    application_id VARCHAR2(4000),
    submitters CLOB CONSTRAINT ensure_json_pece_submitters CHECK (submitters is JSON),

    -- * event metadata
    node_index INTEGER NOT NULL,
    event_id VARCHAR2(4000) NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR2(4000) NOT NULL,
    template_id VARCHAR2(4000) NOT NULL,
    flat_event_witnesses CLOB DEFAULT '{}' NOT NULL CONSTRAINT ensure_json_pece_flat_event_witnesses CHECK (flat_event_witnesses IS JSON),       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses CLOB DEFAULT '{}' NOT NULL CONSTRAINT ensure_json_pece_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),       -- informees for create, exercise, and divulgance events

    -- * information about the corresponding create event
    create_key_value BLOB,        -- used for the mutable state cache

    -- * exercise events (consuming and non_consuming)
    exercise_choice VARCHAR2(4000) NOT NULL,
    exercise_argument BLOB NOT NULL,
    exercise_result BLOB,
    exercise_actors CLOB NOT NULL CONSTRAINT ensure_json_pece_exercise_actors CHECK (exercise_actors IS JSON),
    exercise_child_event_ids CLOB NOT NULL CONSTRAINT ensure_json_pece_exercise_child_event_ids CHECK (exercise_child_event_ids IS JSON),

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
    event_sequential_id NUMBER NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset VARCHAR2(4000) NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR2(4000) NOT NULL,
    ledger_effective_time TIMESTAMP NOT NULL,
    command_id VARCHAR2(4000),
    workflow_id VARCHAR2(4000),
    application_id VARCHAR2(4000),
    submitters CLOB CONSTRAINT ensure_json_pence_submitters CHECK (submitters IS JSON),

    -- * event metadata
    node_index INTEGER NOT NULL,
    event_id VARCHAR2(4000) NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR2(4000) NOT NULL,
    template_id VARCHAR2(4000) NOT NULL,
    flat_event_witnesses CLOB DEFAULT '{}' NOT NULL CONSTRAINT ensure_json_pence_flat_event_witnesses CHECK (flat_event_witnesses IS JSON),       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses CLOB DEFAULT '{}' NOT NULL CONSTRAINT ensure_json_pence_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),       -- informees for create, exercise, and divulgance events

    -- * information about the corresponding create event
    create_key_value BLOB,        -- used for the mutable state cache

    -- * exercise events (consuming and non_consuming)
    exercise_choice VARCHAR2(4000) NOT NULL,
    exercise_argument BLOB NOT NULL,
    exercise_result BLOB,
    exercise_actors CLOB NOT NULL CONSTRAINT ensure_json_exercise_actors CHECK (exercise_actors IS JSON),
    exercise_child_event_ids CLOB NOT NULL CONSTRAINT ensure_json_exercise_child_event_ids CHECK (exercise_child_event_ids IS JSON),

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);

---------------------------------------------------------------------------------------------------
-- Parameters table
---------------------------------------------------------------------------------------------------

-- new field: the sequential_event_id up to which all events have been ingested
ALTER TABLE parameters ADD ledger_end_sequential_id NUMBER;
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
CREATE INDEX participant_events_divulgence_event_offset ON participant_events_divulgence(event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_divulgence_event_sequential_id ON participant_events_divulgence(event_sequential_id);

-- filtering by template
CREATE INDEX participant_events_divulgence_template_id_idx ON participant_events_divulgence(template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_divulgence_tree_event_witnesses_idx ON participant_events_divulgence(JSON_ARRAY(tree_event_witnesses));

-- lookup divulgance events, in order of ingestion
CREATE INDEX participant_events_divulgence_contract_id_idx ON participant_events_divulgence(contract_id, event_sequential_id);


---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_create_event_offset ON participant_events_create(event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_create_event_sequential_id ON participant_events_create(event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_create_event_id_idx ON participant_events_create(event_id);

-- lookup by transaction id
CREATE INDEX participant_events_create_transaction_id_idx ON participant_events_create(transaction_id);

-- filtering by template
CREATE INDEX participant_events_create_template_id_idx ON participant_events_create(template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_create_flat_event_witnesses_idx ON participant_events_create(JSON_ARRAY(flat_event_witnesses));
CREATE INDEX participant_events_create_tree_event_witnesses_idx ON participant_events_create(JSON_ARRAY(tree_event_witnesses));

-- lookup by contract id
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create(ORA_HASH(contract_id));

-- lookup by contract_key
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create(create_key_hash, event_sequential_id);


---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_consuming_exercise_event_offset ON participant_events_consuming_exercise(event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_consuming_exercise_event_sequential_id ON participant_events_consuming_exercise(event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_consuming_exercise_event_id_idx ON participant_events_consuming_exercise(event_id);

-- lookup by transaction id
CREATE INDEX participant_events_consuming_exercise_transaction_id_idx ON participant_events_consuming_exercise(transaction_id);

-- filtering by template
CREATE INDEX participant_events_consuming_exercise_template_id_idx ON participant_events_consuming_exercise(template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_consuming_exercise_flat_event_witnesses_idx ON participant_events_consuming_exercise(JSON_ARRAY(flat_event_witnesses));
CREATE INDEX participant_events_consuming_exercise_tree_event_witnesses_idx ON participant_events_consuming_exercise(JSON_ARRAY(tree_event_witnesses));

-- lookup by contract id
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise(ORA_HASH(contract_id));


---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_non_consuming_exercise_event_offset ON participant_events_non_consuming_exercise(event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_non_consuming_exercise_event_sequential_id ON participant_events_non_consuming_exercise(event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_non_consuming_exercise_event_id_idx ON participant_events_non_consuming_exercise(event_id);

-- lookup by transaction id
CREATE INDEX participant_events_non_consuming_exercise_transaction_id_idx ON participant_events_non_consuming_exercise(transaction_id);

-- filtering by template
CREATE INDEX participant_events_non_consuming_exercise_template_id_idx ON participant_events_non_consuming_exercise(template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- There is no equivalent to GIN index for oracle, but we explicitly mark as a JSON column for indexing
-- NOTE: index name truncated because the full name exceeds the 63 characters length limit
CREATE INDEX participant_events_non_consuming_exercise_flat_event_witnes_idx ON participant_events_non_consuming_exercise(JSON_ARRAY(flat_event_witnesses));
CREATE INDEX participant_events_non_consuming_exercise_tree_event_witnes_idx ON participant_events_non_consuming_exercise(JSON_ARRAY(tree_event_witnesses));
