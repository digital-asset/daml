-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V50: Append-only schema
--
-- This is a major redesign of the index database schema. Updates from the ReadService are
-- now written into the append-only table participant_events, and the set of active contracts is
-- reconstructed from the log of create and archive events.
---------------------------------------------------------------------------------------------------

-- This migration is work in progress. Data continuity is not implemented yet.
-- Even though this migration is hidden behind a feature flag, we add safety checks
-- that prevent it from running on a non-empty database.
CREATE TABLE safety_check(
    event_table_is_empty boolean NOT NULL,
    parameter_table_is_empty boolean NOT NULL
);
INSERT INTO safety_check
VALUES (
   CASE
       WHEN (SELECT count(*) FROM participant_events) > 0 THEN NULL
       ELSE true
   END,
   CASE
       WHEN (SELECT count(*) FROM parameters) > 0 THEN NULL
       ELSE true
   END
);
DROP TABLE safety_check;

-- drop the mutable tables
DROP table participant_contracts CASCADE;
DROP table participant_contract_witnesses CASCADE;

-- alter table parameters
DROP TABLE parameters;
CREATE TABLE parameters (
    ledger_id text NOT NULL,
    ledger_end bytea,
    ledger_end_sequential_id bigint, -- new field: the sequential_event_id up to which all events have been ingested
    external_ledger_end text,
    configuration bytea,
    participant_id text,
    participant_pruned_up_to_inclusive bytea
);

-- create, divulgence, consuming, and non-consuming events
-- statically partitioned to the individual event types so that the planner has solid statistics
--
-- TODO append-only: reorder small fields to the end to avoid unnecessary padding.
DROP TABLE participant_events CASCADE;
/*
 CREATE TABLE participant_events (
    -- * kinds of events
    event_kind smallint NOT NULL, -- Numbers allocated to leave some space for future additions.
    -- 0:  divulgence event
    -- 10: create event
    -- 20: consuming exercise event
    -- 25: non-consuming exercise event

    -- * event identification
    event_sequential_id bigserial NOT NULL, -- TODO temporarily readding bigserial for original write paths
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset bytea,                                 -- NULL for divulgence events

    -- * transaction metadata
    transaction_id text,                                -- NULL for migrated divulgence events
    ledger_effective_time timestamp without time zone,  -- NULL for migrated divulgence events
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer,  -- NULL for migrated divulgence events
    event_id text,       -- NULL for migrated divulgence events
    -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,     -- TODO @simon@ with the new divulgance model supporting public pkv implementations: we need this to enable NULL-s. Do we need to make involved indexes partial?
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument bytea,

    -- * create events only
    create_signatories text[],
    create_observers text[],
    create_agreement_text text,
    create_key_value bytea,
    create_key_hash bytea,

    -- * exercise events (consuming and non_consuming)
    exercise_choice text,
    exercise_argument bytea,
    exercise_result bytea,
    exercise_actors text[],
    exercise_child_event_ids text[],

    --compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
) PARTITION BY LIST (event_kind);


-- Set storage parameters before creating partitions so they follow the pattern.
-- these columns contain data that is generally incompressible, so don't try it
ALTER TABLE participant_events ALTER COLUMN create_key_hash  SET STORAGE EXTERNAL;

-- Partition the events according to the event type.
-- TODO: manually partition the table in order to avoid using PostgreSQL-specific features
CREATE TABLE participant_events_divulgence             PARTITION OF participant_events FOR VALUES IN (0);
CREATE TABLE participant_events_create                 PARTITION OF participant_events FOR VALUES IN (10);
CREATE TABLE participant_events_consuming_exercise     PARTITION OF participant_events FOR VALUES IN (20);
CREATE TABLE participant_events_non_consuming_exercise PARTITION OF participant_events FOR VALUES IN (25);
*/

---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_divulgence (
    -- * kinds of events
    -- [N/A for this event kind]: event_kind smallint NOT NULL,
    -- Numbers allocated to leave some space for future additions.
    -- 0:  divulgence event
    -- 10: create event
    -- 20: consuming exercise event
    -- 25: non-consuming exercise event

    -- * event identification
    event_sequential_id bigserial NOT NULL, -- TODO append-only: temporarily readding bigserial for original write paths
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    -- [N/A for this event kind]: event_offset bytea,                                 -- NULL for divulgence events

    -- * transaction metadata
    -- [N/A for this event kind]: transaction_id text,                                -- NULL for migrated divulgence events
    -- [N/A for this event kind]: ledger_effective_time timestamp without time zone,  -- NULL for migrated divulgence events
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    -- [N/A for this event kind]: node_index integer,  -- NULL for migrated divulgence events
    -- [N/A for this event kind]: event_id text,       -- NULL for migrated divulgence events
    -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,     -- TODO @simon@ with the new divulgance model supporting public pkv implementations: we need this to enable NULL-s. Do we need to make involved indexes partial?
    -- [N/A for this event kind]: flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument bytea,

    -- * create events only
    -- [N/A for this event kind]: create_signatories text[],
    -- [N/A for this event kind]: create_observers text[],
    -- [N/A for this event kind]: create_agreement_text text,
    -- [N/A for this event kind]: create_key_value bytea,
    -- [N/A for this event kind]: create_key_hash bytea,

    -- * exercise events (consuming and non_consuming)
    -- [N/A for this event kind]: exercise_choice text,
    -- [N/A for this event kind]: exercise_argument bytea,
    -- [N/A for this event kind]: exercise_result bytea,
    -- [N/A for this event kind]: exercise_actors text[],
    -- [N/A for this event kind]: exercise_child_event_ids text[],

    --compression flags
    create_argument_compression SMALLINT
    -- [N/A for this event kind]: create_key_value_compression SMALLINT,
    -- [N/A for this event kind]: exercise_argument_compression SMALLINT,
    -- [N/A for this event kind]: exercise_result_compression SMALLINT
);

-- [N/A for this event kind]: ALTER TABLE participant_events ALTER COLUMN create_key_hash  SET STORAGE EXTERNAL;
-- [N/A for this event kind]: CREATE INDEX participant_events_event_offset ON participant_events USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_divulgence_event_sequential_id ON participant_events_divulgence USING btree (event_sequential_id);

-- [N/A for this event kind]: CREATE INDEX participant_events_event_id_idx ON participant_events USING btree (event_id);
-- [N/A for this event kind]: CREATE INDEX participant_events_transaction_id_idx ON participant_events USING btree (transaction_id);

-- filtering by template
CREATE INDEX participant_events_divulgence_template_id_idx ON participant_events_divulgence USING btree (template_id);

-- [N/A for this event kind]: CREATE INDEX participant_events_flat_event_witnesses_idx ON participant_events USING gin (flat_event_witnesses);

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
CREATE TABLE participant_events_create (
    -- * kinds of events
    -- [N/A for this event kind]: event_kind smallint NOT NULL,
    -- Numbers allocated to leave some space for future additions.
    -- 0:  divulgence event
    -- 10: create event
    -- 20: consuming exercise event
    -- 25: non-consuming exercise event

    -- * event identification
    event_sequential_id bigserial NOT NULL, -- TODO append-only: temporarily readding bigserial for original write paths
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset bytea,

    -- * transaction metadata
    transaction_id text,
    ledger_effective_time timestamp without time zone,
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer,
    event_id text,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,     -- TODO @simon@ with the new divulgance model supporting public pkv implementations: we need this to enable NULL-s. Do we need to make involved indexes partial?
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument bytea,

    -- * create events only
    create_signatories text[],
    create_observers text[],
    create_agreement_text text,
    create_key_value bytea,
    create_key_hash bytea,

    -- * exercise events (consuming and non_consuming)
    -- [N/A for this event kind]: exercise_choice text,
    -- [N/A for this event kind]: exercise_argument bytea,
    -- [N/A for this event kind]: exercise_result bytea,
    -- [N/A for this event kind]: exercise_actors text[],
    -- [N/A for this event kind]: exercise_child_event_ids text[],

    --compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT
    -- [N/A for this event kind]: exercise_argument_compression SMALLINT,
    -- [N/A for this event kind]: exercise_result_compression SMALLINT
);
-- these columns contain data that is generally incompressible, so don't try it
ALTER TABLE participant_events_create ALTER COLUMN create_key_hash  SET STORAGE EXTERNAL;

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
CREATE TABLE participant_events_consuming_exercise (
    -- * kinds of events
    -- [N/A for this event kind]: event_kind smallint NOT NULL,
    -- Numbers allocated to leave some space for future additions.
    -- 0:  divulgence event
    -- 10: create event
    -- 20: consuming exercise event
    -- 25: non-consuming exercise event

    -- * event identification
    event_sequential_id bigserial NOT NULL, -- TODO append-only: temporarily readding bigserial for original write paths
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset bytea,

    -- * transaction metadata
    transaction_id text,
    ledger_effective_time timestamp without time zone,
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer,
    event_id text,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,     -- TODO @simon@ with the new divulgance model supporting public pkv implementations: we need this to enable NULL-s. Do we need to make involved indexes partial?
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    -- [N/A for this event kind]: create_argument bytea,

    -- * create events only
    -- [N/A for this event kind]: create_signatories text[],
    -- [N/A for this event kind]: create_observers text[],
    -- [N/A for this event kind]: create_agreement_text text,
    -- [N/A for this event kind]: create_key_value bytea,
    -- [N/A for this event kind]: create_key_hash bytea,

    -- * exercise events (consuming and non_consuming)
    exercise_choice text,
    exercise_argument bytea,
    exercise_result bytea,
    exercise_actors text[],
    exercise_child_event_ids text[],

    --compression flags
    -- [N/A for this event kind]: create_argument_compression SMALLINT,
    -- [N/A for this event kind]: create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);

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
CREATE TABLE participant_events_non_consuming_exercise (
    -- * kinds of events
    -- [N/A for this event kind]: event_kind smallint NOT NULL,
    -- Numbers allocated to leave some space for future additions.
    -- 0:  divulgence event
    -- 10: create event
    -- 20: consuming exercise event
    -- 25: non-consuming exercise event

    -- * event identification
    event_sequential_id bigserial NOT NULL, -- TODO append-only: temporarily readding bigserial for original write paths
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    event_offset bytea,

    -- * transaction metadata
    transaction_id text,
    ledger_effective_time timestamp without time zone,
    command_id text,
    workflow_id text,
    application_id text,
    submitters text[],

    -- * event metadata
    node_index integer,
    event_id text,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text NOT NULL,
    template_id text,     -- TODO @simon@ with the new divulgance model supporting public pkv implementations: we need this to enable NULL-s. Do we need to make involved indexes partial?
    flat_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses text[] DEFAULT '{}'::text[] NOT NULL,       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    -- [N/A for this event kind]: create_argument bytea,

    -- * create events only
    -- [N/A for this event kind]: create_signatories text[],
    -- [N/A for this event kind]: create_observers text[],
    -- [N/A for this event kind]: create_agreement_text text,
    -- [N/A for this event kind]: create_key_value bytea,
    -- [N/A for this event kind]: create_key_hash bytea,

    -- * exercise events (consuming and non_consuming)
    exercise_choice text,
    exercise_argument bytea,
    exercise_result bytea,
    exercise_actors text[],
    exercise_child_event_ids text[],

    --compression flags
    -- [N/A for this event kind]: create_argument_compression SMALLINT,
    -- [N/A for this event kind]: create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);

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
-- Events table: view of all events
---------------------------------------------------------------------------------------------------

/*
Union of all column names:
event_kind,
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
exercise_choice,
exercise_argument,
exercise_result,
exercise_actors,
exercise_child_event_ids,
create_argument_compression,
create_key_value_compression,
exercise_argument_compression,
exercise_result_compression
*/

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
            NULL::bytea as event_offset,
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
            NULL::bytea as create_key_hash,
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
            NULL::bytea as create_key_value,
            NULL::bytea as create_key_hash,
            exercise_choice,
            exercise_argument,
            exercise_result,
            exercise_actors,
            exercise_child_event_ids,
            NULL::smallint as create_argument_compression,
            NULL::smallint as create_key_value_compression,
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
            NULL::bytea as create_key_value,
            NULL::bytea as create_key_hash,
            exercise_choice,
            exercise_argument,
            exercise_result,
            exercise_actors,
            exercise_child_event_ids,
            NULL::smallint as create_argument_compression,
            NULL::smallint as create_key_value_compression,
            exercise_argument_compression,
            exercise_result_compression
        FROM participant_events_non_consuming_exercise
;

---------------------------------------------------------------------------------------------------
-- Indices
---------------------------------------------------------------------------------------------------

-- completions table
--------------------
CREATE INDEX participant_command_completion_offset_application_idx ON participant_command_completions USING btree (completion_offset, application_id, submitters);
