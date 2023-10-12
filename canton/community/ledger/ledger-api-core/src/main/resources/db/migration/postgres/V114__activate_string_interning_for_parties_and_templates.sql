--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

SELECT 'Interning data migration - Preparation...';

-- add temporary index to aid string lookups as migrating data
CREATE UNIQUE INDEX string_interning_external_string_temp_idx ON string_interning USING btree (external_string);

-- drop participant_events view (enables column type changes)
DROP VIEW participant_events;

-- disable hash and merge join to bias the planner to use lookups against the interning table
SET enable_hashjoin = off;
SET enable_mergejoin = off;





SELECT 'Interning data migration - Migrating party_entries...';

-- add party_id to party_entries
ALTER TABLE party_entries
  ADD COLUMN party_id INTEGER;

-- populate internal party id-s
UPDATE party_entries
  SET party_id = string_interning.internal_id
FROM string_interning
WHERE string_interning.external_string = 'p|' || party_entries.party;

-- add index on party_id
CREATE INDEX idx_party_entries_party_id_and_ledger_offset ON party_entries(party_id, ledger_offset);





SELECT 'Interning data migration - Migrating participant_command_completions...';

-- create new table with temporary name
CREATE TABLE participant_command_completions_interned (
    completion_offset VARCHAR NOT NULL,
    record_time BIGINT NOT NULL,
    application_id VARCHAR NOT NULL,
    submitters INTEGER[] NOT NULL,
    command_id VARCHAR NOT NULL,
    -- The transaction ID is `NULL` for rejected transactions.
    transaction_id VARCHAR,
    -- The submission ID will be provided by the participant or driver if the application didn't provide one.
    -- Nullable to support historical data.
    submission_id TEXT,
    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the application as one of:
    -- 1. an initial offset
    -- 2. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    -- 3. an initial timestamp
    deduplication_offset TEXT,
    deduplication_duration_seconds BIGINT,
    deduplication_duration_nanos INT,
    deduplication_start BIGINT,
    -- The three columns below are `NULL` if the completion is for an accepted transaction.
    -- The `rejection_status_details` column contains a Protocol-Buffers-serialized message of type
    -- `daml.platform.index.StatusDetails`, containing the code, message, and further details
    -- (decided by the ledger driver), and may be `NULL` even if the other two columns are set.
    rejection_status_code INTEGER,
    rejection_status_message VARCHAR,
    rejection_status_details BYTEA
);

SELECT 'DEBUG populate new table...';

-- migrate data
INSERT INTO participant_command_completions_interned
SELECT
    completion_offset,
    record_time,
    application_id,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(submitters) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS submitters,
    command_id,
    transaction_id,
    submission_id,
    deduplication_offset,
    deduplication_duration_seconds,
    deduplication_duration_nanos,
    deduplication_start,
    rejection_status_code,
    rejection_status_message,
    rejection_status_details
FROM participant_command_completions;

SELECT 'DEBUG add indexes...';

-- rename old indexes
ALTER INDEX participant_command_completion_offset_application_idx RENAME TO participant_command_completion_offset_application_idx_old;

-- add index(es)
CREATE INDEX participant_command_completion_offset_application_idx ON participant_command_completions_interned USING btree (completion_offset, application_id);

SELECT 'DEBUG replace original table...';

-- replace original table
DROP TABLE participant_command_completions CASCADE;
ALTER TABLE participant_command_completions_interned RENAME TO participant_command_completions;





SELECT 'Interning data migration - Migrating participant_events_divulgence...';

-- create new table with temporary name
CREATE TABLE participant_events_divulgence_interned (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL, -- event identification: same ordering as event_offset
    -- * event identification
    event_offset TEXT, -- offset of the transaction that divulged the contract
    -- * transaction metadata
    workflow_id TEXT,
    -- * submitter info (only visible on submitting participant)
    command_id TEXT,
    application_id TEXT,
    submitters INTEGER[],
    -- * shared event information
    contract_id TEXT NOT NULL,
    template_id INTEGER,
    tree_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- informees
    -- * contract data
    create_argument BYTEA,
    -- * compression flags
    create_argument_compression SMALLINT
);

SELECT 'DEBUG populate new table...';

-- migrate data
INSERT INTO participant_events_divulgence_interned
SELECT
    event_sequential_id,
    event_offset,
    workflow_id,
    command_id,
    application_id,
    CASE WHEN submitters IS NULL THEN NULL ELSE
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(submitters) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) END AS submitters,
    contract_id,
    string_interning.internal_id AS template_id,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(tree_event_witnesses) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS tree_event_witnesses,
    create_argument,
    create_argument_compression
FROM participant_events_divulgence
LEFT OUTER JOIN string_interning
ON ('t|' || participant_events_divulgence.template_id = string_interning.external_string);

SELECT 'DEBUG add indexes...';

-- rename old indexes
ALTER INDEX participant_events_divulgence_event_offset RENAME TO participant_events_divulgence_event_offset_old;
ALTER INDEX participant_events_divulgence_event_sequential_id RENAME TO participant_events_divulgence_event_sequential_id_old;
ALTER INDEX participant_events_divulgence_contract_id_idx RENAME TO participant_events_divulgence_contract_id_idx_old;

-- add index(es)
CREATE INDEX participant_events_divulgence_event_offset ON participant_events_divulgence_interned USING btree (event_offset);
CREATE INDEX participant_events_divulgence_event_sequential_id ON participant_events_divulgence_interned USING btree (event_sequential_id);
CREATE INDEX participant_events_divulgence_contract_id_idx ON participant_events_divulgence_interned USING btree (contract_id, event_sequential_id);

SELECT 'DEBUG replace original table...';

-- replace original table
DROP TABLE participant_events_divulgence CASCADE;
ALTER TABLE participant_events_divulgence_interned RENAME TO participant_events_divulgence;





SELECT 'Interning data migration - Migrating participant_events_create...';

-- create new table with temporary name
CREATE TABLE participant_events_create_interned (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time BIGINT NOT NULL, -- transaction metadata
    node_index INTEGER NOT NULL,              -- event metadata
    -- * event identification
    event_offset TEXT NOT NULL,
    -- * transaction metadata
    transaction_id TEXT NOT NULL,
    workflow_id TEXT,
    -- * submitter info (only visible on submitting participant)
    command_id TEXT,
    application_id TEXT,
    submitters INTEGER[],
    -- * event metadata
    event_id TEXT NOT NULL,       -- string representation of (transaction_id, node_index)
    -- * shared event information
    contract_id TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- stakeholders
    tree_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- informees
    -- * contract data
    create_argument bytea NOT NULL,
    create_signatories INTEGER[] NOT NULL,
    create_observers INTEGER[] NOT NULL,
    create_agreement_text TEXT,
    create_key_value BYTEA,
    create_key_hash TEXT,
    -- * compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT
);

SELECT 'DEBUG populate new table...';

-- migrate data
INSERT INTO participant_events_create_interned
SELECT
    event_sequential_id,
    ledger_effective_time,
    node_index,
    event_offset,
    transaction_id,
    workflow_id,
    command_id,
    application_id,
    CASE WHEN submitters IS NULL THEN NULL ELSE COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(submitters) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) END AS submitters,
    event_id,
    contract_id,
    string_interning.internal_id AS template_id,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(flat_event_witnesses) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS flat_event_witnesses,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(tree_event_witnesses) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS tree_event_witnesses,
    create_argument,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(create_signatories) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS create_signatories,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(create_observers) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS create_observers,
    create_agreement_text,
    create_key_value,
    create_key_hash,
    create_argument_compression,
    create_key_value_compression
FROM participant_events_create
LEFT OUTER JOIN string_interning
ON ('t|' || participant_events_create.template_id = string_interning.external_string);

SELECT 'DEBUG add indexes...';

-- rename old indexes
ALTER INDEX participant_events_create_event_offset RENAME TO participant_events_create_event_offset_old;
ALTER INDEX participant_events_create_event_sequential_id RENAME TO participant_events_create_event_sequential_id_old;
ALTER INDEX participant_events_create_event_id_idx RENAME TO participant_events_create_event_id_idx_old;
ALTER INDEX participant_events_create_transaction_id_idx RENAME TO participant_events_create_transaction_id_idx_old;
ALTER INDEX participant_events_create_contract_id_idx RENAME TO participant_events_create_contract_id_idx_old;
ALTER INDEX participant_events_create_create_key_hash_idx RENAME TO participant_events_create_create_key_hash_idx_old;

-- add index(es)
CREATE INDEX participant_events_create_event_offset ON participant_events_create_interned USING btree (event_offset);
CREATE INDEX participant_events_create_event_sequential_id ON participant_events_create_interned USING btree (event_sequential_id);
CREATE INDEX participant_events_create_event_id_idx ON participant_events_create_interned USING btree (event_id);
CREATE INDEX participant_events_create_transaction_id_idx ON participant_events_create_interned USING btree (transaction_id);
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create_interned USING hash (contract_id);
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create_interned USING btree (create_key_hash, event_sequential_id);

SELECT 'DEBUG replace original table...';

-- replace original table
DROP TABLE participant_events_create CASCADE;
ALTER TABLE participant_events_create_interned RENAME TO participant_events_create;





SELECT 'Interning data migration - Migrating participant_events_consuming_exercise...';

-- create new table with temporary name
CREATE TABLE participant_events_consuming_exercise_interned (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time BIGINT NOT NULL, -- transaction metadata
    node_index INTEGER NOT NULL,              -- event metadata
    -- * event identification
    event_offset TEXT NOT NULL,
    -- * transaction metadata
    transaction_id TEXT NOT NULL,
    workflow_id TEXT,
    -- * submitter info (only visible on submitting participant)
    command_id TEXT,
    application_id TEXT,
    submitters INTEGER[],
    -- * event metadata
    event_id TEXT NOT NULL,        -- string representation of (transaction_id, node_index)
    -- * shared event information
    contract_id TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- stakeholders
    tree_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- informees
    -- * information about the corresponding create event
    create_key_value BYTEA,        -- used for the mutable state cache
    -- * choice data
    exercise_choice TEXT NOT NULL,
    exercise_argument bytea NOT NULL,
    exercise_result BYTEA,
    exercise_actors INTEGER[] NOT NULL,
    exercise_child_event_ids TEXT[] NOT NULL,
    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);

SELECT 'DEBUG populate new table...';

-- migrate data
INSERT INTO participant_events_consuming_exercise_interned
SELECT
    event_sequential_id,
    ledger_effective_time,
    node_index,
    event_offset,
    transaction_id,
    workflow_id,
    command_id,
    application_id,
    CASE WHEN submitters IS NULL THEN NULL ELSE COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(submitters) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) END AS submitters,
    event_id,
    contract_id,
    string_interning.internal_id AS template_id,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(flat_event_witnesses) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS flat_event_witnesses,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(tree_event_witnesses) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS tree_event_witnesses,
    create_key_value,
    exercise_choice,
    exercise_argument,
    exercise_result,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(exercise_actors) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS exercise_actors,
    exercise_child_event_ids,
    create_key_value_compression,
    exercise_argument_compression,
    exercise_result_compression
FROM participant_events_consuming_exercise
LEFT OUTER JOIN string_interning
ON ('t|' || participant_events_consuming_exercise.template_id = string_interning.external_string);

SELECT 'DEBUG add indexes...';

-- rename old indexes
ALTER INDEX participant_events_consuming_exercise_event_offset RENAME TO participant_events_consuming_exercise_event_offset_old;
ALTER INDEX participant_events_consuming_exercise_event_sequential_id RENAME TO participant_events_consuming_exercise_event_sequential_id_old;
ALTER INDEX participant_events_consuming_exercise_event_id_idx RENAME TO participant_events_consuming_exercise_event_id_idx_old;
ALTER INDEX participant_events_consuming_exercise_transaction_id_idx RENAME TO participant_events_consuming_exercise_transaction_id_idx_old;
ALTER INDEX participant_events_consuming_exercise_contract_id_idx RENAME TO participant_events_consuming_exercise_contract_id_idx_old;

-- add index(es)
CREATE INDEX participant_events_consuming_exercise_event_offset ON participant_events_consuming_exercise_interned USING btree (event_offset);
CREATE INDEX participant_events_consuming_exercise_event_sequential_id ON participant_events_consuming_exercise_interned USING btree (event_sequential_id);
CREATE INDEX participant_events_consuming_exercise_event_id_idx ON participant_events_consuming_exercise_interned USING btree (event_id);
CREATE INDEX participant_events_consuming_exercise_transaction_id_idx ON participant_events_consuming_exercise_interned USING btree (transaction_id);
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise_interned USING hash (contract_id);

SELECT 'DEBUG replace original table...';

-- replace original table
DROP TABLE participant_events_consuming_exercise CASCADE;
ALTER TABLE participant_events_consuming_exercise_interned RENAME TO participant_events_consuming_exercise;





SELECT 'Interning data migration - Migrating participant_events_non_consuming_exercise...';

-- create new table with temporary name
CREATE TABLE participant_events_non_consuming_exercise_interned (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time BIGINT NOT NULL, -- transaction metadata
    node_index INTEGER NOT NULL,              -- event metadata
    -- * event identification
    event_offset TEXT NOT NULL,
    -- * transaction metadata
    transaction_id TEXT NOT NULL,
    workflow_id TEXT,
    -- * submitter info (only visible on submitting participant)
    command_id TEXT,
    application_id TEXT,
    submitters INTEGER[],
    -- * event metadata
    event_id TEXT NOT NULL,        -- string representation of (transaction_id, node_index)
    -- * shared event information
    contract_id TEXT NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- stakeholders
    tree_event_witnesses INTEGER[] DEFAULT '{}'::INTEGER[] NOT NULL, -- informees
    -- * information about the corresponding create event
    create_key_value BYTEA,        -- used for the mutable state cache
    -- * choice data
    exercise_choice TEXT NOT NULL,
    exercise_argument bytea NOT NULL,
    exercise_result BYTEA,
    exercise_actors INTEGER[] NOT NULL,
    exercise_child_event_ids TEXT[] NOT NULL,
    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);

SELECT 'DEBUG populate new table...';

-- migrate data
INSERT INTO participant_events_non_consuming_exercise_interned
SELECT
    event_sequential_id,
    ledger_effective_time,
    node_index,
    event_offset,
    transaction_id,
    workflow_id,
    command_id,
    application_id,
    CASE WHEN submitters IS NULL THEN NULL ELSE COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(submitters) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) END AS submitters,
    event_id,
    contract_id,
    string_interning.internal_id AS template_id,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(flat_event_witnesses) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS flat_event_witnesses,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(tree_event_witnesses) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS tree_event_witnesses,
    create_key_value,
    exercise_choice,
    exercise_argument,
    exercise_result,
    COALESCE(
        (
            SELECT array_agg(string_interning.internal_id)
            FROM unnest(exercise_actors) string_array(external_string)
            INNER JOIN string_interning
            ON (string_interning.external_string = 'p|' || string_array.external_string)
        ),
        ARRAY[]::INTEGER[]
    ) AS exercise_actors,
    exercise_child_event_ids,
    create_key_value_compression,
    exercise_argument_compression,
    exercise_result_compression
FROM participant_events_non_consuming_exercise
LEFT OUTER JOIN string_interning
ON ('t|' || participant_events_non_consuming_exercise.template_id = string_interning.external_string);

SELECT 'DEBUG add indexes...';

-- rename old indexes
ALTER INDEX participant_events_non_consuming_exercise_event_offset RENAME TO participant_events_non_consuming_exercise_event_offset_old;
ALTER INDEX participant_events_non_consuming_exercise_event_sequential_id RENAME TO participant_events_non_consuming_exercise_event_sequential_id_old;
ALTER INDEX participant_events_non_consuming_exercise_event_id_idx RENAME TO participant_events_non_consuming_exercise_event_id_idx_old;
ALTER INDEX participant_events_non_consuming_exercise_transaction_id_idx RENAME TO participant_events_non_consuming_exercise_transaction_id_idx_old;

-- add index(es)
CREATE INDEX participant_events_non_consuming_exercise_event_offset ON participant_events_non_consuming_exercise_interned USING btree (event_offset);
CREATE INDEX participant_events_non_consuming_exercise_event_sequential_id ON participant_events_non_consuming_exercise_interned USING btree (event_sequential_id);
CREATE INDEX participant_events_non_consuming_exercise_event_id_idx ON participant_events_non_consuming_exercise_interned USING btree (event_id);
CREATE INDEX participant_events_non_consuming_exercise_transaction_id_idx ON participant_events_non_consuming_exercise_interned USING btree (transaction_id);

SELECT 'DEBUG replace original table...';

-- replace original table
DROP TABLE participant_events_non_consuming_exercise CASCADE;
ALTER TABLE participant_events_non_consuming_exercise_interned RENAME TO participant_events_non_consuming_exercise;





SELECT 'Interning data migration - Cleanup...';

-- drop temporary db objects
DROP INDEX string_interning_external_string_temp_idx;

-- reenable hash and merge joins
SET enable_hashjoin = on;
SET enable_mergejoin = on;

-- re-add the participant_events view
CREATE VIEW participant_events
AS
SELECT
    0::smallint as event_kind,
    event_sequential_id,
    NULL::text as event_offset,
    NULL::text as transaction_id,
    NULL::bigint as ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    NULL::integer as node_index,
    NULL::text as event_id,
    contract_id,
    template_id,
    NULL::INTEGER[] as flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    NULL::INTEGER[] as create_signatories,
    NULL::INTEGER[] as create_observers,
    NULL::text as create_agreement_text,
    NULL::bytea as create_key_value,
    NULL::text as create_key_hash,
    NULL::text as exercise_choice,
    NULL::bytea as exercise_argument,
    NULL::bytea as exercise_result,
    NULL::INTEGER[] as exercise_actors,
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
    NULL::INTEGER[] as exercise_actors,
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
    NULL::INTEGER[] as create_signatories,
    NULL::INTEGER[] as create_observers,
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
    NULL::INTEGER[] as create_signatories,
    NULL::INTEGER[] as create_observers,
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
FROM participant_events_non_consuming_exercise;



SELECT 'Interning data migration - Done';
