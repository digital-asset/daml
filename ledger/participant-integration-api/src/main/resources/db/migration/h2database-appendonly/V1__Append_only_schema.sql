-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE ALIAS array_intersection FOR "com.daml.platform.store.backend.h2.H2FunctionAliases.arrayIntersection";

---------------------------------------------------------------------------------------------------
-- Parameters table
---------------------------------------------------------------------------------------------------
CREATE TABLE parameters (
  ledger_id VARCHAR NOT NULL,
  participant_id VARCHAR NOT NULL,
  ledger_end VARCHAR,
  ledger_end_sequential_id BIGINT,
  participant_pruned_up_to_inclusive VARCHAR,
  participant_all_divulged_contracts_pruned_up_to_inclusive VARCHAR
);

---------------------------------------------------------------------------------------------------
-- Configurations table
---------------------------------------------------------------------------------------------------
CREATE TABLE configuration_entries (
    ledger_offset VARCHAR PRIMARY KEY NOT NULL,
    recorded_at BIGINT NOT NULL,
    submission_id VARCHAR NOT NULL,
    typ VARCHAR NOT NULL,
    configuration BYTEA NOT NULL,
    rejection_reason VARCHAR,

    CONSTRAINT configuration_entries_check_reason
        CHECK (
          (typ = 'accept' AND rejection_reason IS NULL) OR
          (typ = 'reject' AND rejection_reason IS NOT NULL)
        )
);

CREATE INDEX idx_configuration_submission ON configuration_entries (submission_id);

---------------------------------------------------------------------------------------------------
-- Packages table
---------------------------------------------------------------------------------------------------
CREATE TABLE packages (
    package_id VARCHAR PRIMARY KEY NOT NULL,
    upload_id VARCHAR NOT NULL,
    source_description VARCHAR,
    package_size BIGINT NOT NULL,
    known_since BIGINT NOT NULL,
    ledger_offset VARCHAR NOT NULL,
    package BYTEA NOT NULL
);

CREATE INDEX idx_packages_ledger_offset ON packages (ledger_offset);

---------------------------------------------------------------------------------------------------
-- Package entries table
---------------------------------------------------------------------------------------------------
CREATE TABLE package_entries (
    ledger_offset VARCHAR PRIMARY KEY NOT NULL,
    recorded_at BIGINT NOT NULL,
    submission_id VARCHAR,
    typ VARCHAR NOT NULL,
    rejection_reason VARCHAR,

    CONSTRAINT check_package_entry_type
        CHECK (
          (typ = 'accept' AND rejection_reason IS NULL) OR
          (typ = 'reject' AND rejection_reason IS NOT NULL)
        )
);

CREATE INDEX idx_package_entries ON package_entries (submission_id);

---------------------------------------------------------------------------------------------------
-- Party entries table
---------------------------------------------------------------------------------------------------
CREATE TABLE party_entries (
    ledger_offset VARCHAR PRIMARY KEY NOT NULL,
    recorded_at BIGINT NOT NULL,
    submission_id VARCHAR,
    party VARCHAR,
    display_name VARCHAR,
    typ VARCHAR NOT NULL,
    rejection_reason VARCHAR,
    is_local BOOLEAN,

    CONSTRAINT check_party_entry_type
        CHECK (
          (typ = 'accept' AND rejection_reason IS NULL) OR
          (typ = 'reject' AND rejection_reason IS NOT NULL)
        )
);

CREATE INDEX idx_party_entries ON party_entries (submission_id);
CREATE INDEX idx_party_entries_party_and_ledger_offset ON party_entries(party, ledger_offset);

---------------------------------------------------------------------------------------------------
-- Submissions table
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_command_submissions (
    deduplication_key VARCHAR PRIMARY KEY NOT NULL,
    deduplicate_until BIGINT NOT NULL
);

---------------------------------------------------------------------------------------------------
-- Completions table
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_command_completions (
    completion_offset VARCHAR NOT NULL,
    record_time BIGINT NOT NULL,
    application_id VARCHAR NOT NULL,
    submitters ARRAY NOT NULL,
    command_id VARCHAR NOT NULL,
    -- The transaction ID is `NULL` for rejected transactions.
    transaction_id VARCHAR,
    -- The submission ID will be provided by the participant or driver if the application didn't provide one.
    -- Nullable to support historical data.
    submission_id VARCHAR,
    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the application as one of:
    -- 1. an initial offset
    -- 2. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    -- 3. an initial timestamp
    deduplication_offset VARCHAR,
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

CREATE INDEX participant_command_completion_offset_application_idx ON participant_command_completions (completion_offset, application_id);

---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_divulgence (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL, -- event identification: same ordering as event_offset

    -- * event identification
    event_offset VARCHAR, -- offset of the transaction that divulged the contract

    -- * transaction metadata
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    application_id VARCHAR,
    submitters ARRAY,

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id VARCHAR,
    tree_event_witnesses ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * contract data
    create_argument BYTEA,

    -- * compression flags
    create_argument_compression SMALLINT
);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_divulgence_event_offset ON participant_events_divulgence (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_divulgence_event_sequential_id ON participant_events_divulgence (event_sequential_id);

-- filtering by template
CREATE INDEX participant_events_divulgence_template_id_idx ON participant_events_divulgence (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_divulgence_tree_event_witnesses_idx ON participant_events_divulgence (tree_event_witnesses);

-- lookup divulgance events, in order of ingestion
CREATE INDEX participant_events_divulgence_contract_id_idx ON participant_events_divulgence (contract_id, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_create (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint NOT NULL,    -- transaction metadata
    node_index integer NOT NULL,              -- event metadata

    -- * event identification
    event_offset VARCHAR NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    application_id VARCHAR,
    submitters ARRAY,

    -- * event metadata
    event_id VARCHAR NOT NULL,       -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id VARCHAR NOT NULL,
    flat_event_witnesses ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * contract data
    create_argument BYTEA NOT NULL,
    create_signatories ARRAY NOT NULL,
    create_observers ARRAY NOT NULL,
    create_agreement_text VARCHAR,
    create_key_value BYTEA,
    create_key_hash VARCHAR,

    -- * compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT
);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_create_event_offset ON participant_events_create (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_create_event_sequential_id ON participant_events_create (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_create_event_id_idx ON participant_events_create (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_create_transaction_id_idx ON participant_events_create (transaction_id);

-- filtering by template
CREATE INDEX participant_events_create_template_id_idx ON participant_events_create (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_create_flat_event_witnesses_idx ON participant_events_create (flat_event_witnesses);
CREATE INDEX participant_events_create_tree_event_witnesses_idx ON participant_events_create (tree_event_witnesses);

-- lookup by contract id
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create (contract_id);

-- lookup by contract_key
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create (create_key_hash, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint NOT NULL,    -- transaction metadata
    node_index integer NOT NULL,              -- event metadata

    -- * event identification
    event_offset VARCHAR NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    application_id VARCHAR,
    submitters ARRAY,

    -- * event metadata
    event_id VARCHAR NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id VARCHAR NOT NULL,
    flat_event_witnesses ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * information about the corresponding create event
    create_key_value BYTEA,        -- used for the mutable state cache

    -- * choice data
    exercise_choice VARCHAR NOT NULL,
    exercise_argument BYTEA NOT NULL,
    exercise_result BYTEA,
    exercise_actors ARRAY NOT NULL,
    exercise_child_event_ids ARRAY NOT NULL,

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_consuming_exercise_event_offset ON participant_events_consuming_exercise (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_consuming_exercise_event_sequential_id ON participant_events_consuming_exercise (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_consuming_exercise_event_id_idx ON participant_events_consuming_exercise (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_consuming_exercise_transaction_id_idx ON participant_events_consuming_exercise (transaction_id);

-- filtering by template
CREATE INDEX participant_events_consuming_exercise_template_id_idx ON participant_events_consuming_exercise (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
CREATE INDEX participant_events_consuming_exercise_flat_event_witnesses_idx ON participant_events_consuming_exercise (flat_event_witnesses);
CREATE INDEX participant_events_consuming_exercise_tree_event_witnesses_idx ON participant_events_consuming_exercise (tree_event_witnesses);

-- lookup by contract id
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise (contract_id);

---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_non_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint NOT NULL,    -- transaction metadata
    node_index integer NOT NULL,              -- event metadata

    -- * event identification
    event_offset VARCHAR NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    application_id VARCHAR,
    submitters ARRAY,

    -- * event metadata
    event_id VARCHAR NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id VARCHAR NOT NULL,
    flat_event_witnesses ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * information about the corresponding create event
    create_key_value BYTEA,        -- used for the mutable state cache

    -- * choice data
    exercise_choice VARCHAR NOT NULL,
    exercise_argument BYTEA NOT NULL,
    exercise_result BYTEA,
    exercise_actors ARRAY NOT NULL,
    exercise_child_event_ids ARRAY NOT NULL,

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT
);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_non_consuming_exercise_event_offset ON participant_events_non_consuming_exercise (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_non_consuming_exercise_event_sequential_id ON participant_events_non_consuming_exercise (event_sequential_id);

-- lookup by event-id
CREATE INDEX participant_events_non_consuming_exercise_event_id_idx ON participant_events_non_consuming_exercise (event_id);

-- lookup by transaction id
CREATE INDEX participant_events_non_consuming_exercise_transaction_id_idx ON participant_events_non_consuming_exercise (transaction_id);

-- filtering by template
CREATE INDEX participant_events_non_consuming_exercise_template_id_idx ON participant_events_non_consuming_exercise (template_id);

-- filtering by witnesses (visibility) for some queries used in the implementation of
-- GetActiveContracts (flat), GetTransactions (flat) and GetTransactionTrees.
-- Note that Potsgres has trouble using these indices effectively with our paged access.
-- We might decide to drop them.
-- NOTE: index name truncated because the full name exceeds the 63 characters length limit
CREATE INDEX participant_events_non_consuming_exercise_flat_event_witnes_idx ON participant_events_non_consuming_exercise (flat_event_witnesses);
CREATE INDEX participant_events_non_consuming_exercise_tree_event_witnes_idx ON participant_events_non_consuming_exercise (tree_event_witnesses);

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
-- is not negatively affected by a long list of columns that are never used.
CREATE VIEW participant_events
AS
SELECT
    0::smallint as event_kind,
    event_sequential_id,
    NULL::VARCHAR as event_offset,
    NULL::VARCHAR as transaction_id,
    NULL::bigint as ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    NULL::integer as node_index,
    NULL::VARCHAR as event_id,
    contract_id,
    template_id,
    NULL::ARRAY as flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    NULL::ARRAY as create_signatories,
    NULL::ARRAY as create_observers,
    NULL::VARCHAR as create_agreement_text,
    NULL::BYTEA as create_key_value,
    NULL::VARCHAR as create_key_hash,
    NULL::VARCHAR as exercise_choice,
    NULL::BYTEA as exercise_argument,
    NULL::BYTEA as exercise_result,
    NULL::ARRAY as exercise_actors,
    NULL::ARRAY as exercise_child_event_ids,
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
    NULL::VARCHAR as exercise_choice,
    NULL::BYTEA as exercise_argument,
    NULL::BYTEA as exercise_result,
    NULL::ARRAY as exercise_actors,
    NULL::ARRAY as exercise_child_event_ids,
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
    NULL::BYTEA as create_argument,
    NULL::ARRAY as create_signatories,
    NULL::ARRAY as create_observers,
    NULL::VARCHAR as create_agreement_text,
    create_key_value,
    NULL::VARCHAR as create_key_hash,
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
    NULL::BYTEA as create_argument,
    NULL::ARRAY as create_signatories,
    NULL::ARRAY as create_observers,
    NULL::VARCHAR as create_agreement_text,
    create_key_value,
    NULL::VARCHAR as create_key_hash,
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
