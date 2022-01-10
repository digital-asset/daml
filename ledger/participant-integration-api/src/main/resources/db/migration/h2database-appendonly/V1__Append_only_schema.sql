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
  ledger_end_string_interning_id INTEGER,
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
    configuration BINARY LARGE OBJECT NOT NULL,
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
    package BINARY LARGE OBJECT NOT NULL
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
    party_id INTEGER,

    CONSTRAINT check_party_entry_type
        CHECK (
          (typ = 'accept' AND rejection_reason IS NULL) OR
          (typ = 'reject' AND rejection_reason IS NOT NULL)
        )
);

CREATE INDEX idx_party_entries ON party_entries (submission_id);
CREATE INDEX idx_party_entries_party_and_ledger_offset ON party_entries(party, ledger_offset);
CREATE INDEX idx_party_entries_party_id_and_ledger_offset ON party_entries(party_id, ledger_offset);

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
    submitters INTEGER ARRAY NOT NULL,
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
    rejection_status_details BINARY LARGE OBJECT
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
    submitters INTEGER ARRAY,

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id INTEGER,
    tree_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * contract data
    create_argument BINARY LARGE OBJECT,

    -- * compression flags
    create_argument_compression SMALLINT
);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_divulgence_event_offset ON participant_events_divulgence (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_divulgence_event_sequential_id ON participant_events_divulgence (event_sequential_id);

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
    submitters INTEGER ARRAY,

    -- * event metadata
    event_id VARCHAR NOT NULL,       -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * contract data
    create_argument BINARY LARGE OBJECT NOT NULL,
    create_signatories INTEGER ARRAY NOT NULL,
    create_observers INTEGER ARRAY NOT NULL,
    create_agreement_text VARCHAR,
    create_key_value BINARY LARGE OBJECT,
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
    submitters INTEGER ARRAY,

    -- * event metadata
    event_id VARCHAR NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * information about the corresponding create event
    create_key_value BINARY LARGE OBJECT,        -- used for the mutable state cache

    -- * choice data
    exercise_choice VARCHAR NOT NULL,
    exercise_argument BINARY LARGE OBJECT NOT NULL,
    exercise_result BINARY LARGE OBJECT,
    exercise_actors INTEGER ARRAY NOT NULL,
    exercise_child_event_ids VARCHAR ARRAY NOT NULL,

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
    submitters INTEGER ARRAY,

    -- * event metadata
    event_id VARCHAR NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR NOT NULL,
    template_id INTEGER NOT NULL,
    flat_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * information about the corresponding create event
    create_key_value BINARY LARGE OBJECT,        -- used for the mutable state cache

    -- * choice data
    exercise_choice VARCHAR NOT NULL,
    exercise_argument BINARY LARGE OBJECT NOT NULL,
    exercise_result BINARY LARGE OBJECT,
    exercise_actors INTEGER ARRAY NOT NULL,
    exercise_child_event_ids VARCHAR ARRAY NOT NULL,

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
    NULL::INTEGER ARRAY as flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    NULL::INTEGER ARRAY as create_signatories,
    NULL::INTEGER ARRAY as create_observers,
    NULL::VARCHAR as create_agreement_text,
    NULL::BINARY LARGE OBJECT as create_key_value,
    NULL::VARCHAR as create_key_hash,
    NULL::VARCHAR as exercise_choice,
    NULL::BINARY LARGE OBJECT as exercise_argument,
    NULL::BINARY LARGE OBJECT as exercise_result,
    NULL::INTEGER ARRAY as exercise_actors,
    NULL::VARCHAR ARRAY as exercise_child_event_ids,
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
    NULL::BINARY LARGE OBJECT as exercise_argument,
    NULL::BINARY LARGE OBJECT as exercise_result,
    NULL::INTEGER ARRAY as exercise_actors,
    NULL::VARCHAR ARRAY as exercise_child_event_ids,
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
    NULL::BINARY LARGE OBJECT as create_argument,
    NULL::INTEGER ARRAY as create_signatories,
    NULL::INTEGER ARRAY as create_observers,
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
    NULL::BINARY LARGE OBJECT as create_argument,
    NULL::INTEGER ARRAY as create_signatories,
    NULL::INTEGER ARRAY as create_observers,
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

CREATE TABLE string_interning (
    internal_id integer PRIMARY KEY NOT NULL,
    external_string text
);

CREATE TABLE participant_events_create_filter (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL
);

CREATE INDEX idx_participant_events_create_filter_party_template_seq_id_idx ON participant_events_create_filter(party_id, template_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_filter_party_seq_id_idx ON participant_events_create_filter(party_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_seq_id_idx ON participant_events_create_filter(event_sequential_id);
