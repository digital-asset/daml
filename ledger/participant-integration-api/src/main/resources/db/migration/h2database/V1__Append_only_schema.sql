-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE ALIAS array_intersection FOR "com.daml.platform.store.backend.h2.H2FunctionAliases.arrayIntersection";

---------------------------------------------------------------------------------------------------
-- Parameters table
---------------------------------------------------------------------------------------------------
CREATE TABLE parameters (
  ledger_id VARCHAR NOT NULL,
  participant_id VARCHAR NOT NULL,
  ledger_end VARCHAR NOT NULL,
  ledger_end_sequential_id BIGINT NOT NULL,
  ledger_end_string_interning_id INTEGER NOT NULL,
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

CREATE INDEX participant_command_completions_application_id_offset_idx ON participant_command_completions USING btree (application_id, completion_offset);

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
    create_key_value_compression SMALLINT,

    -- * contract driver metadata
    driver_metadata BINARY LARGE OBJECT
);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_create_event_offset ON participant_events_create (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_create_event_sequential_id ON participant_events_create (event_sequential_id);

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

CREATE TABLE string_interning (
    internal_id integer PRIMARY KEY NOT NULL,
    external_string text
);

-----------------------------
-- Filter tables for events
-----------------------------

-- create stakeholders
CREATE TABLE participant_events_create_filter (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL
);
CREATE INDEX idx_participant_events_create_filter_party_template_seq_id_idx ON participant_events_create_filter(party_id, template_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_filter_party_seq_id_idx ON participant_events_create_filter(party_id, event_sequential_id);
CREATE INDEX idx_participant_events_create_seq_id_idx ON participant_events_create_filter(event_sequential_id);

CREATE TABLE pe_create_filter_nonstakeholder_informees (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_create_filter_nonstakeholder_informees_ps_idx ON pe_create_filter_nonstakeholder_informees(party_id, event_sequential_id);
CREATE INDEX pe_create_filter_nonstakeholder_informees_s_idx ON pe_create_filter_nonstakeholder_informees(event_sequential_id);

CREATE TABLE pe_consuming_exercise_filter_stakeholders (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_pts_idx ON pe_consuming_exercise_filter_stakeholders(party_id, template_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_ps_idx  ON pe_consuming_exercise_filter_stakeholders(party_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_s_idx   ON pe_consuming_exercise_filter_stakeholders(event_sequential_id);

CREATE TABLE pe_consuming_exercise_filter_nonstakeholder_informees (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_consuming_exercise_filter_nonstakeholder_informees_ps_idx ON pe_consuming_exercise_filter_nonstakeholder_informees(party_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_nonstakeholder_informees_s_idx ON pe_consuming_exercise_filter_nonstakeholder_informees(event_sequential_id);

CREATE TABLE pe_non_consuming_exercise_filter_informees (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_non_consuming_exercise_filter_informees_ps_idx ON pe_non_consuming_exercise_filter_informees(party_id, event_sequential_id);
CREATE INDEX pe_non_consuming_exercise_filter_informees_s_idx ON pe_non_consuming_exercise_filter_informees(event_sequential_id);


CREATE TABLE participant_transaction_meta(
    transaction_id VARCHAR NOT NULL,
    event_offset VARCHAR NOT NULL,
    event_sequential_id_from BIGINT NOT NULL,
    event_sequential_id_to BIGINT NOT NULL
);
CREATE INDEX participant_transaction_meta_tid_idx ON participant_transaction_meta(transaction_id);
CREATE INDEX participant_transaction_meta_eventoffset_idx ON participant_transaction_meta(event_offset);


CREATE TABLE transaction_metering (
    application_id VARCHAR NOT NULL,
    action_count INTEGER NOT NULL,
    metering_timestamp BIGINT NOT NULL,
    ledger_offset VARCHAR NOT NULL
);

CREATE INDEX transaction_metering_ledger_offset ON transaction_metering(ledger_offset);

CREATE TABLE metering_parameters (
    ledger_metering_end VARCHAR,
    ledger_metering_timestamp BIGINT NOT NULL
);

CREATE TABLE participant_metering (
    application_id VARCHAR NOT NULL,
    from_timestamp BIGINT NOT NULL,
    to_timestamp BIGINT NOT NULL,
    action_count INTEGER NOT NULL,
    ledger_offset VARCHAR NOT NULL
);

CREATE UNIQUE INDEX participant_metering_from_to_application ON participant_metering(from_timestamp, to_timestamp, application_id);


-- NOTE: We keep participant user and party record tables independent from indexer-based tables, such that
--       we maintain a property that they can be moved to a separate database without any extra schema changes.
---------------------------------------------------------------------------------------------------
-- Participant local store: users
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_users (
    internal_id         INTEGER             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id             VARCHAR(256)        NOT NULL UNIQUE,
    primary_party       VARCHAR(512),
    is_deactivated      BOOLEAN             NOT NULL,
    resource_version    BIGINT              NOT NULL,
    created_at          BIGINT              NOT NULL
);
CREATE TABLE participant_user_rights (
    user_internal_id    INTEGER             NOT NULL REFERENCES participant_users (internal_id) ON DELETE CASCADE,
    user_right          INTEGER             NOT NULL,
    for_party           VARCHAR(512),
    for_party2          VARCHAR(512)        GENERATED ALWAYS AS (CASE
                                                                        WHEN for_party IS NOT NULL
                                                                        THEN for_party
                                                                        ELSE ''
                                                                 END),
    granted_at          BIGINT              NOT NULL,
    UNIQUE (user_internal_id, user_right, for_party2)
);
CREATE TABLE participant_user_annotations (
    internal_id         INTEGER             NOT NULL REFERENCES participant_users (internal_id) ON DELETE CASCADE,
    name                VARCHAR(512)        NOT NULL,
    -- 256k = 256*1024 = 262144
    val                 VARCHAR(262144),
    updated_at          BIGINT              NOT NULL,
    UNIQUE (internal_id, name)
);
INSERT INTO participant_users(user_id, primary_party, is_deactivated, resource_version, created_at)
    VALUES ('participant_admin', NULL, false, 0,  0);
INSERT INTO participant_user_rights(user_internal_id, user_right, for_party, granted_at)
    SELECT internal_id, 1, NULL, 0
    FROM participant_users
    WHERE user_id = 'participant_admin';

---------------------------------------------------------------------------------------------------
-- Participant local store: party records
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_party_records (
    internal_id         INTEGER             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    party               VARCHAR(512)        NOT NULL UNIQUE,
    resource_version    BIGINT              NOT NULL,
    created_at          BIGINT              NOT NULL
);
CREATE TABLE participant_party_record_annotations (
    internal_id         INTEGER             NOT NULL REFERENCES participant_party_records (internal_id) ON DELETE CASCADE,
    name                VARCHAR(512)        NOT NULL,
    -- 256k = 256*1024 = 262144
    val                 VARCHAR(262144),
    updated_at          BIGINT              NOT NULL,
    UNIQUE (internal_id, name)
);
