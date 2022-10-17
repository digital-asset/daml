-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create ALIAS array_intersection FOR "com.daml.platform.store.backend.h2.H2FunctionAliases.arrayIntersection";

---------------------------------------------------------------------------------------------------
-- Parameters table
---------------------------------------------------------------------------------------------------
create TABLE parameters (
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
create TABLE configuration_entries (
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

create index idx_configuration_submission on configuration_entries (submission_id);

---------------------------------------------------------------------------------------------------
-- Packages table
---------------------------------------------------------------------------------------------------
create TABLE packages (
    package_id VARCHAR PRIMARY KEY NOT NULL,
    upload_id VARCHAR NOT NULL,
    source_description VARCHAR,
    package_size BIGINT NOT NULL,
    known_since BIGINT NOT NULL,
    ledger_offset VARCHAR NOT NULL,
    package BINARY LARGE OBJECT NOT NULL
);

create index idx_packages_ledger_offset on packages (ledger_offset);

---------------------------------------------------------------------------------------------------
-- Package entries table
---------------------------------------------------------------------------------------------------
create TABLE package_entries (
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

create index idx_package_entries on package_entries (submission_id);

---------------------------------------------------------------------------------------------------
-- Party entries table
---------------------------------------------------------------------------------------------------
create TABLE party_entries (
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

create index idx_party_entries on party_entries (submission_id);
create index idx_party_entries_party_and_ledger_offset on party_entries(party, ledger_offset);
create index idx_party_entries_party_id_and_ledger_offset on party_entries(party_id, ledger_offset);

---------------------------------------------------------------------------------------------------
-- Completions table
---------------------------------------------------------------------------------------------------
create TABLE participant_command_completions (
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

create index participant_command_completion_offset_application_idx on participant_command_completions (completion_offset, application_id);

---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------
create TABLE participant_events_divulgence (
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
create index participant_events_divulgence_event_offset on participant_events_divulgence (event_offset);

-- sequential_id index for paging
create index participant_events_divulgence_event_sequential_id on participant_events_divulgence (event_sequential_id);

-- lookup divulgance events, in order of ingestion
create index participant_events_divulgence_contract_id_idx on participant_events_divulgence (contract_id, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------
create TABLE participant_events_create (
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

    driver_metadata BINARY LARGE OBJECT
);

-- offset index: used to translate to sequential_id
create index participant_events_create_event_offset on participant_events_create (event_offset);

-- sequential_id index for paging
create index participant_events_create_event_sequential_id on participant_events_create (event_sequential_id);

-- lookup by transaction id
create index participant_events_create_transaction_id_idx on participant_events_create (transaction_id);

-- lookup by contract id
create index participant_events_create_contract_id_idx on participant_events_create (contract_id);

-- lookup by contract_key
create index participant_events_create_create_key_hash_idx on participant_events_create (create_key_hash, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------
create TABLE participant_events_consuming_exercise (
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
create index participant_events_consuming_exercise_event_offset on participant_events_consuming_exercise (event_offset);

-- sequential_id index for paging
create index participant_events_consuming_exercise_event_sequential_id on participant_events_consuming_exercise (event_sequential_id);

-- lookup by transaction id
create index participant_events_consuming_exercise_transaction_id_idx on participant_events_consuming_exercise (transaction_id);

-- lookup by contract id
create index participant_events_consuming_exercise_contract_id_idx on participant_events_consuming_exercise (contract_id);

---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------
create TABLE participant_events_non_consuming_exercise (
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
create index participant_events_non_consuming_exercise_event_offset on participant_events_non_consuming_exercise (event_offset);

-- sequential_id index for paging
create index participant_events_non_consuming_exercise_event_sequential_id on participant_events_non_consuming_exercise (event_sequential_id);

-- lookup by transaction id
create index participant_events_non_consuming_exercise_transaction_id_idx on participant_events_non_consuming_exercise (transaction_id);

create TABLE string_interning (
    internal_id integer PRIMARY KEY NOT NULL,
    external_string text
);

create TABLE participant_events_create_filter (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL
);

create index idx_participant_events_create_filter_party_template_seq_id_idx on participant_events_create_filter(party_id, template_id, event_sequential_id);
create index idx_participant_events_create_filter_party_seq_id_idx on participant_events_create_filter(party_id, event_sequential_id);
create index idx_participant_events_create_seq_id_idx on participant_events_create_filter(event_sequential_id);

create TABLE transaction_metering (
    application_id VARCHAR NOT NULL,
    action_count INTEGER NOT NULL,
    metering_timestamp BIGINT NOT NULL,
    ledger_offset VARCHAR NOT NULL
);

create index transaction_metering_ledger_offset on transaction_metering(ledger_offset);

create TABLE metering_parameters (
    ledger_metering_end VARCHAR,
    ledger_metering_timestamp BIGINT NOT NULL
);

create TABLE participant_metering (
    application_id VARCHAR NOT NULL,
    from_timestamp BIGINT NOT NULL,
    to_timestamp BIGINT NOT NULL,
    action_count INTEGER NOT NULL,
    ledger_offset VARCHAR NOT NULL
);

create unique index participant_metering_from_to_application on participant_metering(from_timestamp, to_timestamp, application_id);


-- NOTE: We keep participant user and party record tables independent from indexer-based tables, such that
--       we maintain a property that they can be moved to a separate database without any extra schema changes.
---------------------------------------------------------------------------------------------------
-- Participant local store: users
---------------------------------------------------------------------------------------------------
create TABLE participant_users (
    internal_id         INTEGER             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id             VARCHAR(256)        NOT NULL UNIQUE,
    primary_party       VARCHAR(512),
    is_deactivated      BOOLEAN             NOT NULL,
    resource_version    BIGINT              NOT NULL,
    created_at          BIGINT              NOT NULL
);
create TABLE participant_user_rights (
    user_internal_id    INTEGER             NOT NULL REFERENCES participant_users (internal_id) ON delete CASCADE,
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
create TABLE participant_user_annotations (
    internal_id         INTEGER             NOT NULL REFERENCES participant_users (internal_id) ON delete CASCADE,
    name                VARCHAR(512)        NOT NULL,
    -- 256k = 256*1024 = 262144
    val                 VARCHAR(262144),
    updated_at          BIGINT              NOT NULL,
    UNIQUE (internal_id, name)
);
insert into participant_users(user_id, primary_party, is_deactivated, resource_version, created_at)
    values ('participant_admin', null, false, 0,  0);
insert into participant_user_rights(user_internal_id, user_right, for_party, granted_at)
    select internal_id, 1, null, 0
    from participant_users
    where user_id = 'participant_admin';

---------------------------------------------------------------------------------------------------
-- Participant local store: party records
---------------------------------------------------------------------------------------------------
create TABLE participant_party_records (
    internal_id         INTEGER             GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    party               VARCHAR(512)        NOT NULL UNIQUE,
    resource_version    BIGINT              NOT NULL,
    created_at          BIGINT              NOT NULL
);
create TABLE participant_party_record_annotations (
    internal_id         INTEGER             NOT NULL REFERENCES participant_party_records (internal_id) ON delete CASCADE,
    name                VARCHAR(512)        NOT NULL,
    -- 256k = 256*1024 = 262144
    val                 VARCHAR(262144),
    updated_at          BIGINT              NOT NULL,
    UNIQUE (internal_id, name)
);
