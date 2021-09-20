-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V100: Append-only schema
--
-- This is a major redesign of the index database schema. Updates from the ReadService are
-- now written into the append-only table participant_events, and the set of active contracts is
-- reconstructed from the log of create and archive events.
---------------------------------------------------------------------------------------------------

CREATE TABLE packages
(
    -- The unique identifier of the package (the hash of its content)
    package_id         VARCHAR2(4000) primary key not null,
    -- Packages are uploaded as DAR files (i.e., in groups)
    -- This field can be used to find out which packages were uploaded together
    upload_id          NVARCHAR2(1000)            not null,
    -- A human readable description of the package source
    source_description NVARCHAR2(1000),
    -- The size of the archive payload (i.e., the serialized DAML-LF package), in bytes
    package_size       NUMBER                     not null,
    -- The time when the package was added
    known_since        NUMBER                     not null,
    -- The ledger end at the time when the package was added
    ledger_offset      VARCHAR2(4000)             not null,
    -- The DAML-LF archive, serialized using the protobuf message `daml_lf.Archive`.
    --  See also `daml-lf/archive/da/daml_lf.proto`.
    package            BLOB                       not null
);
CREATE INDEX packages_ledger_offset_idx ON packages(ledger_offset);



CREATE TABLE configuration_entries
(
    ledger_offset    VARCHAR2(4000)  not null primary key,
    recorded_at      NUMBER          not null,
    submission_id    NVARCHAR2(1000) not null,
    -- The type of entry, one of 'accept' or 'reject'.
    typ              NVARCHAR2(1000) not null,
    -- The configuration that was proposed and either accepted or rejected depending on the type.
    -- Encoded according to participant-state/protobuf/ledger_configuration.proto.
    -- Add the current configuration column to parameters.
    configuration    BLOB            not null,

    -- If the type is 'rejection', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason NVARCHAR2(1000),

    -- Check that fields are correctly set based on the type.
    constraint configuration_entries_check_entry
        check (
                (typ = 'accept' and rejection_reason is null) or
                (typ = 'reject' and rejection_reason is not null))
);

CREATE INDEX idx_configuration_submission ON configuration_entries (submission_id);

CREATE TABLE package_entries
(
    ledger_offset    VARCHAR2(4000)  not null primary key,
    recorded_at      NUMBER          not null,
    -- SubmissionId for package to be uploaded
    submission_id    NVARCHAR2(1000),
    -- The type of entry, one of 'accept' or 'reject'
    typ              NVARCHAR2(1000) not null,
    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason NVARCHAR2(1000),

    constraint check_package_entry_type
        check (
                (typ = 'accept' and rejection_reason is null) or
                (typ = 'reject' and rejection_reason is not null)
            )
);

-- Index for retrieving the package entry by submission id
CREATE INDEX idx_package_entries ON package_entries (submission_id);

CREATE TABLE party_entries
(
    -- The ledger end at the time when the party allocation was added
    -- cannot BLOB add as primary key with oracle
    ledger_offset    VARCHAR2(4000)  primary key not null,
    recorded_at      NUMBER          not null,
    -- SubmissionId for the party allocation
    submission_id    NVARCHAR2(1000),
    -- party
    party            NVARCHAR2(1000),
    -- displayName
    display_name     NVARCHAR2(1000),
    -- The type of entry, 'accept' or 'reject'
    typ              NVARCHAR2(1000) not null,
    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason NVARCHAR2(1000),
    -- true if the party was added on participantId node that owns the party
    is_local         NUMBER(1, 0),

    constraint check_party_entry_type
        check (
                (typ = 'accept' and rejection_reason is null and party is not null) or
                (typ = 'reject' and rejection_reason is not null)
            )
);
CREATE INDEX idx_party_entries ON party_entries(submission_id);
CREATE INDEX idx_party_entries_party_and_ledger_offset ON party_entries(party, ledger_offset);

CREATE TABLE participant_command_completions
(
    completion_offset           VARCHAR2(4000)  NOT NULL,
    record_time                 NUMBER          NOT NULL,
    application_id              NVARCHAR2(1000) NOT NULL,

    -- The submission ID will be provided by the participant or driver if the application didn't provide one.
    -- Nullable to support historical data.
    submission_id               NVARCHAR2(1000),

    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the application as one of:
    -- 1. an initial offset
    -- 2. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    -- 3. an initial timestamp
    deduplication_offset            VARCHAR2(4000),
    deduplication_duration_seconds  NUMBER,
    deduplication_duration_nanos    NUMBER,
    deduplication_start             NUMBER,

    submitters                  CLOB NOT NULL CONSTRAINT ensure_json_submitters CHECK (submitters IS JSON),
    command_id                  NVARCHAR2(1000) NOT NULL,

    transaction_id              NVARCHAR2(1000), -- null for rejected transactions and checkpoints
    rejection_status_code       INTEGER,         -- null for accepted transactions and checkpoints
    rejection_status_message    CLOB,            -- null for accepted transactions and checkpoints
    rejection_status_details    BLOB             -- null for accepted transactions and checkpoints
);

CREATE INDEX participant_command_completions_idx ON participant_command_completions(completion_offset, application_id);

CREATE TABLE participant_command_submissions
(
    -- The deduplication key
    deduplication_key NVARCHAR2(1000) primary key not null,
    -- The time the command will stop being deduplicated
    deduplicate_until NUMBER                      not null
);

---------------------------------------------------------------------------------------------------
-- Events table: divulgence
---------------------------------------------------------------------------------------------------
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
    tree_event_witnesses CLOB DEFAULT '[]' NOT NULL CONSTRAINT ensure_json_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),       -- informees for create, exercise, and divulgance events

    -- * divulgence and create events
    create_argument BLOB,

    -- * compression flags
    create_argument_compression SMALLINT
);

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
CREATE SEARCH INDEX participant_events_divulgence_tree_event_witnesses_idx ON participant_events_divulgence (tree_event_witnesses) FOR JSON;

-- lookup divulgance events, in order of ingestion
CREATE INDEX participant_events_divulgence_contract_id_idx ON participant_events_divulgence(contract_id, event_sequential_id);


---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_create (
    -- * event identification
    event_sequential_id NUMBER NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)
    ledger_effective_time NUMBER NOT NULL,
    node_index INTEGER NOT NULL,
    event_offset VARCHAR2(4000) NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR2(4000) NOT NULL,
    workflow_id VARCHAR2(4000),
    command_id  VARCHAR2(4000),
    application_id VARCHAR2(4000),
    submitters CLOB CONSTRAINT ensure_json_pec_submitters CHECK (submitters IS JSON),

    -- * event metadata
    event_id VARCHAR2(4000) NOT NULL,        -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id VARCHAR2(4000) NOT NULL,
    template_id VARCHAR2(4000) NOT NULL,
    flat_event_witnesses CLOB DEFAULT '[]' NOT NULL CONSTRAINT ensure_json_pec_flat_event_witnesses CHECK (flat_event_witnesses IS JSON),       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses CLOB DEFAULT '[]' NOT NULL CONSTRAINT ensure_json_pec_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),       -- informees for create, exercise, and divulgance events

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
CREATE SEARCH INDEX participant_events_create_flat_event_witnesses_idx ON participant_events_create (flat_event_witnesses) FOR JSON;
CREATE SEARCH INDEX participant_events_create_tree_event_witnesses_idx ON participant_events_create (tree_event_witnesses) FOR JSON;

-- lookup by contract id
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create(contract_id);

-- lookup by contract_key
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create(create_key_hash, event_sequential_id);

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
    ledger_effective_time NUMBER NOT NULL,
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
    flat_event_witnesses CLOB DEFAULT '[]' NOT NULL CONSTRAINT ensure_json_pece_flat_event_witnesses CHECK (flat_event_witnesses IS JSON),       -- stakeholders of create events and consuming exercise events
    tree_event_witnesses CLOB DEFAULT '[]' NOT NULL CONSTRAINT ensure_json_pece_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),       -- informees for create, exercise, and divulgance events

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
CREATE SEARCH INDEX participant_events_consuming_exercise_flat_event_witnesses_idx ON participant_events_consuming_exercise (flat_event_witnesses) FOR JSON;
CREATE SEARCH INDEX participant_events_consuming_exercise_tree_event_witnesses_idx ON participant_events_consuming_exercise (tree_event_witnesses) FOR JSON;

-- lookup by contract id
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise (contract_id);

---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_non_consuming_exercise (
    -- * event identification
    event_sequential_id NUMBER NOT NULL,
    -- NOTE: this must be assigned sequentially by the indexer such that
    -- for all events ev1, ev2 it holds that '(ev1.offset < ev2.offset) <=> (ev1.event_sequential_id < ev2.event_sequential_id)

    ledger_effective_time NUMBER NOT NULL,
    node_index INTEGER NOT NULL,
    event_offset VARCHAR2(4000) NOT NULL,

    -- * transaction metadata
    transaction_id VARCHAR2(4000) NOT NULL,
    workflow_id VARCHAR2(4000),
    command_id VARCHAR2(4000),
    application_id VARCHAR2(4000),
    submitters CLOB CONSTRAINT ensure_json_pence_submitters CHECK (submitters IS JSON),

    -- * event metadata
    event_id VARCHAR2(4000) NOT NULL,                                   -- string representation of (transaction_id, node_index)

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
CREATE SEARCH INDEX participant_events_non_consuming_exercise_flat_event_witness_idx ON participant_events_non_consuming_exercise (flat_event_witnesses) FOR JSON;
CREATE SEARCH INDEX participant_events_non_consuming_exercise_tree_event_witness_idx ON participant_events_non_consuming_exercise (tree_event_witnesses) FOR JSON;

CREATE VIEW participant_events AS
SELECT cast(0 as SMALLINT)          AS event_kind,
       participant_events_divulgence.event_sequential_id,
       cast(NULL as VARCHAR2(4000)) AS event_offset,
       cast(NULL as VARCHAR2(4000)) AS transaction_id,
       cast(NULL as NUMBER)         AS ledger_effective_time,
       participant_events_divulgence.command_id,
       participant_events_divulgence.workflow_id,
       participant_events_divulgence.application_id,
       participant_events_divulgence.submitters,
       cast(NULL as INTEGER)        as node_index,
       cast(NULL as VARCHAR2(4000)) as event_id,
       participant_events_divulgence.contract_id,
       participant_events_divulgence.template_id,
       to_clob('[]')                AS flat_event_witnesses,
       participant_events_divulgence.tree_event_witnesses,
       participant_events_divulgence.create_argument,
       to_clob('[]')                AS create_signatories,
       to_clob('[]')                AS create_observers,
       cast(NULL as VARCHAR2(4000)) AS create_agreement_text,
       NULL                 AS create_key_value,
       cast(NULL as VARCHAR2(4000)) AS create_key_hash,
       cast(NULL as VARCHAR2(4000)) AS exercise_choice,
       NULL AS exercise_argument,
       NULL AS exercise_result,
       to_clob('[]')                AS exercise_actors,
       to_clob('[]')                AS exercise_child_event_ids,
       participant_events_divulgence.create_argument_compression,
       cast(NULL as SMALLINT)       AS create_key_value_compression,
       cast(NULL as SMALLINT)       AS exercise_argument_compression,
       cast(NULL as SMALLINT)       AS exercise_result_compression
FROM participant_events_divulgence
UNION ALL
SELECT (10)                         AS event_kind,
       participant_events_create.event_sequential_id,
       participant_events_create.event_offset,
       participant_events_create.transaction_id,
       participant_events_create.ledger_effective_time,
       participant_events_create.command_id,
       participant_events_create.workflow_id,
       participant_events_create.application_id,
       participant_events_create.submitters,
       participant_events_create.node_index,
       participant_events_create.event_id,
       participant_events_create.contract_id,
       participant_events_create.template_id,
       participant_events_create.flat_event_witnesses,
       participant_events_create.tree_event_witnesses,
       participant_events_create.create_argument,
       participant_events_create.create_signatories,
       participant_events_create.create_observers,
       participant_events_create.create_agreement_text,
       participant_events_create.create_key_value,
       participant_events_create.create_key_hash,
       cast(NULL as VARCHAR2(4000)) AS exercise_choice,
       NULL AS exercise_argument,
       NULL AS exercise_result,
       to_clob('[]')                AS exercise_actors,
       to_clob('[]')                AS exercise_child_event_ids,
       participant_events_create.create_argument_compression,
       participant_events_create.create_key_value_compression,
       cast(NULL as SMALLINT)       AS exercise_argument_compression,
       cast(NULL as SMALLINT)       AS exercise_result_compression
FROM participant_events_create
UNION ALL
SELECT (20)          AS event_kind,
       participant_events_consuming_exercise.event_sequential_id,
       participant_events_consuming_exercise.event_offset,
       participant_events_consuming_exercise.transaction_id,
       participant_events_consuming_exercise.ledger_effective_time,
       participant_events_consuming_exercise.command_id,
       participant_events_consuming_exercise.workflow_id,
       participant_events_consuming_exercise.application_id,
       participant_events_consuming_exercise.submitters,
       participant_events_consuming_exercise.node_index,
       participant_events_consuming_exercise.event_id,
       participant_events_consuming_exercise.contract_id,
       participant_events_consuming_exercise.template_id,
       participant_events_consuming_exercise.flat_event_witnesses,
       participant_events_consuming_exercise.tree_event_witnesses,
       NULL  AS create_argument,
       to_clob('[]') AS create_signatories,
       to_clob('[]') AS create_observers,
       NULL          AS create_agreement_text,
       participant_events_consuming_exercise.create_key_value,
       NULL          AS create_key_hash,
       participant_events_consuming_exercise.exercise_choice,
       participant_events_consuming_exercise.exercise_argument,
       participant_events_consuming_exercise.exercise_result,
       participant_events_consuming_exercise.exercise_actors,
       participant_events_consuming_exercise.exercise_child_event_ids,
       NULL          AS create_argument_compression,
       participant_events_consuming_exercise.create_key_value_compression,
       participant_events_consuming_exercise.exercise_argument_compression,
       participant_events_consuming_exercise.exercise_result_compression
FROM participant_events_consuming_exercise
UNION ALL
SELECT (25)          AS event_kind,
       participant_events_non_consuming_exercise.event_sequential_id,
       participant_events_non_consuming_exercise.event_offset,
       participant_events_non_consuming_exercise.transaction_id,
       participant_events_non_consuming_exercise.ledger_effective_time,
       participant_events_non_consuming_exercise.command_id,
       participant_events_non_consuming_exercise.workflow_id,
       participant_events_non_consuming_exercise.application_id,
       participant_events_non_consuming_exercise.submitters,
       participant_events_non_consuming_exercise.node_index,
       participant_events_non_consuming_exercise.event_id,
       participant_events_non_consuming_exercise.contract_id,
       participant_events_non_consuming_exercise.template_id,
       participant_events_non_consuming_exercise.flat_event_witnesses,
       participant_events_non_consuming_exercise.tree_event_witnesses,
       NULL  AS create_argument,
       to_clob('[]') AS create_signatories,
       to_clob('[]') AS create_observers,
       NULL          AS create_agreement_text,
       participant_events_non_consuming_exercise.create_key_value,
       NULL          AS create_key_hash,
       participant_events_non_consuming_exercise.exercise_choice,
       participant_events_non_consuming_exercise.exercise_argument,
       participant_events_non_consuming_exercise.exercise_result,
       participant_events_non_consuming_exercise.exercise_actors,
       participant_events_non_consuming_exercise.exercise_child_event_ids,
       NULL          AS create_argument_compression,
       participant_events_non_consuming_exercise.create_key_value_compression,
       participant_events_non_consuming_exercise.exercise_argument_compression,
       participant_events_non_consuming_exercise.exercise_result_compression
FROM participant_events_non_consuming_exercise;

---------------------------------------------------------------------------------------------------
-- Parameters table
---------------------------------------------------------------------------------------------------

-- new field: the sequential_event_id up to which all events have been ingested
CREATE TABLE parameters
-- this table is meant to have a single row storing all the parameters we have
(
    -- the generated or configured id identifying the ledger
    ledger_id                          NVARCHAR2(1000) not null,
    -- stores the head offset, meant to change with every new ledger entry
    ledger_end                         VARCHAR2(4000),
    participant_id                     NVARCHAR2(1000) not null,
    participant_pruned_up_to_inclusive VARCHAR2(4000),
    participant_all_divulged_contracts_pruned_up_to_inclusive VARCHAR2(4000),
    ledger_end_sequential_id           NUMBER
);

