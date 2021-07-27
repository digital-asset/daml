-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Stores the history of the ledger -- mostly transactions. This table
-- is immutable in the sense that rows can never be modified, only
-- added.

-- subsequently dropped by V30__

CREATE TABLE parameters
-- this table is meant to have a single row storing all the parameters we have
(
    -- the generated or configured id identifying the ledger
    ledger_id                          NVARCHAR2(1000) not null,
    -- stores the head offset, meant to change with every new ledger entry
    ledger_end                         VARCHAR2(4000),
    external_ledger_end                NVARCHAR2(1000),
    participant_id                     NVARCHAR2(1000),
    participant_pruned_up_to_inclusive VARCHAR2(4000),
    configuration                      BLOB
);

---------------------------------------------------------------------------------------------------
-- V4: List of parties
--
-- This schema version adds a table for tracking known parties.
-- In the sandbox, parties are added implicitly when they are first mentioned in a transaction,
-- or explicitly through an API call.
---------------------------------------------------------------------------------------------------


CREATE TABLE parties
(
    -- The unique identifier of the party
    party         NVARCHAR2(1000) primary key not null,
    -- A human readable name of the party, might not be unique
    display_name  NVARCHAR2(1000),
    -- True iff the party was added explicitly through an API call
    explicit      NUMBER(1, 0)                not null,
    -- For implicitly added parties: the offset of the transaction that introduced the party
    -- For explicitly added parties: the ledger end at the time when the party was added
    ledger_offset VARCHAR2(4000),
    is_local      NUMBER(1, 0)                not null
);

---------------------------------------------------------------------------------------------------
-- V5: List of packages
--
-- This schema version adds a table for tracking DAML-LF packages.
-- Previously, packages were only stored in memory and needed to be specified through the CLI.
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
    "size"             NUMBER                     not null,
    -- The time when the package was added
    known_since        TIMESTAMP                  not null,
    -- The ledger end at the time when the package was added
    ledger_offset      VARCHAR2(4000)             not null,
    -- The DAML-LF archive, serialized using the protobuf message `daml_lf.Archive`.
    --  See also `daml-lf/archive/da/daml_lf.proto`.
    package            BLOB                       not null
);

---------------------------------------------------------------------------------------------------
-- V12: Add table for ledger configuration changes
--
-- This schema version adds a table for ledger configuration changes and adds the latest
-- configuration to the parameters table.
---------------------------------------------------------------------------------------------------

-- Table for storing a log of ledger configuration changes and rejections.
CREATE TABLE configuration_entries
(
    ledger_offset    VARCHAR2(4000)  not null primary key,
    recorded_at      timestamp       not null,
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

-- Index for retrieving the configuration entry by submission identifier.
-- To be used for completing configuration submissions.
CREATE UNIQUE INDEX idx_configuration_submission ON configuration_entries (submission_id);

---------------------------------------------------------------------------------------------------
-- V13: party_entries
--
-- This schema version adds a table for tracking party allocation submissions
---------------------------------------------------------------------------------------------------

CREATE TABLE party_entries
(
    -- The ledger end at the time when the party allocation was added
    -- cannot BLOB add as primary key with oracle
    ledger_offset    VARCHAR2(4000)  primary key not null,
    recorded_at      timestamp       not null, --with timezone
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

-- Index for retrieving the party allocation entry by submission id per participant
CREATE UNIQUE INDEX idx_party_entries
    ON party_entries (submission_id);


---------------------------------------------------------------------------------------------------
-- V14: package_entries
--
-- This schema version adds a table for tracking DAML-LF package submissions
-- It includes id to track the package submission and status
---------------------------------------------------------------------------------------------------

CREATE TABLE package_entries
(
    ledger_offset    VARCHAR2(4000)  not null primary key,
    recorded_at      timestamp       not null,
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
CREATE UNIQUE INDEX idx_package_entries
    ON package_entries (submission_id);

CREATE TABLE participant_command_completions
(
    completion_offset VARCHAR2(4000)  not null,
    record_time       TIMESTAMP       not null,

    application_id    NVARCHAR2(1000) not null,
    submitters        CLOB NOT NULL CONSTRAINT ensure_json_submitters CHECK (submitters IS JSON),
    command_id        NVARCHAR2(1000) not null,

    transaction_id    NVARCHAR2(1000), -- null if the command was rejected and checkpoints
    status_code       INTEGER,         -- null for successful command and checkpoints
    status_message    CLOB  -- null for successful command and checkpoints
);

create index participant_command_completions_idx on participant_command_completions(completion_offset, application_id);

---------------------------------------------------------------------------------------------------
-- V16: New command deduplication
--
-- Command deduplication has moved from ledger to participant
---------------------------------------------------------------------------------------------------

CREATE TABLE participant_command_submissions
(
    -- The deduplication key
    deduplication_key NVARCHAR2(1000) primary key not null,
    -- The time the command will stop being deduplicated
    deduplicate_until timestamp                   not null
);


create table participant_events
(
    event_id                      VARCHAR2(1000) primary key                      not null,
    event_offset                  VARCHAR2(4000)                                  not null,
    contract_id                   VARCHAR2(1000)                                  not null,
    transaction_id                VARCHAR2(1000)                                  not null,
    ledger_effective_time         TIMESTAMP                                       not null,
    template_id                   VARCHAR2(1000)                                  not null,
    node_index                    NUMBER                                          not null, -- post-traversal order of an event within a transaction

    -- these fields can be null if the transaction originated in another participant
    command_id                    VARCHAR2(1000),
    workflow_id                   VARCHAR2(1000),                                           -- null unless provided by a Ledger API call
    application_id                VARCHAR2(1000),
    submitters                    CLOB NOT NULL CONSTRAINT ensure_json_participant_submitters CHECK (submitters IS JSON),

    -- non-null iff this event is a create
    create_argument               BLOB,
    create_signatories            CLOB NOT NULL CONSTRAINT ensure_json_participant_create_signatories CHECK (create_signatories IS JSON),
    create_observers              CLOB NOT NULL CONSTRAINT ensure_json_participant_create_observers CHECK (create_observers IS JSON),
    create_agreement_text         VARCHAR2(1000),                                           -- null if agreement text is not provided
    create_consumed_at            VARCHAR2(4000),                                           -- null if the contract created by this event is active
    create_key_value              BLOB,                                                     -- null if the contract created by this event has no key

    -- non-null iff this event is an exercise
    exercise_consuming            NUMBER(1, 0),
    exercise_choice               VARCHAR2(1000),
    exercise_argument             BLOB,
    exercise_result               BLOB,
    exercise_actors               CLOB NOT NULL CONSTRAINT ensure_json_participant_exercise_actors CHECK (exercise_actors IS JSON),
    exercise_child_event_ids      CLOB NOT NULL CONSTRAINT ensure_json_participant_exercise_child_event_ids CHECK (exercise_child_event_ids IS JSON),                                            -- event identifiers of consequences of this exercise

    flat_event_witnesses          CLOB NOT NULL CONSTRAINT ensure_json_participant_flat_event_witnesses CHECK (flat_event_witnesses IS JSON),
    tree_event_witnesses          CLOB NOT NULL CONSTRAINT ensure_json_participant_tree_event_witnesses CHECK (tree_event_witnesses IS JSON),

    event_sequential_id           NUMBER GENERATED BY DEFAULT ON NULL AS IDENTITY not null,
    create_argument_compression   NUMBER,
    create_key_value_compression  NUMBER,
    exercise_argument_compression NUMBER,
    exercise_result_compression   NUMBER
);

-- support ordering by offset and transaction, ready for serving via the Ledger API
create index participant_events_offset_txn_node_idx on participant_events (event_offset, transaction_id, node_index);

-- support looking up a create event by the identifier of the contract it created, so that
-- consuming exercise events can use it to set the value of create_consumed_at
create index participant_events_contract_idx on participant_events (contract_id);

-- support requests of transactions by transaction_id
create index participant_events_transaction_idx on participant_events (transaction_id);

-- support filtering by template
create index participant_events_template_ids on participant_events (template_id);

-- 4. create a new index involving event_sequential_id
create index participant_events_event_sequential_id on participant_events (event_sequential_id);

-- 5. we need this index to convert event_offset to event_sequential_id
create index participant_events_event_offset on participant_events (event_offset);

create index participant_events_flat_event_witnesses_idx on participant_events (JSON_ARRAY(flat_event_witnesses));
create index participant_events_tree_event_witnesses_idx on participant_events (JSON_ARRAY(tree_event_witnesses));


---------------------------------------------------------------------------------------------------
-- V26: Contracts new schema
--
-- Used for interpretation and validation by the DAML engine
---------------------------------------------------------------------------------------------------

-- contains all active and divulged contracts
create table participant_contracts
(
    contract_id                  NVARCHAR2(1000) primary key not null,

    -- template_id and create_argument need to be nullable to leave this fields empty as part of this migration
    -- a second (java) migration will fill them up
    -- a third (sql) migration will make them non-nullable, as it's supposed to always be populated
    template_id                  NVARCHAR2(1000)             not null,
    create_argument              BLOB                        not null,

    -- the following fields are null for divulged contracts
    create_stakeholders          CLOB NOT NULL CONSTRAINT ensure_json_create_stakeholders CHECK (create_stakeholders IS JSON),
    create_key_hash              VARCHAR2(4000),
    create_ledger_effective_time TIMESTAMP,
    create_argument_compression  SMALLINT
);

-- support looking up a contract by key
create unique index participant_contracts_idx on participant_contracts (create_key_hash);


-- visibility of contracts to parties
create table participant_contract_witnesses
(
    contract_id      NVARCHAR2(1000) not null,
    contract_witness NVARCHAR2(1000) not null,

    primary key (contract_id, contract_witness),
    foreign key (contract_id) references participant_contracts (contract_id)
);
