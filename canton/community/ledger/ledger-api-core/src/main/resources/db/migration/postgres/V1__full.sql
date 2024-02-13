-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- Table for storing a log of ledger configuration changes and rejections.

CREATE TABLE configuration_entries (
    ledger_offset varchar primary key not null,
    recorded_at bigint not null, -- with time zone

    submission_id varchar not null,
    -- The type of entry, one of 'accept' or 'reject'.
    typ varchar not null,

    -- The configuration that was proposed and either accepted or rejected depending on the type.
    -- Encoded according to participant-state/protobuf/ledger_configuration.proto.
    configuration bytea not null,

    -- If the type is 'rejection', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar,

    -- Check that fields are correctly set based on the type.
    constraint check_entry
    check (
      (typ = 'accept' and rejection_reason is null) or
      (typ = 'reject' and rejection_reason is not null))
);

-- Index for retrieving the configuration entry by submission identifier.
-- To be used for completing configuration submissions.
CREATE INDEX idx_configuration_submission ON configuration_entries USING btree (submission_id);


CREATE TABLE metering_parameters (
    ledger_metering_end text,
    ledger_metering_timestamp bigint not null
);

CREATE TABLE participant_metering (
    application_id text not null,
    from_timestamp bigint not null,
    to_timestamp bigint not null,
    action_count integer not null,
    ledger_offset text not null
);

CREATE UNIQUE INDEX participant_metering_from_to_application ON participant_metering(from_timestamp, to_timestamp, application_id);


---------------------------------------------------------------------------------------------------
-- package_entries
--
-- This schema version adds a table for tracking DAML-LF package submissions
-- It includes id to track the package submission and status
---------------------------------------------------------------------------------------------------

CREATE TABLE package_entries (
    ledger_offset varchar primary key not null,
    recorded_at bigint not null, --with timezone
    -- SubmissionId for package to be uploaded
    submission_id varchar,
    -- The type of entry, one of 'accept' or 'reject'
    typ varchar not null,
    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar,
    constraint check_package_entry_type
        check (
                (typ = 'accept' and rejection_reason is null) or
                (typ = 'reject' and rejection_reason is not null)
            )
);

-- Index for retrieving the package entry by submission id
CREATE INDEX idx_package_entries ON package_entries USING btree (submission_id);


---------------------------------------------------------------------------------------------------
-- List of packages
--
-- This schema version adds a table for tracking DAML-LF packages.
-- Previously, packages were only stored in memory and needed to be specified through the CLI.
---------------------------------------------------------------------------------------------------

CREATE TABLE packages (
    -- The unique identifier of the package (the hash of its content)
    package_id varchar primary key not null,
    -- Packages are uploaded as DAR files (i.e., in groups)
    -- This field can be used to find out which packages were uploaded together
    upload_id varchar not null,
    -- A human readable description of the package source
    source_description varchar,
    -- The size of the archive payload (i.e., the serialized DAML-LF package), in bytes
    package_size bigint not null,
    -- The time when the package was added
    known_since bigint not null,
    -- The ledger end at the time when the package was added
    ledger_offset varchar not null,
    -- The DAML-LF archive, serialized using the protobuf message `daml_lf.Archive`.
    --  See also `daml-lf/archive/da/daml_lf.proto`.
    package bytea not null
);

---------------------------------------------------------------------------------------------------
-- Indices to speed up indexer initialization
--
-- At startup, the indexer deletes all entries with an offset beyond the stored ledger end.
-- Such entries can be written when the indexer crashes right before updating the ledger end.
-- This migration adds missing indices to speed up the deletion of such entries.
---------------------------------------------------------------------------------------------------
CREATE INDEX packages_ledger_offset_idx ON packages USING btree (ledger_offset);

-- this table is meant to have a single row storing all the parameters we have

---------------------------------------------------------------------------------------------------
-- Parameter table
--
-- We make sure the following invariant holds:
-- - The ledger_end and ledger_end_sequential_id are always defined at the same time. I.e., either
--   both are NULL, or both are defined.
---------------------------------------------------------------------------------------------------
CREATE TABLE parameters (
    -- stores the head offset, meant to change with every new ledger entry
    ledger_end varchar not null,
    participant_id varchar not null,
    -- Add the column for most recent pruning offset to parameters.
    -- A value of NULL means that the participant has not been pruned so far.
    participant_pruned_up_to_inclusive varchar,
    -- the sequential_event_id up to which all events have been ingested
    ledger_end_sequential_id bigint not null,
    participant_all_divulged_contracts_pruned_up_to_inclusive varchar,
    -- string_interning ledger-end tracking
    ledger_end_string_interning_id integer not null
);


---------------------------------------------------------------------------------------------------
-- Completions table
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_command_completions (
    completion_offset varchar not null,
    record_time bigint not null,
    application_id varchar not null,
    submitters integer[] not null,
    command_id varchar not null,
    -- The transaction ID is `NULL` for rejected transactions.
    transaction_id varchar,
    -- The submission ID will be provided by the participant or driver if the application didn't provide one.
    -- Nullable to support historical data.
    submission_id text,
    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the application as one of:
    -- 1. an initial offset
    -- 2. an initial timestamp
    -- 3. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    deduplication_offset text,
    deduplication_duration_seconds bigint,
    deduplication_duration_nanos integer,
    deduplication_start bigint,

    -- The three columns below are `NULL` if the completion is for an accepted transaction.
    -- The `rejection_status_details` column contains a Protocol-Buffers-serialized message of type
    -- `daml.platform.index.StatusDetails`, containing the code, message, and further details
    -- (decided by the ledger driver), and may be `NULL` even if the other two columns are set.
    rejection_status_code integer,
    rejection_status_message varchar,
    rejection_status_details bytea,

    domain_id integer not null,
    trace_context bytea
);

CREATE INDEX participant_command_completions_application_id_offset_idx ON participant_command_completions USING btree (application_id, completion_offset);

---------------------------------------------------------------------------------------------------
-- Events table: Assign
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_assign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset

    -- * event identification
    event_offset text not null,

    -- * transaction metadata
    update_id text not null,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,

    submitter integer,

    -- * shared event information
    contract_id text not null,
    template_id integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders

    -- * common reassignment
    source_domain_id integer not null,
    target_domain_id integer not null,
    unassign_id text not null,
    reassignment_counter bigint not null,

    -- * assigned specific
    create_argument bytea not null,
    create_signatories integer[] default '{}'::integer[] not null,
    create_observers integer[] default '{}'::integer[] not null,
    create_agreement_text text,
    create_key_value bytea,
    create_key_hash text,
    create_argument_compression smallint,
    create_key_value_compression smallint,
    ledger_effective_time bigint not null,
    driver_metadata bytea not null,

    create_key_maintainers integer[],
    trace_context bytea
);

-- index for queries resolving contract ID to sequential IDs.
CREATE INDEX participant_events_assign_event_contract_id ON participant_events_assign USING btree (contract_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX participant_events_assign_event_offset ON participant_events_assign USING btree (event_offset, event_sequential_id);

-- sequential_id index for paging
CREATE INDEX participant_events_assign_event_sequential_id ON participant_events_assign USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,  -- transaction metadata
    node_index integer not null,            -- event metadata

    -- * event identification
    event_offset text not null,

    -- * transaction metadata
    transaction_id text not null,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,
    application_id text,
    submitters integer[],

    -- * event metadata
    event_id text not null,     -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text not null,
    template_id integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders
    tree_event_witnesses integer[] default '{}'::integer[] not null, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,          -- used for the mutable state cache

    -- * choice data
    exercise_choice text not null,
    exercise_argument bytea not null,
    exercise_result bytea,
    exercise_actors integer[] not null,
    exercise_child_event_ids text[] not null,

    -- * compression flags
    create_key_value_compression smallint,
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    domain_id integer not null,
    trace_context bytea
);

-- lookup by contract id
CREATE INDEX participant_events_consuming_exercise_contract_id_idx ON participant_events_consuming_exercise USING hash (contract_id);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_consuming_exercise_event_offset ON participant_events_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_consuming_exercise_event_sequential_id ON participant_events_consuming_exercise USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: create
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_create (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,  -- transaction metadata
    node_index integer not null,            -- event metadata

    -- * event identification
    event_offset text not null,

    -- * transaction metadata
    transaction_id text not null,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,
    application_id text,
    submitters integer[],

    -- * event metadata
    event_id text not null,     -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text not null,
    template_id integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders
    tree_event_witnesses integer[] default '{}'::integer[] not null, -- informees

    -- * contract data
    create_argument bytea not null,
    create_signatories integer[] not null,
    create_observers integer[] not null,
    create_agreement_text text,
    create_key_value bytea,
    create_key_hash text,

    -- * compression flags
    create_argument_compression smallint,
    create_key_value_compression smallint,
    driver_metadata bytea,
    domain_id integer not null,
    create_key_maintainers integer[],
    trace_context bytea
);

-- lookup by contract id
CREATE INDEX participant_events_create_contract_id_idx ON participant_events_create USING hash (contract_id);

-- lookup by contract_key
CREATE INDEX participant_events_create_create_key_hash_idx ON participant_events_create USING btree (create_key_hash, event_sequential_id);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_create_event_offset ON participant_events_create USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_create_event_sequential_id ON participant_events_create USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: non-consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_non_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,  -- transaction metadata
    node_index integer not null,            -- event metadata

    -- * event identification
    event_offset text not null,

    -- * transaction metadata
    transaction_id text not null,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,
    application_id text,
    submitters integer[],

    -- * event metadata
    event_id text not null,         -- string representation of (transaction_id, node_index)

    -- * shared event information
    contract_id text not null,
    template_id integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders
    tree_event_witnesses integer[] default '{}'::integer[] not null, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,     -- used for the mutable state cache

    -- * choice data
    exercise_choice text not null,
    exercise_argument bytea not null,
    exercise_result bytea,
    exercise_actors integer[] not null,
    exercise_child_event_ids text[] not null,

    -- * compression flags
    create_key_value_compression smallint,
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    domain_id integer not null,
    trace_context bytea
);

-- offset index: used to translate to sequential_id
CREATE INDEX participant_events_non_consuming_exercise_event_offset ON participant_events_non_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX participant_events_non_consuming_exercise_event_sequential_id ON participant_events_non_consuming_exercise USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events table: Unassign
---------------------------------------------------------------------------------------------------
CREATE TABLE participant_events_unassign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset

    -- * event identification
    event_offset text not null,

    -- * transaction metadata
    update_id text not null,
    workflow_id text,

    -- * submitter info (only visible on submitting participant)
    command_id text,

    submitter integer,

    -- * shared event information
    contract_id text not null,
    template_id integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders

    -- * common reassignment
    source_domain_id integer not null,
    target_domain_id integer not null,
    unassign_id text not null,
    reassignment_counter bigint not null,

    -- * unassigned specific
    assignment_exclusivity bigint,

    trace_context bytea
);

-- multi-column index supporting per contract per domain lookup before/after sequential id query
CREATE INDEX participant_events_unassign_contract_id_composite ON participant_events_unassign USING btree (contract_id, source_domain_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX participant_events_unassign_event_offset ON participant_events_unassign USING btree (event_offset, event_sequential_id);

-- sequential_id index for paging
CREATE INDEX participant_events_unassign_event_sequential_id ON participant_events_unassign USING btree (event_sequential_id);

--
-- Name: participant_identity_provider_config; Type: TABLE; Schema: public; Owner: fly
--

CREATE TABLE participant_identity_provider_config (
    identity_provider_id varchar(255) primary key not null collate "C",
    issuer varchar not null unique,
    jwks_url varchar not null,
    is_deactivated boolean not null,
    audience varchar
);

---------------------------------------------------------------------------------------------------
-- Append-only schema
--
-- Updates from the ReadService are written into the append-only table participant_events, and the
-- set of active contracts is reconstructed from the log of create and archive events.
---------------------------------------------------------------------------------------------------

-- Party record tables
CREATE TABLE participant_party_records (
    internal_id             serial          primary key,
    party                   varchar(512)    not null unique collate "C",
    resource_version        bigint          not null,
    created_at              bigint          not null,
    identity_provider_id    varchar(255)    default null references participant_identity_provider_config (identity_provider_id)
);

CREATE TABLE participant_party_record_annotations (
    internal_id     integer         not null references participant_party_records (internal_id) on delete cascade,
    name            varchar(512)    not null,
    val             text,
    updated_at      bigint          not null,
    unique (internal_id, name)
);


-- Point-wise lookup
CREATE TABLE participant_transaction_meta (
    transaction_id text not null,
    event_offset text not null,
    event_sequential_id_first bigint not null,
    event_sequential_id_last bigint not null
);

CREATE INDEX participant_transaction_meta_event_offset_idx ON participant_transaction_meta USING btree (event_offset);
CREATE INDEX participant_transaction_meta_tid_idx ON participant_transaction_meta USING btree (transaction_id);

CREATE TABLE participant_users (
    internal_id         serial          primary key,
    user_id             varchar(256)    not null unique collate "C",
    primary_party       varchar(512),
    created_at          bigint          not null,
    is_deactivated      boolean         default false not null,
    resource_version    bigint          default 0 not null,
    identity_provider_id varchar(255)   default null references participant_identity_provider_config (identity_provider_id)
);

CREATE TABLE participant_user_rights (
    user_internal_id    integer         not null references participant_users (internal_id) on delete cascade,
    user_right          integer         not null,
    for_party           varchar(512),
    granted_at          bigint          not null,
    unique (user_internal_id, user_right, for_party)
);

CREATE UNIQUE INDEX participant_user_rights_user_internal_id_user_right_idx
    ON participant_user_rights USING btree (user_internal_id, user_right)
    WHERE (for_party is null);

INSERT INTO participant_users(user_id, primary_party, created_at) VALUES ('participant_admin', null, 0);
INSERT INTO participant_user_rights(user_internal_id, user_right, for_party, granted_at)
SELECT internal_id, 1, null, 0
FROM participant_users
WHERE user_id = 'participant_admin';

CREATE TABLE participant_user_annotations (
    internal_id     integer         not null references participant_users (internal_id) on delete cascade,
    name            varchar(512)    not null,
    val text,
    updated_at      bigint          not null,
    unique (internal_id, name)
);

---------------------------------------------------------------------------------------------------
-- party_entries
--
-- This schema version adds a table for tracking party allocation submissions
---------------------------------------------------------------------------------------------------
CREATE TABLE party_entries (
    -- The ledger end at the time when the party allocation was added
    ledger_offset varchar primary key not null,
    recorded_at bigint not null, --with timezone

    -- SubmissionId for the party allocation
    submission_id varchar,

    -- party
    party varchar,

    -- displayName
    display_name varchar,

    -- The type of entry, 'accept' or 'reject'
    typ varchar not null,

    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar,

    -- true if the party was added on participantId node that owns the party
    is_local boolean,

    -- string interning id
    party_id integer,

    constraint check_party_entry_type
        check (
                (typ = 'accept' and rejection_reason is null and party is not null) or
                (typ = 'reject' and rejection_reason is not null)
            )
);

-- Index for retrieving the party allocation entry by submission id per participant
CREATE INDEX idx_party_entries ON party_entries USING btree (submission_id);

CREATE INDEX idx_party_entries_party_and_ledger_offset ON party_entries USING btree (party, ledger_offset);

CREATE INDEX idx_party_entries_party_id_and_ledger_offset ON party_entries USING btree (party_id, ledger_offset);

CREATE TABLE pe_assign_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);
CREATE INDEX pe_assign_id_filter_stakeholder_pts_idx ON pe_assign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX pe_assign_id_filter_stakeholder_ps_idx  ON pe_assign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX pe_assign_id_filter_stakeholder_s_idx   ON pe_assign_id_filter_stakeholder(event_sequential_id);


CREATE TABLE pe_consuming_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    party_id integer not null
);

CREATE INDEX pe_consuming_id_filter_non_stakeholder_informee_ps_idx ON pe_consuming_id_filter_non_stakeholder_informee USING btree (party_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_non_stakeholder_informee_s_idx ON pe_consuming_id_filter_non_stakeholder_informee USING btree (event_sequential_id);

CREATE TABLE pe_consuming_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX pe_consuming_id_filter_stakeholder_ps_idx ON pe_consuming_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_stakeholder_pts_idx ON pe_consuming_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX pe_consuming_id_filter_stakeholder_s_idx ON pe_consuming_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE pe_create_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    party_id integer not null
);

CREATE INDEX pe_create_id_filter_non_stakeholder_informee_ps_idx ON pe_create_id_filter_non_stakeholder_informee USING btree (party_id, event_sequential_id);
CREATE INDEX pe_create_id_filter_non_stakeholder_informee_s_idx ON pe_create_id_filter_non_stakeholder_informee USING btree (event_sequential_id);


CREATE TABLE pe_create_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX pe_create_id_filter_stakeholder_pt_idx ON pe_create_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX pe_create_id_filter_stakeholder_pts_idx ON pe_create_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX pe_create_id_filter_stakeholder_s_idx ON pe_create_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE pe_non_consuming_id_filter_informee (
    event_sequential_id bigint not null,
    party_id integer not null
);

CREATE INDEX pe_non_consuming_id_filter_informee_ps_idx ON pe_non_consuming_id_filter_informee USING btree (party_id, event_sequential_id);
CREATE INDEX pe_non_consuming_id_filter_informee_s_idx ON pe_non_consuming_id_filter_informee USING btree (event_sequential_id);

CREATE TABLE pe_unassign_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX pe_unassign_id_filter_stakeholder_ps_idx ON pe_unassign_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX pe_unassign_id_filter_stakeholder_pts_idx ON pe_unassign_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX pe_unassign_id_filter_stakeholder_s_idx ON pe_unassign_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE string_interning (
    internal_id integer primary key not null,
    external_string text
);

CREATE TABLE transaction_metering (
    application_id text not null,
    action_count integer not null,
    metering_timestamp bigint not null,
    ledger_offset text not null
);

CREATE INDEX transaction_metering_ledger_offset ON transaction_metering USING btree (ledger_offset);


