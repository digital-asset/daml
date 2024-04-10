-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- Metering parameters
--
-- This table is meant to have a single row storing the current metering parameters.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_metering_parameters (
    ledger_metering_end text,
    ledger_metering_timestamp bigint not null
);

---------------------------------------------------------------------------------------------------
-- Metering consolidated entries
--
-- This table is written periodically to store partial sums of transaction metrics.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_participant_metering (
    application_id text not null,
    from_timestamp bigint not null,
    to_timestamp bigint not null,
    action_count integer not null,
    ledger_offset text not null
);

CREATE UNIQUE INDEX lapi_participant_metering_from_to_application_idx ON lapi_participant_metering(from_timestamp, to_timestamp, application_id);

---------------------------------------------------------------------------------------------------
-- Metering raw entries
--
-- This table is written for every transaction and stores its metrics.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_transaction_metering (
    application_id text not null,
    action_count integer not null,
    metering_timestamp bigint not null,
    ledger_offset text not null
);

CREATE INDEX lapi_transaction_metering_ledger_offset_idx ON lapi_transaction_metering USING btree (ledger_offset);

---------------------------------------------------------------------------------------------------
-- Package entries
--
-- A table for tracking DAML-LF package submissions.
-- It includes id to track the package submission and status.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_package_entries (
    ledger_offset varchar(4000) collate "C" primary key not null,
    recorded_at bigint not null, --with timezone
    -- SubmissionId for package to be uploaded
    submission_id varchar(1000) collate "C",
    -- The type of entry, one of 'accept' or 'reject'
    typ varchar(1000) collate "C" not null,
    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar(1000) collate "C",
    constraint check_package_entry_type
        check (
                (typ = 'accept' and rejection_reason is null) or
                (typ = 'reject' and rejection_reason is not null)
            )
);

-- Index for retrieving the package entry by submission id
CREATE INDEX lapi_package_entries_idx ON lapi_package_entries USING btree (submission_id);

---------------------------------------------------------------------------------------------------
-- List of packages
--
-- A table for tracking DAML-LF packages.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_packages (
    -- The unique identifier of the package (the hash of its content)
    package_id varchar(4000) collate "C" primary key not null,
    -- Packages are uploaded as DAR files (i.e., in groups)
    -- This field can be used to find out which packages were uploaded together
    upload_id varchar(1000) collate "C" not null,
    -- A human readable description of the package source
    source_description varchar(1000) collate "C",
    -- The size of the archive payload (i.e., the serialized DAML-LF package), in bytes
    package_size bigint not null,
    -- The time when the package was added
    known_since bigint not null,
    -- The ledger end at the time when the package was added
    ledger_offset varchar(4000) collate "C" not null,
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
CREATE INDEX lapi_packages_ledger_offset_idx ON lapi_packages USING btree (ledger_offset);

---------------------------------------------------------------------------------------------------
-- Parameters
--
-- This table is meant to have a single row storing all the parameters we have.
-- We make sure the following invariant holds:
-- - The ledger_end and ledger_end_sequential_id are always defined at the same time. I.e., either
--   both are NULL, or both are defined.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_parameters (
    -- stores the head offset, meant to change with every new ledger entry
    ledger_end varchar(4000) collate "C" not null,
    participant_id varchar(1000) collate "C" not null,
    -- Add the column for most recent pruning offset to parameters.
    -- A value of NULL means that the participant has not been pruned so far.
    participant_pruned_up_to_inclusive varchar(4000) collate "C",
    -- the sequential_event_id up to which all events have been ingested
    ledger_end_sequential_id bigint not null,
    participant_all_divulged_contracts_pruned_up_to_inclusive varchar(4000) collate "C",
    -- lapi_string_interning ledger-end tracking
    ledger_end_string_interning_id integer not null
);


---------------------------------------------------------------------------------------------------
-- Completions
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_command_completions (
    completion_offset varchar(4000) collate "C" not null,
    record_time bigint not null,
    application_id varchar(4000) collate "C" not null,
    submitters integer[] not null,
    command_id varchar(4000) collate "C" not null,
    -- The transaction ID is `NULL` for rejected transactions.
    transaction_id varchar(4000) collate "C",
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
    rejection_status_message varchar(4000) collate "C",
    rejection_status_details bytea,

    domain_id integer not null,
    trace_context bytea
);

CREATE INDEX lapi_command_completions_application_id_offset_idx ON lapi_command_completions USING btree (application_id, completion_offset);

---------------------------------------------------------------------------------------------------
-- Events: Assign
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_assign (
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
    package_name integer not null,
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
    create_key_value bytea,
    create_key_hash text,
    create_argument_compression smallint,
    create_key_value_compression smallint,
    ledger_effective_time bigint not null,
    driver_metadata bytea not null,

    create_key_maintainers integer[],
    trace_context bytea,
    record_time bigint not null
);

-- index for queries resolving contract ID to sequential IDs.
CREATE INDEX lapi_events_assign_event_contract_id_idx ON lapi_events_assign USING btree (contract_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX lapi_events_assign_event_offset_idx ON lapi_events_assign USING btree (event_offset, event_sequential_id);

-- sequential_id index for paging
CREATE INDEX lapi_events_assign_event_sequential_id_idx ON lapi_events_assign USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_consuming_exercise (
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
    package_name integer not null,
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
    trace_context bytea,
    record_time bigint not null
);

-- lookup by contract id
CREATE INDEX lapi_events_consuming_exercise_contract_id_idx ON lapi_events_consuming_exercise USING hash (contract_id);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_consuming_exercise_event_offset_idx ON lapi_events_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_consuming_exercise_event_sequential_id_idx ON lapi_events_consuming_exercise USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: create
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_create (
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
    package_name integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders
    tree_event_witnesses integer[] default '{}'::integer[] not null, -- informees

    -- * contract data
    create_argument bytea not null,
    create_signatories integer[] not null,
    create_observers integer[] not null,
    create_key_value bytea,
    create_key_hash text,

    -- * compression flags
    create_argument_compression smallint,
    create_key_value_compression smallint,
    driver_metadata bytea,
    domain_id integer not null,
    create_key_maintainers integer[],
    trace_context bytea,
    record_time bigint not null
);

-- lookup by contract id
CREATE INDEX lapi_events_create_contract_id_idx ON lapi_events_create USING hash (contract_id);

-- lookup by contract_key
CREATE INDEX lapi_events_create_create_key_hash_idx ON lapi_events_create USING btree (create_key_hash, event_sequential_id);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_create_event_offset_idx ON lapi_events_create USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_create_event_sequential_id_idx ON lapi_events_create USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: non-consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_non_consuming_exercise (
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
    package_name integer not null,
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
    trace_context bytea,
    record_time bigint not null
);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_non_consuming_exercise_event_offset_idx ON lapi_events_non_consuming_exercise USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_non_consuming_exercise_event_sequential_id_idx ON lapi_events_non_consuming_exercise USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Unassign
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_unassign (
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
    package_name integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders

    -- * common reassignment
    source_domain_id integer not null,
    target_domain_id integer not null,
    unassign_id text not null,
    reassignment_counter bigint not null,

    -- * unassigned specific
    assignment_exclusivity bigint,

    trace_context bytea,
    record_time bigint not null
);

-- multi-column index supporting per contract per domain lookup before/after sequential id query
CREATE INDEX lapi_events_unassign_contract_id_composite_idx ON lapi_events_unassign USING btree (contract_id, source_domain_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX lapi_events_unassign_event_offset_idx ON lapi_events_unassign USING btree (event_offset, event_sequential_id);

-- sequential_id index for paging
CREATE INDEX lapi_events_unassign_event_sequential_id_idx ON lapi_events_unassign USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Identity provider configs
--
-- This table stores identity provider records used in the ledger api identity provider config
-- service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_identity_provider_config (
    identity_provider_id varchar(255) collate "C" primary key not null,
    issuer varchar(4000) collate "C" not null unique,
    jwks_url varchar(4000) collate "C" not null,
    is_deactivated boolean not null,
    audience varchar(4000) collate "C"
);

---------------------------------------------------------------------------------------------------
-- Party records
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_records (
    internal_id serial primary key,
    party varchar(512) collate "C" not null unique,
    resource_version bigint not null,
    created_at bigint not null,
    identity_provider_id varchar(255) collate "C" default null references lapi_identity_provider_config (identity_provider_id)
);

---------------------------------------------------------------------------------------------------
-- Party record annotations
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_record_annotations (
    internal_id integer not null references lapi_party_records (internal_id) on delete cascade,
    name varchar(512) collate "C" not null,
    val text,
    updated_at bigint not null,
    unique (internal_id, name)
);

---------------------------------------------------------------------------------------------------
-- Transaction meta information
--
-- This table is used in point-wise lookups.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_transaction_meta (
    transaction_id text not null,
    event_offset text not null,
    event_sequential_id_first bigint not null,
    event_sequential_id_last bigint not null
);

CREATE INDEX lapi_transaction_meta_event_offset_idx ON lapi_transaction_meta USING btree (event_offset);
CREATE INDEX lapi_transaction_meta_tid_idx ON lapi_transaction_meta USING btree (transaction_id);

---------------------------------------------------------------------------------------------------
-- User entries
--
-- This table stores user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_users (
    internal_id serial primary key,
    user_id varchar(256) collate "C" not null unique,
    primary_party varchar(512) collate "C",
    created_at bigint not null,
    is_deactivated boolean default false not null,
    resource_version bigint default 0 not null,
    identity_provider_id varchar(255) collate "C" default null references lapi_identity_provider_config (identity_provider_id)
);

---------------------------------------------------------------------------------------------------
-- User rights
--
-- This table stores user rights used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_user_rights (
    user_internal_id integer not null references lapi_users (internal_id) on delete cascade,
    user_right integer not null,
    for_party varchar(512) collate "C",
    granted_at bigint not null,
    unique (user_internal_id, user_right, for_party)
);

CREATE UNIQUE INDEX lapi_user_rights_user_internal_id_user_right_idx
    ON lapi_user_rights USING btree (user_internal_id, user_right)
    WHERE (for_party is null);

INSERT INTO lapi_users(user_id, primary_party, created_at) VALUES ('participant_admin', null, 0);
INSERT INTO lapi_user_rights(user_internal_id, user_right, for_party, granted_at)
SELECT internal_id, 1, null, 0
FROM lapi_users
WHERE user_id = 'participant_admin';

---------------------------------------------------------------------------------------------------
-- User annotations
--
-- This table stores additional per user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_user_annotations (
    internal_id integer not null references lapi_users (internal_id) on delete cascade,
    name varchar(512) collate "C" not null,
    val text,
    updated_at bigint not null,
    unique (internal_id, name)
);

---------------------------------------------------------------------------------------------------
-- Party entries
--
-- A table for tracking party allocation submissions
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_entries (
    -- The ledger end at the time when the party allocation was added
    ledger_offset varchar(4000) collate "C" primary key not null,
    recorded_at bigint not null, --with timezone

    -- SubmissionId for the party allocation
    submission_id varchar(1000) collate "C",

    -- party
    party varchar(512) collate "C",

    -- displayName
    display_name varchar(1000) collate "C",

    -- The type of entry, 'accept' or 'reject'
    typ varchar(1000) collate "C" not null,

    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar(1000) collate "C",

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
CREATE INDEX lapi_party_entries_idx ON lapi_party_entries USING btree (submission_id);

CREATE INDEX lapi_party_entries_party_and_ledger_offset_idx ON lapi_party_entries USING btree (party, ledger_offset);

CREATE INDEX lapi_party_entries_party_id_and_ledger_offset_idx ON lapi_party_entries USING btree (party_id, ledger_offset);

CREATE TABLE lapi_pe_assign_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_pts_idx ON lapi_pe_assign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_ps_idx  ON lapi_pe_assign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_s_idx   ON lapi_pe_assign_id_filter_stakeholder(event_sequential_id);


CREATE TABLE lapi_pe_consuming_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_ps_idx ON lapi_pe_consuming_id_filter_non_stakeholder_informee USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_s_idx ON lapi_pe_consuming_id_filter_non_stakeholder_informee USING btree (event_sequential_id);

CREATE TABLE lapi_pe_consuming_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_ps_idx ON lapi_pe_consuming_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_pts_idx ON lapi_pe_consuming_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_s_idx ON lapi_pe_consuming_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE lapi_pe_create_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_ps_idx ON lapi_pe_create_id_filter_non_stakeholder_informee USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_s_idx ON lapi_pe_create_id_filter_non_stakeholder_informee USING btree (event_sequential_id);


CREATE TABLE lapi_pe_create_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_create_id_filter_stakeholder_pt_idx ON lapi_pe_create_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_pts_idx ON lapi_pe_create_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_s_idx ON lapi_pe_create_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE lapi_pe_non_consuming_id_filter_informee (
    event_sequential_id bigint not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_non_consuming_id_filter_informee_ps_idx ON lapi_pe_non_consuming_id_filter_informee USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_s_idx ON lapi_pe_non_consuming_id_filter_informee USING btree (event_sequential_id);

CREATE TABLE lapi_pe_unassign_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_unassign_id_filter_stakeholder_ps_idx ON lapi_pe_unassign_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_unassign_id_filter_stakeholder_pts_idx ON lapi_pe_unassign_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_unassign_id_filter_stakeholder_s_idx ON lapi_pe_unassign_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE lapi_string_interning (
    internal_id integer primary key not null,
    external_string text
);


