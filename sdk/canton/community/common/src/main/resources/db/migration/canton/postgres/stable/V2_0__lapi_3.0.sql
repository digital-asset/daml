-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- Metering parameters
--
-- This table is meant to have a single row storing the current metering parameters.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_metering_parameters (
    ledger_metering_end bigint,
    ledger_metering_timestamp bigint not null
);

---------------------------------------------------------------------------------------------------
-- Metering consolidated entries
--
-- This table is written periodically to store partial sums of transaction metrics.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_participant_metering (
    user_id varchar collate "C" not null,
    from_timestamp bigint not null,
    to_timestamp bigint not null,
    action_count integer not null,
    ledger_offset bigint
);

CREATE UNIQUE INDEX lapi_participant_metering_from_to_user_idx ON lapi_participant_metering(from_timestamp, to_timestamp, user_id);

---------------------------------------------------------------------------------------------------
-- Metering raw entries
--
-- This table is written for every transaction and stores its metrics.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_transaction_metering (
    user_id varchar collate "C" not null,
    action_count integer not null,
    metering_timestamp bigint not null,
    ledger_offset bigint
);

CREATE INDEX lapi_transaction_metering_ledger_offset_idx ON lapi_transaction_metering USING btree (ledger_offset);

---------------------------------------------------------------------------------------------------
-- Parameters
--
-- This table is meant to have a single row storing all the parameters we have.
-- We make sure the following invariant holds:
-- - The ledger_end, ledger_end_sequential_id, ledger_end_string_interning_id and
--   ledger_end_publication_time are always defined at the same time. I.e., either
--   all are NULL, or all are defined.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_parameters (
    -- stores the head offset, meant to change with every new ledger entry
    -- NULL denotes the participant begin
    ledger_end bigint,
    participant_id varchar collate "C" not null,
    -- Add the column for most recent pruning offset to parameters.
    -- A value of NULL means that the participant has not been pruned so far.
    participant_pruned_up_to_inclusive bigint,
    -- the sequential_event_id up to which all events have been ingested
    -- NULL denotes that no events have been ingested
    ledger_end_sequential_id bigint,
    participant_all_divulged_contracts_pruned_up_to_inclusive bigint,
    -- lapi_string_interning ledger-end tracking
    ledger_end_string_interning_id integer,
    ledger_end_publication_time bigint
);

CREATE TABLE lapi_post_processing_end (
    -- null signifies the participant begin
    post_processing_end bigint
);

CREATE TABLE lapi_ledger_end_synchronizer_index (
  synchronizer_id INTEGER PRIMARY KEY not null,
  sequencer_timestamp BIGINT,
  repair_timestamp BIGINT,
  repair_counter BIGINT,
  record_time BIGINT NOT NULL
);

---------------------------------------------------------------------------------------------------
-- Completions
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_command_completions (
    completion_offset bigint not null,
    record_time bigint not null,
    publication_time bigint not null,
    user_id varchar collate "C" not null,
    submitters integer[] not null,
    command_id varchar collate "C" not null,
    -- The update ID is `NULL` for rejected transactions/reassignments.
    update_id varchar collate "C",
    -- The submission ID will be provided by the participant or driver if the user didn't provide one.
    -- Nullable to support historical data.
    submission_id varchar collate "C",
    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the user as one of:
    -- 1. an initial offset
    -- 2. an initial timestamp
    -- 3. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    deduplication_offset bigint,
    deduplication_duration_seconds bigint,
    deduplication_duration_nanos integer,

    -- The three columns below are `NULL` if the completion is for an accepted transaction.
    -- The `rejection_status_details` column contains a Protocol-Buffers-serialized message of type
    -- `daml.platform.index.StatusDetails`, containing the code, message, and further details
    -- (decided by the ledger driver), and may be `NULL` even if the other two columns are set.
    rejection_status_code integer,
    rejection_status_message varchar collate "C",
    rejection_status_details bytea,

    synchronizer_id integer not null,
    message_uuid varchar collate "C",
    is_transaction boolean not null,
    trace_context bytea
);

CREATE INDEX lapi_command_completions_user_id_offset_idx ON lapi_command_completions USING btree (user_id, completion_offset);
CREATE INDEX lapi_command_completions_offset_idx ON lapi_command_completions USING btree (completion_offset);
CREATE INDEX lapi_command_completions_publication_time_idx ON lapi_command_completions USING btree (publication_time, completion_offset);
CREATE INDEX lapi_command_completions_synchronizer_record_time_idx ON lapi_command_completions USING btree (synchronizer_id, record_time);
CREATE INDEX lapi_command_completions_synchronizer_offset_idx ON lapi_command_completions USING btree (synchronizer_id, completion_offset);
---------------------------------------------------------------------------------------------------
-- Events: Assign
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_assign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id varchar collate "C" not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",

    submitter integer,
    node_id integer not null,               -- event metadata

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_name integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders

    -- * common reassignment
    source_synchronizer_id integer not null,
    target_synchronizer_id integer not null,
    unassign_id varchar collate "C" not null,
    reassignment_counter bigint not null,

    -- * assigned specific
    create_argument bytea not null,
    create_signatories integer[] default '{}'::integer[] not null,
    create_observers integer[] default '{}'::integer[] not null,
    create_key_value bytea,
    create_key_hash varchar collate "C",
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

-- index for queries resolving (contract ID, synchronizer id, sequential ID) to sequential IDs.
CREATE INDEX lapi_events_assign_event_contract_id_synchronizer_id_seq_id_idx ON lapi_events_assign USING btree (contract_id, target_synchronizer_id, event_sequential_id);

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
    node_id integer not null,               -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id varchar collate "C" not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",
    user_id varchar collate "C",
    submitters integer[],

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_name integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders
    tree_event_witnesses integer[] default '{}'::integer[] not null, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,          -- used for the mutable state cache

    -- * choice data
    exercise_choice varchar collate "C" not null,
    exercise_argument bytea not null,
    exercise_result bytea,
    exercise_actors integer[] not null,
    exercise_last_descendant_node_id integer not null,

    -- * compression flags
    create_key_value_compression smallint,
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    synchronizer_id integer not null,
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
    node_id integer not null,               -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id varchar collate "C" not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",
    user_id varchar collate "C",
    submitters integer[],

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_name integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders
    tree_event_witnesses integer[] default '{}'::integer[] not null, -- informees

    -- * contract data
    create_argument bytea not null,
    create_signatories integer[] not null,
    create_observers integer[] not null,
    create_key_value bytea,
    create_key_hash varchar collate "C",

    -- * compression flags
    create_argument_compression smallint,
    create_key_value_compression smallint,
    driver_metadata bytea not null,
    synchronizer_id integer not null,
    create_key_maintainers integer[],
    trace_context bytea,
    record_time bigint not null
);

-- lookup by contract_id
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
    node_id integer not null,               -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id varchar collate "C" not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",
    user_id varchar collate "C",
    submitters integer[],

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_name integer not null,
    tree_event_witnesses integer[] default '{}'::integer[] not null, -- informees

    -- * information about the corresponding create event
    create_key_value bytea,     -- used for the mutable state cache

    -- * choice data
    exercise_choice varchar collate "C" not null,
    exercise_argument bytea not null,
    exercise_result bytea,
    exercise_actors integer[] not null,
    exercise_last_descendant_node_id integer not null,

    -- * compression flags
    create_key_value_compression smallint,
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    synchronizer_id integer not null,
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
    event_offset bigint not null,

    -- * transaction metadata
    update_id varchar collate "C" not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",

    submitter integer,
    node_id integer not null,               -- event metadata

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_name integer not null,
    flat_event_witnesses integer[] default '{}'::integer[] not null, -- stakeholders

    -- * common reassignment
    source_synchronizer_id integer not null,
    target_synchronizer_id integer not null,
    unassign_id varchar collate "C" not null,
    reassignment_counter bigint not null,

    -- * unassigned specific
    assignment_exclusivity bigint,

    trace_context bytea,
    record_time bigint not null
);

-- multi-column index supporting per contract per synchronizer lookup before/after sequential id query
CREATE INDEX lapi_events_unassign_contract_id_composite_idx ON lapi_events_unassign USING btree (contract_id, source_synchronizer_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX lapi_events_unassign_event_offset_idx ON lapi_events_unassign USING btree (event_offset, event_sequential_id);

-- sequential_id index for paging
CREATE INDEX lapi_events_unassign_event_sequential_id_idx ON lapi_events_unassign USING btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Topology (participant authorization mappings)
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_party_to_participant (
    event_sequential_id bigint not null,
    event_offset bigint not null,
    update_id varchar collate "C" not null,
    party_id integer not null,
    participant_id varchar collate "C" not null,
    participant_permission integer not null,
    participant_authorization_event integer not null,
    synchronizer_id integer not null,
    record_time bigint not null,
    trace_context bytea
);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_party_to_participant_event_offset_idx ON lapi_events_party_to_participant USING btree (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_party_to_participant_event_sequential_id_idx ON lapi_events_party_to_participant USING btree (event_sequential_id);

-- party_id with event_sequential_id for id queries
CREATE INDEX lapi_events_party_to_participant_event_party_sequential_id_idx ON lapi_events_party_to_participant USING btree (party_id, event_sequential_id);

-- party_id with event_sequential_id for id queries
CREATE INDEX lapi_events_party_to_participant_event_did_recordt_idx ON lapi_events_party_to_participant USING btree (synchronizer_id, record_time);

---------------------------------------------------------------------------------------------------
-- Identity provider configs
--
-- This table stores identity provider records used in the ledger api identity provider config
-- service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_identity_provider_config (
    identity_provider_id varchar collate "C" primary key not null,
    issuer varchar collate "C" not null unique,
    jwks_url varchar collate "C" not null,
    is_deactivated boolean not null,
    audience varchar collate "C"
);

---------------------------------------------------------------------------------------------------
-- Party records
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_records (
    internal_id serial primary key,
    party varchar collate "C" not null unique,
    resource_version bigint not null,
    created_at bigint not null,
    identity_provider_id varchar collate "C" default null references lapi_identity_provider_config (identity_provider_id)
);

---------------------------------------------------------------------------------------------------
-- Party record annotations
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_record_annotations (
    internal_id integer not null references lapi_party_records (internal_id) on delete cascade,
    name varchar collate "C" not null,
    val varchar collate "C",
    updated_at bigint not null,
    unique (internal_id, name)
);

---------------------------------------------------------------------------------------------------
-- Transaction meta information
--
-- This table is used in point-wise lookups.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_transaction_meta (
    update_id varchar collate "C" not null,
    event_offset bigint not null,
    publication_time bigint not null,
    record_time bigint not null,
    synchronizer_id integer not null,
    event_sequential_id_first bigint not null,
    event_sequential_id_last bigint not null
);

CREATE INDEX lapi_transaction_meta_event_offset_idx ON lapi_transaction_meta USING btree (event_offset);
CREATE INDEX lapi_transaction_meta_uid_idx ON lapi_transaction_meta USING btree (update_id);
CREATE INDEX lapi_transaction_meta_publication_time_idx ON lapi_transaction_meta USING btree (publication_time, event_offset);
CREATE INDEX lapi_transaction_meta_synchronizer_record_time_idx ON lapi_transaction_meta USING btree (synchronizer_id, record_time);
CREATE INDEX lapi_transaction_meta_synchronizer_offset_idx ON lapi_transaction_meta USING btree (synchronizer_id, event_offset);

---------------------------------------------------------------------------------------------------
-- User entries
--
-- This table stores user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_users (
    internal_id serial primary key,
    user_id varchar collate "C" not null unique,
    primary_party varchar collate "C",
    created_at bigint not null,
    is_deactivated boolean default false not null,
    resource_version bigint default 0 not null,
    identity_provider_id varchar collate "C" default null references lapi_identity_provider_config (identity_provider_id)
);

---------------------------------------------------------------------------------------------------
-- User rights
--
-- This table stores user rights used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_user_rights (
    user_internal_id integer not null references lapi_users (internal_id) on delete cascade,
    user_right integer not null,
    for_party varchar collate "C",
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
    name varchar collate "C" not null,
    val varchar collate "C",
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
    ledger_offset bigint not null,
    recorded_at bigint not null, --with timezone

    -- SubmissionId for the party allocation
    submission_id varchar collate "C",

    -- party
    party varchar collate "C",

    -- The type of entry, 'accept' or 'reject'
    typ varchar collate "C" not null,

    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar collate "C",

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
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_ts_idx  ON lapi_pe_assign_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_s_idx   ON lapi_pe_assign_id_filter_stakeholder(event_sequential_id);


CREATE TABLE lapi_pe_consuming_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_pts_idx ON lapi_pe_consuming_id_filter_non_stakeholder_informee USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_ps_idx  ON lapi_pe_consuming_id_filter_non_stakeholder_informee USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_ts_idx  ON lapi_pe_consuming_id_filter_non_stakeholder_informee USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_s_idx   ON lapi_pe_consuming_id_filter_non_stakeholder_informee USING btree (event_sequential_id);

CREATE TABLE lapi_pe_consuming_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_ps_idx  ON lapi_pe_consuming_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_pts_idx ON lapi_pe_consuming_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_ts_idx  ON lapi_pe_consuming_id_filter_stakeholder USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_s_idx   ON lapi_pe_consuming_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE lapi_pe_create_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_pts_idx ON lapi_pe_create_id_filter_non_stakeholder_informee USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_ps_idx  ON lapi_pe_create_id_filter_non_stakeholder_informee USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_ts_idx  ON lapi_pe_create_id_filter_non_stakeholder_informee USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_s_idx   ON lapi_pe_create_id_filter_non_stakeholder_informee USING btree (event_sequential_id);

CREATE TABLE lapi_pe_create_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_create_id_filter_stakeholder_ps_idx  ON lapi_pe_create_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_pts_idx ON lapi_pe_create_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_ts_idx  ON lapi_pe_create_id_filter_stakeholder USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_s_idx   ON lapi_pe_create_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE lapi_pe_non_consuming_id_filter_informee (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_non_consuming_id_filter_informee_pts_idx ON lapi_pe_non_consuming_id_filter_informee USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_ps_idx  ON lapi_pe_non_consuming_id_filter_informee USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_ts_idx  ON lapi_pe_non_consuming_id_filter_informee USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_s_idx   ON lapi_pe_non_consuming_id_filter_informee USING btree (event_sequential_id);

CREATE TABLE lapi_pe_unassign_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null
);

CREATE INDEX lapi_pe_unassign_id_filter_stakeholder_ps_idx ON lapi_pe_unassign_id_filter_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_pe_unassign_id_filter_stakeholder_pts_idx ON lapi_pe_unassign_id_filter_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_unassign_id_filter_stakeholder_ts_idx ON lapi_pe_unassign_id_filter_stakeholder USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_pe_unassign_id_filter_stakeholder_s_idx ON lapi_pe_unassign_id_filter_stakeholder USING btree (event_sequential_id);

CREATE TABLE lapi_string_interning (
    internal_id integer primary key not null,
    external_string varchar collate "C"
);
