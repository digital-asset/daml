-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- Parameters
--
-- This table is meant to have a single row storing all the parameters we have.
-- We make sure the following invariant holds:
-- - The ledger_end, ledger_end_sequential_id, ledger_end_string_interning_id and
--   ledger_end_publication_time are always defined at the same time. I.e., either
--   all are NULL, or all are defined.
---------------------------------------------------------------------------------------------------
create table lapi_parameters (
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
    -- lapi_string_interning ledger-end tracking
    ledger_end_string_interning_id integer,
    ledger_end_publication_time bigint
);

create table lapi_post_processing_end (
    -- null signifies the participant begin
    post_processing_end bigint
);

create table lapi_ledger_end_synchronizer_index (
  synchronizer_id integer primary key not null,
  sequencer_timestamp bigint,
  repair_timestamp bigint,
  repair_counter bigint,
  record_time bigint not null
);

---------------------------------------------------------------------------------------------------
-- Completions
---------------------------------------------------------------------------------------------------
create table lapi_command_completions (
    completion_offset bigint not null,
    record_time bigint not null,
    publication_time bigint not null,
    user_id integer not null,
    submitters bytea not null,
    command_id varchar collate "C" not null,
    -- The update ID is `NULL` for rejected transactions/reassignments.
    update_id bytea,
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
    trace_context bytea not null
);

create index lapi_command_completions_user_id_offset_idx on lapi_command_completions using btree (user_id, completion_offset);
create index lapi_command_completions_offset_idx on lapi_command_completions using btree (completion_offset);
create index lapi_command_completions_publication_time_idx on lapi_command_completions using btree (publication_time, completion_offset);
create index lapi_command_completions_synchronizer_record_time_offset_idx on lapi_command_completions using btree (synchronizer_id, record_time, completion_offset);
create index lapi_command_completions_synchronizer_offset_idx on lapi_command_completions using btree (synchronizer_id, completion_offset);

---------------------------------------------------------------------------------------------------
-- Events: Activate Contract
---------------------------------------------------------------------------------------------------
create table lapi_events_activate_contract (
   -- update related columns
   event_offset bigint not null,
   update_id bytea not null,
   workflow_id varchar collate "C",
   command_id varchar collate "C",
   submitters bytea,
   record_time bigint not null,
   synchronizer_id integer not null,
   trace_context bytea not null,
   external_transaction_hash bytea,

   -- event related columns
   event_type smallint not null, -- all event types
   event_sequential_id bigint not null, -- all event types
   node_id integer not null, -- all event types
   additional_witnesses bytea, -- create events
   source_synchronizer_id integer, -- assign events
   reassignment_counter bigint, -- assign events
   reassignment_id bytea, -- assign events
   representative_package_id integer not null, -- create events

   -- contract related columns
   internal_contract_id bigint not null, -- all event types
   create_key_hash varchar collate "C" -- create
);

-- sequential_id index
create index lapi_events_activate_sequential_id_idx on lapi_events_activate_contract using btree (event_sequential_id) include (event_type, synchronizer_id);
-- event_offset index
create index lapi_events_activate_offset_idx on lapi_events_activate_contract using btree (event_offset);
-- internal_contract_id index
create index lapi_events_activate_internal_contract_id_idx on lapi_events_activate_contract using btree (internal_contract_id, event_sequential_id);
-- contract_key index
create index lapi_events_activate_contract_key_idx on lapi_events_activate_contract using btree (create_key_hash, event_sequential_id) where create_key_hash is not null;

-- filter table for stakeholders
create table lapi_filter_activate_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_activate_stakeholder_ps_idx  on lapi_filter_activate_stakeholder using btree (party_id, event_sequential_id);
create index lapi_filter_activate_stakeholder_pts_idx on lapi_filter_activate_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_activate_stakeholder_ts_idx  on lapi_filter_activate_stakeholder using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_filter_activate_stakeholder_s_idx   on lapi_filter_activate_stakeholder using btree (event_sequential_id, first_per_sequential_id);

-- filter table for additional witnesses
create table lapi_filter_activate_witness (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_activate_witness_ps_idx  on lapi_filter_activate_witness using btree (party_id, event_sequential_id);
create index lapi_filter_activate_witness_pts_idx on lapi_filter_activate_witness using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_activate_witness_ts_idx  on lapi_filter_activate_witness using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_filter_activate_witness_s_idx   on lapi_filter_activate_witness using btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Deactivate Contract
---------------------------------------------------------------------------------------------------
create table lapi_events_deactivate_contract (
   -- update related columns
   event_offset bigint not null,
   update_id bytea not null,
   workflow_id varchar collate "C",
   command_id varchar collate "C",
   submitters bytea,
   record_time bigint not null,
   synchronizer_id integer not null,
   trace_context bytea not null,
   external_transaction_hash bytea,

   -- event related columns
   event_type smallint not null, -- all event types
   event_sequential_id bigint not null, -- all event types
   node_id integer not null, -- all event types
   deactivated_event_sequential_id bigint, -- all event types
   additional_witnesses bytea, -- consuming events
   exercise_choice integer, -- consuming events
   exercise_choice_interface integer, -- consuming events
   exercise_argument bytea, -- consuming events
   exercise_result bytea, -- consuming events
   exercise_actors bytea, -- consuming events
   exercise_last_descendant_node_id integer, -- consuming events
   exercise_argument_compression smallint, -- consuming events
   exercise_result_compression smallint, -- consuming events
   reassignment_id bytea, -- unassign events
   assignment_exclusivity bigint, -- unassign events
   target_synchronizer_id integer, -- unassign events
   reassignment_counter bigint, -- unassign events

   -- contract related columns
   contract_id bytea, -- all event types
   internal_contract_id bigint, -- all event types
   template_id integer not null, -- all event types
   package_id integer not null, -- all event types
   stakeholders bytea not null, -- all event types
   ledger_effective_time bigint -- consuming events
);

-- sequential_id index
create index lapi_events_deactivate_sequential_id_idx on lapi_events_deactivate_contract using btree (event_sequential_id) include (event_type);
-- event_offset index
create index lapi_events_deactivate_offset_idx on lapi_events_deactivate_contract using btree (event_offset);
-- internal_contract_id index
create index lapi_events_deactivate_internal_contract_id_idx on lapi_events_deactivate_contract using btree (internal_contract_id, event_sequential_id) where internal_contract_id is not null;
-- internal_contract_id index serving only the consuming exercises (PersistentEventType.ConsumingExercise)
-- this index is needed by batched contract lookups for interpretation and event_query_service
create index lapi_events_deactivate_internal_contract_id_archive_idx on lapi_events_deactivate_contract using btree (internal_contract_id, event_sequential_id) where internal_contract_id is not null and event_type = 3;
-- deactivation reference index
create index lapi_events_deactivated_event_sequential_id_idx on lapi_events_deactivate_contract using btree (deactivated_event_sequential_id) include (event_sequential_id) where deactivated_event_sequential_id is not null;

-- filter table for stakeholders
create table lapi_filter_deactivate_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_deactivate_stakeholder_ps_idx  on lapi_filter_deactivate_stakeholder using btree (party_id, event_sequential_id);
create index lapi_filter_deactivate_stakeholder_pts_idx on lapi_filter_deactivate_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_deactivate_stakeholder_ts_idx  on lapi_filter_deactivate_stakeholder using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_filter_deactivate_stakeholder_s_idx   on lapi_filter_deactivate_stakeholder using btree (event_sequential_id, first_per_sequential_id);

-- filter table for additional witnesses
create table lapi_filter_deactivate_witness (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_deactivate_witness_ps_idx  on lapi_filter_deactivate_witness using btree (party_id, event_sequential_id);
create index lapi_filter_deactivate_witness_pts_idx on lapi_filter_deactivate_witness using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_deactivate_witness_ts_idx  on lapi_filter_deactivate_witness using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_filter_deactivate_witness_s_idx   on lapi_filter_deactivate_witness using btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Various Witnessed
---------------------------------------------------------------------------------------------------
create table lapi_events_various_witnessed (
   -- tx related columns
   event_offset bigint not null,
   update_id bytea not null,
   workflow_id varchar collate "C",
   command_id varchar collate "C",
   submitters bytea,
   record_time bigint not null,
   synchronizer_id integer not null,
   trace_context bytea not null,
   external_transaction_hash bytea,

   -- event related columns
   event_type smallint not null, -- all event types
   event_sequential_id bigint not null, -- all event types
   node_id integer not null, -- all event types
   additional_witnesses bytea not null, -- all event types
   consuming boolean, -- exercise
   exercise_choice integer, -- exercise
   exercise_choice_interface integer, -- exercise
   exercise_argument bytea, -- exercise
   exercise_result bytea, -- exercise
   exercise_actors bytea, -- exercise
   exercise_last_descendant_node_id integer, -- exercise
   exercise_argument_compression smallint, -- exercise
   exercise_result_compression smallint, -- exercise
   representative_package_id integer, -- create events

   -- contract related columns
   contract_id bytea,
   internal_contract_id bigint,
   template_id integer,
   package_id integer,
   ledger_effective_time bigint
);

-- sequential_id index
create index lapi_events_various_sequential_id_idx on lapi_events_various_witnessed using btree (event_sequential_id) include (event_type);
-- event_offset index
create index lapi_events_various_offset_idx on lapi_events_various_witnessed using btree (event_offset);
-- internal_contract_id index
create index lapi_events_various_internal_contract_id_idx on lapi_events_various_witnessed using btree (internal_contract_id, event_sequential_id) where internal_contract_id is not null;

-- filter table for additional witnesses
create table lapi_filter_various_witness (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_various_witness_ps_idx  on lapi_filter_various_witness using btree (party_id, event_sequential_id);
create index lapi_filter_various_witness_pts_idx on lapi_filter_various_witness using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_various_witness_ts_idx  on lapi_filter_various_witness using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_filter_various_witness_s_idx   on lapi_filter_various_witness using btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Assign
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_assign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id bytea not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",

    submitter integer,
    node_id integer not null,               -- event metadata

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_id integer not null,
    flat_event_witnesses bytea not null, -- stakeholders

    -- * common reassignment
    source_synchronizer_id integer not null,
    target_synchronizer_id integer not null,
    reassignment_id bytea not null,
    reassignment_counter bigint not null,

    -- * assigned specific
    create_argument bytea not null,
    create_signatories bytea not null,
    create_observers bytea not null,
    create_key_value bytea,
    create_key_hash varchar collate "C",
    create_argument_compression smallint,
    create_key_value_compression smallint,
    ledger_effective_time bigint not null,
    authentication_data bytea not null,

    create_key_maintainers bytea,
    trace_context bytea not null,
    record_time bigint not null,
    internal_contract_id bigint not null
);

-- index for queries resolving contract ID to sequential IDs.
create index lapi_events_assign_event_contract_id_idx on lapi_events_assign using btree (contract_id, event_sequential_id);

-- index for queries resolving (contract ID, synchronizer id, sequential ID) to sequential IDs.
create index lapi_events_assign_event_contract_id_synchronizer_id_seq_id_idx on lapi_events_assign using btree (contract_id, target_synchronizer_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
create index lapi_events_assign_event_offset_idx on lapi_events_assign using btree (event_offset, event_sequential_id);

-- sequential_id index for paging
create index lapi_events_assign_event_sequential_id_idx on lapi_events_assign using btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: consuming exercise
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,  -- transaction metadata
    node_id integer not null,               -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id bytea not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",
    submitters bytea,

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_id integer not null,
    flat_event_witnesses bytea not null, -- stakeholders
    tree_event_witnesses bytea not null, -- informees

    -- * choice data
    exercise_choice integer not null,
    exercise_choice_interface integer,
    exercise_argument bytea not null,
    exercise_result bytea,
    exercise_actors bytea not null,
    exercise_last_descendant_node_id integer not null,

    -- * compression flags
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    synchronizer_id integer not null,
    trace_context bytea not null,
    record_time bigint not null,
    external_transaction_hash bytea,
    deactivated_event_sequential_id bigint
);

-- lookup by deactivation
create index lapi_events_consuming_exercise_deactivated_id_idx on lapi_events_consuming_exercise using btree (deactivated_event_sequential_id) include (event_sequential_id) where deactivated_event_sequential_id is not null;

-- lookup by contract id
create index lapi_events_consuming_exercise_contract_id_idx on lapi_events_consuming_exercise using hash (contract_id);

-- offset index: used to translate to sequential_id
create index lapi_events_consuming_exercise_event_offset_idx on lapi_events_consuming_exercise using btree (event_offset);

-- sequential_id index for paging
create index lapi_events_consuming_exercise_event_sequential_id_idx on lapi_events_consuming_exercise using btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: create
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_create (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,  -- transaction metadata
    node_id integer not null,               -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id bytea not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",
    submitters bytea,

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_id integer not null,
    representative_package_id integer not null,
    flat_event_witnesses bytea not null, -- stakeholders
    tree_event_witnesses bytea not null, -- informees

    -- * contract data
    create_argument bytea not null,
    create_signatories bytea not null,
    create_observers bytea not null,
    create_key_value bytea,
    create_key_hash varchar collate "C",

    -- * compression flags
    create_argument_compression smallint,
    create_key_value_compression smallint,
    authentication_data bytea not null,
    synchronizer_id integer not null,
    create_key_maintainers bytea,
    trace_context bytea not null,
    record_time bigint not null,
    external_transaction_hash bytea,
    internal_contract_id bigint not null
);

-- lookup by contract_id
create index lapi_events_create_contract_id_idx on lapi_events_create using hash (contract_id);

-- lookup by contract_key
create index lapi_events_create_create_key_hash_idx on lapi_events_create using btree (create_key_hash, event_sequential_id) where create_key_hash is not null;

-- offset index: used to translate to sequential_id
create index lapi_events_create_event_offset_idx on lapi_events_create using btree (event_offset);

-- sequential_id index for paging
create index lapi_events_create_event_sequential_id_idx on lapi_events_create using btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: non-consuming exercise
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_non_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,  -- transaction metadata
    node_id integer not null,               -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id bytea not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",
    submitters bytea,

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_id integer not null,
    tree_event_witnesses bytea not null, -- informees

    -- * choice data
    exercise_choice integer not null,
    exercise_choice_interface integer,
    exercise_argument bytea not null,
    exercise_result bytea,
    exercise_actors bytea not null,
    exercise_last_descendant_node_id integer not null,

    -- * compression flags
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    synchronizer_id integer not null,
    trace_context bytea not null,
    record_time bigint not null,
    external_transaction_hash bytea
);

-- offset index: used to translate to sequential_id
create index lapi_events_non_consuming_exercise_event_offset_idx on lapi_events_non_consuming_exercise using btree (event_offset);

-- sequential_id index for paging
create index lapi_events_non_consuming_exercise_event_sequential_id_idx on lapi_events_non_consuming_exercise using btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Unassign
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_unassign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,    -- event identification: same ordering as event_offset

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id bytea not null,
    workflow_id varchar collate "C",

    -- * submitter info (only visible on submitting participant)
    command_id varchar collate "C",

    submitter integer,
    node_id integer not null,               -- event metadata

    -- * shared event information
    contract_id bytea not null,
    template_id integer not null,
    package_id integer not null,
    flat_event_witnesses bytea not null, -- stakeholders

    -- * common reassignment
    source_synchronizer_id integer not null,
    target_synchronizer_id integer not null,
    reassignment_id bytea not null,
    reassignment_counter bigint not null,

    -- * unassigned specific
    assignment_exclusivity bigint,

    trace_context bytea not null,
    record_time bigint not null,
    deactivated_event_sequential_id bigint
);

-- deactivations
create index lapi_events_unassign_deactivated_idx on lapi_events_unassign using btree (deactivated_event_sequential_id) include (event_sequential_id) where deactivated_event_sequential_id is not null;

-- multi-column index supporting per contract per synchronizer lookup before/after sequential id query
create index lapi_events_unassign_contract_id_composite_idx on lapi_events_unassign using btree (contract_id, source_synchronizer_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
create index lapi_events_unassign_event_offset_idx on lapi_events_unassign using btree (event_offset, event_sequential_id);

-- sequential_id index for paging
create index lapi_events_unassign_event_sequential_id_idx on lapi_events_unassign using btree (event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Topology (participant authorization mappings)
---------------------------------------------------------------------------------------------------
create table lapi_events_party_to_participant (
    event_sequential_id bigint not null,
    event_offset bigint not null,
    update_id bytea not null,
    party_id integer not null,
    participant_id integer not null,
    participant_permission integer not null,
    participant_authorization_event integer not null,
    synchronizer_id integer not null,
    record_time bigint not null,
    trace_context bytea not null
);

-- offset index: used to translate to sequential_id
create index lapi_events_party_to_participant_event_offset_idx on lapi_events_party_to_participant using btree (event_offset);

-- sequential_id index for paging
create index lapi_events_party_to_participant_event_sequential_id_idx on lapi_events_party_to_participant using btree (event_sequential_id);

-- party_id with event_sequential_id for id queries
create index lapi_events_party_to_participant_event_party_sequential_id_idx on lapi_events_party_to_participant using btree (party_id, event_sequential_id);

-- party_id with event_sequential_id for id queries
create index lapi_events_party_to_participant_event_did_recordt_idx on lapi_events_party_to_participant using btree (synchronizer_id, record_time);

---------------------------------------------------------------------------------------------------
-- Identity provider configs
--
-- This table stores identity provider records used in the ledger api identity provider config
-- service.
---------------------------------------------------------------------------------------------------
create table lapi_identity_provider_config (
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
create table lapi_party_records (
    internal_id integer generated always as identity primary key,
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
create table lapi_party_record_annotations (
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
create table lapi_update_meta (
    update_id bytea not null,
    event_offset bigint not null,
    publication_time bigint not null,
    record_time bigint not null,
    synchronizer_id integer not null,
    event_sequential_id_first bigint not null,
    event_sequential_id_last bigint not null
);

create index lapi_update_meta_event_offset_idx on lapi_update_meta using btree (event_offset);
create index lapi_update_meta_uid_idx on lapi_update_meta using hash (update_id);
create index lapi_update_meta_publication_time_idx on lapi_update_meta using btree (publication_time, event_offset);
create index lapi_update_meta_synchronizer_record_time_offset_idx on lapi_update_meta using btree (synchronizer_id, record_time, event_offset);
create index lapi_update_meta_synchronizer_offset_idx on lapi_update_meta using btree (synchronizer_id, event_offset);

---------------------------------------------------------------------------------------------------
-- User entries
--
-- This table stores user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
create table lapi_users (
    internal_id integer generated always as identity primary key,
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
create table lapi_user_rights (
    user_internal_id integer not null references lapi_users (internal_id) on delete cascade,
    user_right integer not null,
    for_party varchar collate "C",
    granted_at bigint not null,
    unique (user_internal_id, user_right, for_party)
);

create unique index lapi_user_rights_user_internal_id_user_right_idx
    on lapi_user_rights using btree (user_internal_id, user_right)
    where (for_party is null);

insert into lapi_users(user_id, primary_party, created_at) values ('participant_admin', null, 0);
insert into lapi_user_rights(user_internal_id, user_right, for_party, granted_at)
select internal_id, 1, null, 0
from lapi_users
where user_id = 'participant_admin';

---------------------------------------------------------------------------------------------------
-- User annotations
--
-- This table stores additional per user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
create table lapi_user_annotations (
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
create table lapi_party_entries (
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
create index lapi_party_entries_idx on lapi_party_entries using btree (submission_id);

create index lapi_party_entries_party_and_ledger_offset_idx on lapi_party_entries using btree (party, ledger_offset);

create index lapi_party_entries_party_id_and_ledger_offset_idx on lapi_party_entries using btree (party_id, ledger_offset);

-- deprecated, to be removed. See #28008
create table lapi_pe_assign_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_pe_assign_id_filter_stakeholder_pts_idx on lapi_pe_assign_id_filter_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_pe_assign_id_filter_stakeholder_ps_idx  on lapi_pe_assign_id_filter_stakeholder using btree (party_id, event_sequential_id);
create index lapi_pe_assign_id_filter_stakeholder_ts_idx  on lapi_pe_assign_id_filter_stakeholder using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_pe_assign_id_filter_stakeholder_s_idx   on lapi_pe_assign_id_filter_stakeholder using btree (event_sequential_id, first_per_sequential_id);


-- deprecated, to be removed. See #28008
create table lapi_pe_consuming_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);

create index lapi_pe_consuming_id_filter_non_stakeholder_informee_pts_idx on lapi_pe_consuming_id_filter_non_stakeholder_informee using btree (party_id, template_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_non_stakeholder_informee_ps_idx  on lapi_pe_consuming_id_filter_non_stakeholder_informee using btree (party_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_non_stakeholder_informee_ts_idx  on lapi_pe_consuming_id_filter_non_stakeholder_informee using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_pe_consuming_id_filter_non_stakeholder_informee_s_idx   on lapi_pe_consuming_id_filter_non_stakeholder_informee using btree (event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_consuming_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);

create index lapi_pe_consuming_id_filter_stakeholder_ps_idx  on lapi_pe_consuming_id_filter_stakeholder using btree (party_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_stakeholder_pts_idx on lapi_pe_consuming_id_filter_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_stakeholder_ts_idx  on lapi_pe_consuming_id_filter_stakeholder using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_pe_consuming_id_filter_stakeholder_s_idx   on lapi_pe_consuming_id_filter_stakeholder using btree (event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_create_id_filter_non_stakeholder_informee (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);

create index lapi_pe_create_id_filter_non_stakeholder_informee_pts_idx on lapi_pe_create_id_filter_non_stakeholder_informee using btree (party_id, template_id, event_sequential_id);
create index lapi_pe_create_id_filter_non_stakeholder_informee_ps_idx  on lapi_pe_create_id_filter_non_stakeholder_informee using btree (party_id, event_sequential_id);
create index lapi_pe_create_id_filter_non_stakeholder_informee_ts_idx  on lapi_pe_create_id_filter_non_stakeholder_informee using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_pe_create_id_filter_non_stakeholder_informee_s_idx   on lapi_pe_create_id_filter_non_stakeholder_informee using btree (event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_create_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);

create index lapi_pe_create_id_filter_stakeholder_ps_idx  on lapi_pe_create_id_filter_stakeholder using btree (party_id, event_sequential_id);
create index lapi_pe_create_id_filter_stakeholder_pts_idx on lapi_pe_create_id_filter_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_pe_create_id_filter_stakeholder_ts_idx  on lapi_pe_create_id_filter_stakeholder using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_pe_create_id_filter_stakeholder_s_idx   on lapi_pe_create_id_filter_stakeholder using btree (event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_non_consuming_id_filter_informee (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);

create index lapi_pe_non_consuming_id_filter_informee_pts_idx on lapi_pe_non_consuming_id_filter_informee using btree (party_id, template_id, event_sequential_id);
create index lapi_pe_non_consuming_id_filter_informee_ps_idx  on lapi_pe_non_consuming_id_filter_informee using btree (party_id, event_sequential_id);
create index lapi_pe_non_consuming_id_filter_informee_ts_idx  on lapi_pe_non_consuming_id_filter_informee using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_pe_non_consuming_id_filter_informee_s_idx   on lapi_pe_non_consuming_id_filter_informee using btree (event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_reassignment_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);

create index lapi_pe_reassignment_id_filter_stakeholder_ps_idx on lapi_pe_reassignment_id_filter_stakeholder using btree (party_id, event_sequential_id);
create index lapi_pe_reassignment_id_filter_stakeholder_pts_idx on lapi_pe_reassignment_id_filter_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_pe_reassignment_id_filter_stakeholder_ts_idx on lapi_pe_reassignment_id_filter_stakeholder using btree (template_id, event_sequential_id) where first_per_sequential_id;
create index lapi_pe_reassignment_id_filter_stakeholder_s_idx on lapi_pe_reassignment_id_filter_stakeholder using btree (event_sequential_id, first_per_sequential_id);

create table lapi_string_interning (
    internal_id integer primary key not null,
    external_string varchar collate "C"
);
