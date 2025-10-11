-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create alias array_intersection for "com.digitalasset.canton.store.db.h2.H2FunctionAliases.arrayIntersection";

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
  participant_id varchar not null,
  ledger_end bigint,
  ledger_end_sequential_id bigint,
  ledger_end_string_interning_id integer,
  ledger_end_publication_time bigint,
  participant_pruned_up_to_inclusive bigint
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
-- Party entries
--
-- A table for tracking party allocation submissions
---------------------------------------------------------------------------------------------------
create table lapi_party_entries (
    ledger_offset bigint not null,
    recorded_at bigint not null,
    submission_id varchar,
    party varchar,
    typ varchar not null,
    rejection_reason varchar,
    is_local boolean,
    party_id integer,

    constraint check_party_entry_type
        check (
          (typ = 'accept' and rejection_reason is null) or
          (typ = 'reject' and rejection_reason is not null)
        )
);

create index lapi_party_entries_idx on lapi_party_entries (submission_id);
create index lapi_party_entries_party_and_ledger_offset_idx on lapi_party_entries(party, ledger_offset);
create index lapi_party_entries_party_id_and_ledger_offset_idx on lapi_party_entries(party_id, ledger_offset);

---------------------------------------------------------------------------------------------------
-- Completions
---------------------------------------------------------------------------------------------------
create table lapi_command_completions (
    completion_offset bigint not null,
    record_time bigint not null,
    publication_time bigint not null,
    user_id integer not null,
    submitters binary large object not null,
    command_id varchar not null,
    -- The update ID is `NULL` for rejected transactions/reassignments.
    update_id binary varying,
    -- The submission ID will be provided by the participant or driver if the application didn't provide one.
    -- Nullable to support historical data.
    submission_id varchar,
    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the application as one of:
    -- 1. an initial offset
    -- 2. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    -- 3. an initial timestamp
    deduplication_offset bigint,
    deduplication_duration_seconds bigint,
    deduplication_duration_nanos int,
    -- The three columns below are `NULL` if the completion is for an accepted transaction.
    -- The `rejection_status_details` column contains a Protocol-Buffers-serialized message of type
    -- `daml.platform.index.StatusDetails`, containing the code, message, and further details
    -- (decided by the ledger driver), and may be `NULL` even if the other two columns are set.
    rejection_status_code integer,
    rejection_status_message varchar,
    rejection_status_details binary large object,
    synchronizer_id integer not null,
    message_uuid varchar,
    is_transaction boolean not null,
    trace_context binary large object not null
);

create index lapi_command_completions_user_id_offset_idx on lapi_command_completions using btree (user_id, completion_offset);
create index lapi_command_completions_offset_idx on lapi_command_completions using btree (completion_offset);
create index lapi_command_completions_publication_time_idx on lapi_command_completions using btree (publication_time, completion_offset);
create index lapi_command_completions_synchronizer_record_time_idx on lapi_command_completions using btree (synchronizer_id, record_time);
create index lapi_command_completions_synchronizer_offset_idx on lapi_command_completions using btree (synchronizer_id, completion_offset);

---------------------------------------------------------------------------------------------------
-- Events: Activate Contract
---------------------------------------------------------------------------------------------------
create table lapi_events_activate_contract (
   -- update related columns
   event_offset bigint not null,
   update_id binary large object not null,
   workflow_id varchar,
   command_id varchar,
   submitters binary large object,
   record_time bigint not null,
   synchronizer_id integer not null,
   trace_context binary large object not null,
   external_transaction_hash binary large object,

   -- event related columns
   event_type smallint not null, -- all event types
   event_sequential_id bigint not null, -- all event types
   node_id integer not null, -- all event types
   additional_witnesses binary large object, -- create events
   source_synchronizer_id integer, -- assign events
   reassignment_counter bigint, -- assign events
   reassignment_id binary large object, -- assign events
   representative_package_id integer not null, -- create events

   -- contract related columns
   internal_contract_id bigint not null, -- all event types
   create_key_hash varchar -- create
);

-- sequential_id index
create index lapi_events_activate_sequential_id_idx on lapi_events_activate_contract using btree (event_sequential_id);
-- event_offset index
create index lapi_events_activate_offset_idx on lapi_events_activate_contract using btree (event_offset);
-- internal_contract_id index
create index lapi_events_activate_internal_contract_id_idx on lapi_events_activate_contract using btree (internal_contract_id, event_sequential_id);
-- contract_key index
create index lapi_events_activate_contract_key_idx on lapi_events_activate_contract using btree (create_key_hash, event_sequential_id);

-- filter table for stakeholders
create table lapi_filter_activate_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_activate_stakeholder_ps_idx  on lapi_filter_activate_stakeholder using btree (party_id, event_sequential_id);
create index lapi_filter_activate_stakeholder_pts_idx on lapi_filter_activate_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_activate_stakeholder_ts_idx  on lapi_filter_activate_stakeholder using btree (template_id, event_sequential_id);
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
create index lapi_filter_activate_witness_ts_idx  on lapi_filter_activate_witness using btree (template_id, event_sequential_id);
create index lapi_filter_activate_witness_s_idx   on lapi_filter_activate_witness using btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Deactivate Contract
---------------------------------------------------------------------------------------------------
create table lapi_events_deactivate_contract (
   -- update related columns
   event_offset bigint not null,
   update_id binary large object not null,
   workflow_id varchar,
   command_id varchar,
   submitters binary large object,
   record_time bigint not null,
   synchronizer_id integer not null,
   trace_context binary large object not null,
   external_transaction_hash binary large object,

   -- event related columns
   event_type smallint not null, -- all event types
   event_sequential_id bigint not null, -- all event types
   node_id integer not null, -- all event types
   deactivated_event_sequential_id bigint, -- all event types
   additional_witnesses binary large object, -- consuming events
   exercise_choice integer, -- consuming events
   exercise_choice_interface integer, -- consuming events
   exercise_argument binary large object, -- consuming events
   exercise_result binary large object, -- consuming events
   exercise_actors binary large object, -- consuming events
   exercise_last_descendant_node_id integer, -- consuming events
   exercise_argument_compression smallint, -- consuming events
   exercise_result_compression smallint, -- consuming events
   reassignment_id binary large object, -- unassign events
   assignment_exclusivity bigint, -- unassign events
   target_synchronizer_id integer, -- unassign events
   reassignment_counter bigint, -- unassign events

   -- contract related columns
   contract_id binary large object not null, -- all event types
   internal_contract_id bigint not null, -- all event types
   template_id integer not null, -- all event types
   package_id integer not null, -- all event types
   stakeholders binary large object not null, -- all event types
   ledger_effective_time bigint -- consuming events
);

-- sequential_id index
create index lapi_events_deactivate_sequential_id_idx on lapi_events_deactivate_contract using btree (event_sequential_id);
-- event_offset index
create index lapi_events_deactivate_offset_idx on lapi_events_deactivate_contract using btree (event_offset);
-- internal_contract_id index
create index lapi_events_deactivate_internal_contract_id_idx on lapi_events_deactivate_contract using btree (internal_contract_id, event_sequential_id);
-- deactivation reference index
create index lapi_events_deactivated_event_sequential_id_idx on lapi_events_deactivate_contract using btree (deactivated_event_sequential_id);

-- filter table for stakeholders
create table lapi_filter_deactivate_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_deactivate_stakeholder_ps_idx  on lapi_filter_deactivate_stakeholder using btree (party_id, event_sequential_id);
create index lapi_filter_deactivate_stakeholder_pts_idx on lapi_filter_deactivate_stakeholder using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_deactivate_stakeholder_ts_idx  on lapi_filter_deactivate_stakeholder using btree (template_id, event_sequential_id);
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
create index lapi_filter_deactivate_witness_ts_idx  on lapi_filter_deactivate_witness using btree (template_id, event_sequential_id);
create index lapi_filter_deactivate_witness_s_idx   on lapi_filter_deactivate_witness using btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Various Witnessed
---------------------------------------------------------------------------------------------------
create table lapi_events_various_witnessed (
   -- tx related columns
   event_offset bigint not null,
   update_id binary large object not null,
   workflow_id varchar,
   command_id varchar,
   submitters binary large object,
   record_time bigint not null,
   synchronizer_id integer not null,
   trace_context binary large object not null,
   external_transaction_hash binary large object,

   -- event related columns
   event_type smallint not null, -- all event types
   event_sequential_id bigint not null, -- all event types
   node_id integer not null, -- all event types
   additional_witnesses binary large object, -- all event types
   consuming boolean, -- exercise
   exercise_choice integer, -- exercise
   exercise_choice_interface integer, -- exercise
   exercise_argument binary large object, -- exercise
   exercise_result binary large object, -- exercise
   exercise_actors binary large object, -- exercise
   exercise_last_descendant_node_id integer, -- exercise
   exercise_argument_compression smallint, -- exercise
   exercise_result_compression smallint, -- exercise
   representative_package_id integer, -- create events

   -- contract related columns
   contract_id binary large object,
   internal_contract_id bigint,
   template_id integer,
   package_id integer,
   ledger_effective_time bigint
);

-- sequential_id index
create index lapi_events_various_sequential_id_idx on lapi_events_various_witnessed using btree (event_sequential_id);
-- event_offset index
create index lapi_events_various_offset_idx on lapi_events_various_witnessed using btree (event_offset);
-- internal_contract_id index
create index lapi_events_various_internal_contract_id_idx on lapi_events_various_witnessed using btree (internal_contract_id, event_sequential_id);

-- filter table for additional witnesses
create table lapi_filter_various_witness (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_filter_various_witness_ps_idx  on lapi_filter_various_witness using btree (party_id, event_sequential_id);
create index lapi_filter_various_witness_pts_idx on lapi_filter_various_witness using btree (party_id, template_id, event_sequential_id);
create index lapi_filter_various_witness_ts_idx  on lapi_filter_various_witness using btree (template_id, event_sequential_id);
create index lapi_filter_various_witness_s_idx   on lapi_filter_various_witness using btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: create
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_create (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,    -- transaction metadata
    node_id integer not null,                 -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id binary varying not null,
    workflow_id varchar,

    -- * submitter info (only visible on submitting participant)
    command_id varchar,
    submitters binary large object,

    -- * shared event information
    contract_id binary varying not null,
    template_id integer not null,
    package_id integer not null,
    representative_package_id integer not null,
    flat_event_witnesses binary large object not null, -- stakeholders
    tree_event_witnesses binary large object not null, -- informees

    -- * contract data
    create_argument binary large object not null,
    create_signatories binary large object not null,
    create_observers binary large object not null,
    create_key_value binary large object,
    create_key_hash varchar,
    create_key_maintainers binary large object,

    -- * compression flags
    create_argument_compression smallint,
    create_key_value_compression smallint,

    -- * contract authentication data
    authentication_data binary large object not null,

    synchronizer_id integer not null,
    trace_context binary large object not null,
    record_time bigint not null,
    external_transaction_hash  binary large object,
    internal_contract_id bigint not null
);

-- offset index: used to translate to sequential_id
create index lapi_events_create_event_offset_idx on lapi_events_create (event_offset);

-- sequential_id index for paging
create index lapi_events_create_event_sequential_id_idx on lapi_events_create (event_sequential_id);

-- lookup by contract_id
create index lapi_events_create_contract_id_idx on lapi_events_create (contract_id);

-- lookup by contract_key
create index lapi_events_create_create_key_hash_idx on lapi_events_create (create_key_hash, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: consuming exercise
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,    -- transaction metadata
    node_id integer not null,                 -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id binary varying not null,
    workflow_id varchar,

    -- * submitter info (only visible on submitting participant)
    command_id varchar,
    submitters binary large object,

    -- * shared event information
    contract_id binary varying not null,
    template_id integer not null,
    package_id integer not null,
    flat_event_witnesses binary large object not null, -- stakeholders
    tree_event_witnesses binary large object not null, -- informees

    -- * choice data
    exercise_choice integer not null,
    exercise_choice_interface integer,
    exercise_argument binary large object not null,
    exercise_result binary large object,
    exercise_actors binary large object not null,
    exercise_last_descendant_node_id integer not null,

    -- * compression flags
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    synchronizer_id integer not null,
    trace_context binary large object not null,
    record_time bigint not null,
    external_transaction_hash  binary large object,
    deactivated_event_sequential_id bigint
);

-- deactivations
create index lapi_events_consuming_exercise_deactivated_idx on lapi_events_consuming_exercise (deactivated_event_sequential_id, event_sequential_id);

-- offset index: used to translate to sequential_id
create index lapi_events_consuming_exercise_event_offset_idx on lapi_events_consuming_exercise (event_offset);

-- sequential_id index for paging
create index lapi_events_consuming_exercise_event_sequential_id_idx on lapi_events_consuming_exercise (event_sequential_id);

-- lookup by contract id
create index lapi_events_consuming_exercise_contract_id_idx on lapi_events_consuming_exercise (contract_id);

---------------------------------------------------------------------------------------------------
-- Events: non-consuming exercise
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_non_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint not null,    -- transaction metadata
    node_id integer not null,                 -- event metadata

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id binary varying not null,
    workflow_id varchar,

    -- * submitter info (only visible on submitting participant)
    command_id varchar,
    submitters binary large object,

    -- * shared event information
    contract_id binary varying not null,
    template_id integer not null,
    package_id integer not null,
    tree_event_witnesses binary large object not null, -- informees

    -- * choice data
    exercise_choice integer not null,
    exercise_choice_interface integer,
    exercise_argument binary large object not null,
    exercise_result binary large object,
    exercise_actors binary large object not null,
    exercise_last_descendant_node_id integer not null,

    -- * compression flags
    exercise_argument_compression smallint,
    exercise_result_compression smallint,

    synchronizer_id integer not null,
    trace_context binary large object not null,
    record_time bigint not null,
    external_transaction_hash  binary large object
);

-- offset index: used to translate to sequential_id
create index lapi_events_non_consuming_exercise_event_offset_idx on lapi_events_non_consuming_exercise (event_offset);

-- sequential_id index for paging
create index lapi_events_non_consuming_exercise_event_sequential_id_idx on lapi_events_non_consuming_exercise (event_sequential_id);

create table lapi_string_interning (
    internal_id integer primary key not null,
    external_string varchar
);

---------------------------------------------------------------------------------------------------
-- Events: Unassign
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_unassign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id binary varying not null,
    workflow_id varchar,

    -- * submitter info (only visible on submitting participant)
    command_id varchar,

    submitter integer not null,
    node_id integer not null,                 -- event metadata

    -- * shared event information
    contract_id binary varying not null,
    template_id integer not null,
    package_id integer not null,
    flat_event_witnesses binary large object not null, -- stakeholders

    -- * common reassignment
    source_synchronizer_id integer not null,
    target_synchronizer_id integer not null,
    reassignment_id binary large object not null,
    reassignment_counter bigint not null,

    -- * unassigned specific
    assignment_exclusivity bigint,

    trace_context binary large object not null,
    record_time bigint not null,
    deactivated_event_sequential_id bigint
);

-- sequential_id index for paging
create index lapi_events_unassign_deactivated_idx on lapi_events_unassign (deactivated_event_sequential_id, event_sequential_id);

-- sequential_id index for paging
create index lapi_events_unassign_event_sequential_id_idx on lapi_events_unassign (event_sequential_id);

-- multi-column index supporting per contract per synchronizer lookup before/after sequential id query
create index lapi_events_unassign_contract_id_composite_idx on lapi_events_unassign (contract_id, source_synchronizer_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
create index lapi_events_unassign_event_offset_idx on lapi_events_unassign (event_offset, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Assign
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
create table lapi_events_assign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint not null,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset bigint not null,

    -- * transaction metadata
    update_id binary varying not null,
    workflow_id varchar,

    -- * submitter info (only visible on submitting participant)
    command_id varchar,

    submitter integer not null,
    node_id integer not null,                 -- event metadata

    -- * shared event information
    contract_id binary varying not null,
    template_id integer not null,
    package_id integer not null,
    flat_event_witnesses binary large object not null, -- stakeholders

    -- * common reassignment
    source_synchronizer_id integer not null,
    target_synchronizer_id integer not null,
    reassignment_id binary large object not null,
    reassignment_counter bigint not null,

    -- * assigned specific
    create_argument binary large object not null,
    create_signatories binary large object not null,
    create_observers binary large object not null,
    create_key_value binary large object,
    create_key_hash varchar,
    create_key_maintainers binary large object,
    create_argument_compression smallint,
    create_key_value_compression smallint,
    ledger_effective_time bigint not null,
    authentication_data binary large object not null,

    trace_context binary large object not null,
    record_time bigint not null,
    internal_contract_id bigint not null
);

-- sequential_id index for paging
create index lapi_events_assign_event_sequential_id_idx on lapi_events_assign (event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
create index lapi_events_assign_event_offset_idx on lapi_events_assign (event_offset, event_sequential_id);

-- index for queries resolving contract ID to sequential IDs.
create index lapi_events_assign_event_contract_id_idx on lapi_events_assign (contract_id, event_sequential_id);

-- index for queries resolving (contract ID, synchronizer id, sequential ID) to sequential IDs.
create index lapi_events_assign_event_contract_id_synchronizer_id_seq_id_idx on lapi_events_assign (contract_id, target_synchronizer_id, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Topology (participant authorization mappings)
---------------------------------------------------------------------------------------------------
create table lapi_events_party_to_participant (
    event_sequential_id bigint not null,
    event_offset bigint not null,
    update_id binary varying not null,
    party_id integer not null,
    participant_id integer not null,
    participant_permission integer not null,
    participant_authorization_event integer not null,
    synchronizer_id integer not null,
    record_time bigint not null,
    trace_context binary large object not null
);

-- offset index: used to translate to sequential_id
create index lapi_events_party_to_participant_event_offset_idx on lapi_events_party_to_participant (event_offset);

-- sequential_id index for paging
create index lapi_events_party_to_participant_event_sequential_id_idx on lapi_events_party_to_participant (event_sequential_id);

-- party_id with event_sequential_id for id queries
create index lapi_events_party_to_participant_event_party_sequential_id_idx on lapi_events_party_to_participant (party_id, event_sequential_id);

-- party_id with event_sequential_id for id queries
create index lapi_events_party_to_participant_event_did_recordt_idx on lapi_events_party_to_participant (synchronizer_id, record_time);
-----------------------------
-- Filter tables for events
-----------------------------

-- create stakeholders
-- deprecated, to be removed. See #28008
create table lapi_pe_create_id_filter_stakeholder (
    event_sequential_id bigint not null,
    template_id integer not null,
    party_id integer not null,
    first_per_sequential_id boolean
);
create index lapi_pe_create_id_filter_stakeholder_pts_idx on lapi_pe_create_id_filter_stakeholder(party_id, template_id, event_sequential_id);
create index lapi_pe_create_id_filter_stakeholder_ps_idx on lapi_pe_create_id_filter_stakeholder(party_id, event_sequential_id);
create index lapi_pe_create_id_filter_stakeholder_ts_idx on lapi_pe_create_id_filter_stakeholder(template_id, event_sequential_id);
create index lapi_pe_create_id_filter_stakeholder_s_idx on lapi_pe_create_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_create_id_filter_non_stakeholder_informee (
   event_sequential_id bigint not null,
   template_id integer not null,
   party_id integer not null,
   first_per_sequential_id boolean
);
create index lapi_pe_create_id_filter_non_stakeholder_informee_pts_idx on lapi_pe_create_id_filter_non_stakeholder_informee(party_id, template_id, event_sequential_id);
create index lapi_pe_create_id_filter_non_stakeholder_informee_ps_idx on lapi_pe_create_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
create index lapi_pe_create_id_filter_non_stakeholder_informee_ts_idx on lapi_pe_create_id_filter_non_stakeholder_informee(template_id, event_sequential_id);
create index lapi_pe_create_id_filter_non_stakeholder_informee_s_idx on lapi_pe_create_id_filter_non_stakeholder_informee(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_consuming_id_filter_stakeholder (
   event_sequential_id bigint not null,
   template_id integer not null,
   party_id integer not null,
   first_per_sequential_id boolean
);
create index lapi_pe_consuming_id_filter_stakeholder_pts_idx on lapi_pe_consuming_id_filter_stakeholder(party_id, template_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_stakeholder_ps_idx  on lapi_pe_consuming_id_filter_stakeholder(party_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_stakeholder_ts_idx  on lapi_pe_consuming_id_filter_stakeholder(template_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_stakeholder_s_idx   on lapi_pe_consuming_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_reassignment_id_filter_stakeholder (
   event_sequential_id bigint not null,
   template_id integer not null,
   party_id integer not null,
   first_per_sequential_id boolean
);
create index lapi_pe_reassignment_id_filter_stakeholder_pts_idx on lapi_pe_reassignment_id_filter_stakeholder(party_id, template_id, event_sequential_id);
create index lapi_pe_reassignment_id_filter_stakeholder_ps_idx  on lapi_pe_reassignment_id_filter_stakeholder(party_id, event_sequential_id);
create index lapi_pe_reassignment_id_filter_stakeholder_ts_idx  on lapi_pe_reassignment_id_filter_stakeholder(template_id, event_sequential_id);
create index lapi_pe_reassignment_id_filter_stakeholder_s_idx   on lapi_pe_reassignment_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_assign_id_filter_stakeholder (
   event_sequential_id bigint not null,
   template_id integer not null,
   party_id integer not null,
   first_per_sequential_id boolean
);
create index lapi_pe_assign_id_filter_stakeholder_pts_idx on lapi_pe_assign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
create index lapi_pe_assign_id_filter_stakeholder_ps_idx  on lapi_pe_assign_id_filter_stakeholder(party_id, event_sequential_id);
create index lapi_pe_assign_id_filter_stakeholder_ts_idx  on lapi_pe_assign_id_filter_stakeholder(template_id, event_sequential_id);
create index lapi_pe_assign_id_filter_stakeholder_s_idx   on lapi_pe_assign_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_consuming_id_filter_non_stakeholder_informee (
   event_sequential_id bigint not null,
   template_id integer not null,
   party_id integer not null,
   first_per_sequential_id boolean
);
create index lapi_pe_consuming_id_filter_non_stakeholder_informee_pts_idx on lapi_pe_consuming_id_filter_non_stakeholder_informee(party_id, template_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_non_stakeholder_informee_ps_idx  on lapi_pe_consuming_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_non_stakeholder_informee_ts_idx  on lapi_pe_consuming_id_filter_non_stakeholder_informee(template_id, event_sequential_id);
create index lapi_pe_consuming_id_filter_non_stakeholder_informee_s_idx   on lapi_pe_consuming_id_filter_non_stakeholder_informee(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
create table lapi_pe_non_consuming_id_filter_informee (
   event_sequential_id bigint not null,
   template_id integer not null,
   party_id integer not null,
   first_per_sequential_id boolean
);
create index lapi_pe_non_consuming_id_filter_informee_pts_idx on lapi_pe_non_consuming_id_filter_informee(party_id, template_id, event_sequential_id);
create index lapi_pe_non_consuming_id_filter_informee_ps_idx  on lapi_pe_non_consuming_id_filter_informee(party_id, event_sequential_id);
create index lapi_pe_non_consuming_id_filter_informee_ts_idx  on lapi_pe_non_consuming_id_filter_informee(template_id, event_sequential_id);
create index lapi_pe_non_consuming_id_filter_informee_s_idx   on lapi_pe_non_consuming_id_filter_informee(event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Transaction meta information
--
-- This table is used in point-wise lookups.
---------------------------------------------------------------------------------------------------
create table lapi_update_meta(
    update_id binary varying not null,
    event_offset bigint not null,
    publication_time bigint not null,
    record_time bigint not null,
    synchronizer_id integer not null,
    event_sequential_id_first bigint not null,
    event_sequential_id_last bigint not null
);
create index lapi_update_meta_uid_idx on lapi_update_meta(update_id);
create index lapi_update_meta_event_offset_idx on lapi_update_meta(event_offset);
create index lapi_update_meta_publication_time_idx on lapi_update_meta using btree (publication_time, event_offset);
create index lapi_update_meta_synchronizer_record_time_idx on lapi_update_meta using btree (synchronizer_id, record_time);
create index lapi_update_meta_synchronizer_offset_idx on lapi_update_meta using btree (synchronizer_id, event_offset);

-- NOTE: We keep participant user and party record tables independent from indexer-based tables, such that
--       we maintain a property that they can be moved to a separate database without any extra schema changes.
---------------------------------------------------------------------------------------------------
-- Identity provider configs
--
-- This table stores identity provider records used in the ledger api identity provider config
-- service.
---------------------------------------------------------------------------------------------------
create table lapi_identity_provider_config
(
    identity_provider_id varchar primary key not null,
    issuer varchar not null unique,
    jwks_url varchar not null,
    is_deactivated boolean not null,
    audience varchar null
);

---------------------------------------------------------------------------------------------------
-- User entries
--
-- This table stores user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
create table lapi_users (
    internal_id integer generated always as identity primary key,
    user_id varchar not null unique,
    primary_party varchar,
    identity_provider_id varchar references lapi_identity_provider_config (identity_provider_id),
    is_deactivated boolean not null,
    resource_version bigint not null,
    created_at bigint not null
);

---------------------------------------------------------------------------------------------------
-- User rights
--
-- This table stores user rights used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
create table lapi_user_rights (
    user_internal_id integer not null references lapi_users (internal_id) on delete cascade,
    user_right integer not null,
    for_party varchar,
    for_party2 varchar generated always as (case
                                                     when for_party is not null
                                                     then for_party
                                                     else ''
                                                     end),
    granted_at bigint not null,
    unique (user_internal_id, user_right, for_party2)
);

---------------------------------------------------------------------------------------------------
-- User annotations
--
-- This table stores additional per user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
create table lapi_user_annotations (
    internal_id integer not null references lapi_users (internal_id) on delete cascade,
    name varchar not null,
    val varchar,
    updated_at bigint not null,
    unique (internal_id, name)
);

insert into lapi_users(user_id, primary_party, identity_provider_id, is_deactivated, resource_version, created_at)
    values ('participant_admin', null, null, false, 0,  0);
insert into lapi_user_rights(user_internal_id, user_right, for_party, granted_at)
    select internal_id, 1, null, 0
    from lapi_users
    where user_id = 'participant_admin';

---------------------------------------------------------------------------------------------------
-- Party records
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
create table lapi_party_records (
    internal_id integer generated always as identity primary key,
    party varchar not null unique,
    identity_provider_id varchar references lapi_identity_provider_config (identity_provider_id),
    resource_version bigint not null,
    created_at bigint not null
);

---------------------------------------------------------------------------------------------------
-- Party record annotations
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
create table lapi_party_record_annotations (
    internal_id integer not null references lapi_party_records (internal_id) on delete cascade,
    name varchar not null,
    val varchar,
    updated_at bigint not null,
    unique (internal_id, name)
);
