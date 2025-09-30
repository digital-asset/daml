-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE ALIAS array_intersection FOR "com.digitalasset.canton.store.db.h2.H2FunctionAliases.arrayIntersection";

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
  participant_id VARCHAR NOT NULL,
  ledger_end BIGINT,
  ledger_end_sequential_id BIGINT,
  ledger_end_string_interning_id INTEGER,
  ledger_end_publication_time BIGINT,
  participant_pruned_up_to_inclusive BIGINT
);

CREATE TABLE lapi_post_processing_end (
    -- null signifies the participant begin
    post_processing_end BIGINT
);


CREATE TABLE lapi_ledger_end_synchronizer_index (
  synchronizer_id INTEGER PRIMARY KEY NOT NULL,
  sequencer_timestamp BIGINT,
  repair_timestamp BIGINT,
  repair_counter BIGINT,
  record_time BIGINT NOT NULL
);

---------------------------------------------------------------------------------------------------
-- Party entries
--
-- A table for tracking party allocation submissions
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_entries (
    ledger_offset BIGINT NOT NULL,
    recorded_at BIGINT NOT NULL,
    submission_id VARCHAR,
    party VARCHAR,
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

CREATE INDEX lapi_party_entries_idx ON lapi_party_entries (submission_id);
CREATE INDEX lapi_party_entries_party_and_ledger_offset_idx ON lapi_party_entries(party, ledger_offset);
CREATE INDEX lapi_party_entries_party_id_and_ledger_offset_idx ON lapi_party_entries(party_id, ledger_offset);

---------------------------------------------------------------------------------------------------
-- Completions
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_command_completions (
    completion_offset BIGINT NOT NULL,
    record_time BIGINT NOT NULL,
    publication_time BIGINT NOT NULL,
    user_id INTEGER NOT NULL,
    submitters BINARY LARGE OBJECT NOT NULL,
    command_id VARCHAR NOT NULL,
    -- The update ID is `NULL` for rejected transactions/reassignments.
    update_id VARCHAR,
    -- The submission ID will be provided by the participant or driver if the application didn't provide one.
    -- Nullable to support historical data.
    submission_id VARCHAR,
    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the application as one of:
    -- 1. an initial offset
    -- 2. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    -- 3. an initial timestamp
    deduplication_offset BIGINT,
    deduplication_duration_seconds BIGINT,
    deduplication_duration_nanos INT,
    -- The three columns below are `NULL` if the completion is for an accepted transaction.
    -- The `rejection_status_details` column contains a Protocol-Buffers-serialized message of type
    -- `daml.platform.index.StatusDetails`, containing the code, message, and further details
    -- (decided by the ledger driver), and may be `NULL` even if the other two columns are set.
    rejection_status_code INTEGER,
    rejection_status_message VARCHAR,
    rejection_status_details BINARY LARGE OBJECT,
    synchronizer_id INTEGER NOT NULL,
    message_uuid VARCHAR,
    is_transaction BOOLEAN NOT NULL,
    trace_context BINARY LARGE OBJECT NOT NULL
);

CREATE INDEX lapi_command_completions_user_id_offset_idx ON lapi_command_completions USING btree (user_id, completion_offset);
CREATE INDEX lapi_command_completions_offset_idx ON lapi_command_completions USING btree (completion_offset);
CREATE INDEX lapi_command_completions_publication_time_idx ON lapi_command_completions USING btree (publication_time, completion_offset);
CREATE INDEX lapi_command_completions_synchronizer_record_time_idx ON lapi_command_completions USING btree (synchronizer_id, record_time);
CREATE INDEX lapi_command_completions_synchronizer_offset_idx ON lapi_command_completions USING btree (synchronizer_id, completion_offset);

---------------------------------------------------------------------------------------------------
-- Events: Activate Contract
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_activate_contract (
   -- update related columns
   event_offset BIGINT NOT NULL,
   update_id BINARY LARGE OBJECT NOT NULL,
   workflow_id VARCHAR,
   command_id VARCHAR,
   submitters BINARY LARGE OBJECT,
   record_time BIGINT NOT NULL,
   synchronizer_id INTEGER NOT NULL,
   trace_context BINARY LARGE OBJECT NOT NULL,
   external_transaction_hash BINARY LARGE OBJECT,

   -- event related columns
   event_type SMALLINT NOT NULL, -- all event types
   event_sequential_id BIGINT NOT NULL, -- all event types
   node_id INTEGER NOT NULL, -- all event types
   additional_witnesses BINARY LARGE OBJECT, -- create events
   source_synchronizer_id INTEGER, -- assign events
   reassignment_counter BIGINT, -- assign events
   reassignment_id BINARY LARGE OBJECT, -- assign events
   representative_package_id INTEGER, -- create events

   -- contract related columns
   internal_contract_id BIGINT NOT NULL, -- all event types
   create_key_hash VARCHAR -- create
);

-- sequential_id index
CREATE INDEX lapi_events_activate_sequential_id_idx ON lapi_events_activate_contract USING btree (event_sequential_id);
-- event_offset index
CREATE INDEX lapi_events_activate_offset_idx ON lapi_events_activate_contract USING btree (event_offset);
-- internal_contract_id index
CREATE INDEX lapi_events_activate_internal_contract_id_idx ON lapi_events_activate_contract USING btree (internal_contract_id, event_sequential_id);
-- contract_key index
CREATE INDEX lapi_events_activate_contract_key_idx ON lapi_events_activate_contract USING btree (create_key_hash, event_sequential_id);

-- filter table for stakeholders
CREATE TABLE lapi_filter_activate_stakeholder (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL,
    first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_filter_activate_stakeholder_ps_idx  ON lapi_filter_activate_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_filter_activate_stakeholder_pts_idx ON lapi_filter_activate_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_filter_activate_stakeholder_ts_idx  ON lapi_filter_activate_stakeholder USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_filter_activate_stakeholder_s_idx   ON lapi_filter_activate_stakeholder USING btree (event_sequential_id, first_per_sequential_id);

-- filter table for additional witnesses
CREATE TABLE lapi_filter_activate_witness (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL,
    first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_filter_activate_witness_ps_idx  ON lapi_filter_activate_witness USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_filter_activate_witness_pts_idx ON lapi_filter_activate_witness USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_filter_activate_witness_ts_idx  ON lapi_filter_activate_witness USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_filter_activate_witness_s_idx   ON lapi_filter_activate_witness USING btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Deactivate Contract
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_deactivate_contract (
   -- update related columns
   event_offset BIGINT NOT NULL,
   update_id BINARY LARGE OBJECT NOT NULL,
   workflow_id VARCHAR,
   command_id VARCHAR,
   submitters BINARY LARGE OBJECT,
   record_time BIGINT NOT NULL,
   synchronizer_id INTEGER NOT NULL,
   trace_context BINARY LARGE OBJECT NOT NULL,
   external_transaction_hash BINARY LARGE OBJECT,

   -- event related columns
   event_type SMALLINT NOT NULL, -- all event types
   event_sequential_id BIGINT NOT NULL, -- all event types
   node_id INTEGER NOT NULL, -- all event types
   deactivated_event_sequential_id BIGINT, -- all event types
   additional_witnesses BINARY LARGE OBJECT, -- consuming events
   exercise_choice INTEGER, -- consuming events
   exercise_choice_interface INTEGER, -- consuming events
   exercise_argument BINARY LARGE OBJECT, -- consuming events
   exercise_result BINARY LARGE OBJECT, -- consuming events
   exercise_actors BINARY LARGE OBJECT, -- consuming events
   exercise_last_descendant_node_id INTEGER, -- consuming events
   exercise_argument_compression SMALLINT, -- consuming events
   exercise_result_compression SMALLINT, -- consuming events
   reassignment_id BINARY LARGE OBJECT, -- unassign events
   assignment_exclusivity BIGINT, -- unassign events
   target_synchronizer_id INTEGER, -- unassign events
   reassignment_counter BIGINT, -- unassign events

   -- contract related columns
   contract_id BINARY LARGE OBJECT NOT NULL, -- all event types
   internal_contract_id BIGINT NOT NULL, -- all event types
   template_id INTEGER NOT NULL, -- all event types
   package_id INTEGER NOT NULL, -- all event types
   stakeholders BINARY LARGE OBJECT NOT NULL, -- all event types
   ledger_effective_time BIGINT -- consuming events
);

-- sequential_id index
CREATE INDEX lapi_events_deactivate_sequential_id_idx ON lapi_events_deactivate_contract USING btree (event_sequential_id);
-- event_offset index
CREATE INDEX lapi_events_deactivate_offset_idx ON lapi_events_deactivate_contract USING btree (event_offset);
-- internal_contract_id index
CREATE INDEX lapi_events_deactivate_internal_contract_id_idx ON lapi_events_deactivate_contract USING btree (internal_contract_id, event_sequential_id);
-- deactivation reference index
CREATE INDEX lapi_events_deactivated_event_sequential_id_idx ON lapi_events_deactivate_contract USING btree (deactivated_event_sequential_id);

-- filter table for stakeholders
CREATE TABLE lapi_filter_deactivate_stakeholder (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL,
    first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_filter_deactivate_stakeholder_ps_idx  ON lapi_filter_deactivate_stakeholder USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_filter_deactivate_stakeholder_pts_idx ON lapi_filter_deactivate_stakeholder USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_filter_deactivate_stakeholder_ts_idx  ON lapi_filter_deactivate_stakeholder USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_filter_deactivate_stakeholder_s_idx   ON lapi_filter_deactivate_stakeholder USING btree (event_sequential_id, first_per_sequential_id);

-- filter table for additional witnesses
CREATE TABLE lapi_filter_deactivate_witness (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL,
    first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_filter_deactivate_witness_ps_idx  ON lapi_filter_deactivate_witness USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_filter_deactivate_witness_pts_idx ON lapi_filter_deactivate_witness USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_filter_deactivate_witness_ts_idx  ON lapi_filter_deactivate_witness USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_filter_deactivate_witness_s_idx   ON lapi_filter_deactivate_witness USING btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Various Witnessed
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_various_witnessed (
   -- tx related columns
   event_offset BIGINT NOT NULL,
   update_id BINARY LARGE OBJECT NOT NULL,
   workflow_id VARCHAR,
   command_id VARCHAR,
   submitters BINARY LARGE OBJECT,
   record_time BIGINT NOT NULL,
   synchronizer_id INTEGER NOT NULL,
   trace_context BINARY LARGE OBJECT NOT NULL,
   external_transaction_hash BINARY LARGE OBJECT,

   -- event related columns
   event_type SMALLINT NOT NULL, -- all event types
   event_sequential_id BIGINT NOT NULL, -- all event types
   node_id INTEGER NOT NULL, -- all event types
   additional_witnesses BINARY LARGE OBJECT, -- all event types
   consuming BOOLEAN, -- exercise
   exercise_choice INTEGER, -- exercise
   exercise_choice_interface INTEGER, -- exercise
   exercise_argument BINARY LARGE OBJECT, -- exercise
   exercise_result BINARY LARGE OBJECT, -- exercise
   exercise_actors BINARY LARGE OBJECT, -- exercise
   exercise_last_descendant_node_id INTEGER, -- exercise
   exercise_argument_compression SMALLINT, -- exercise
   exercise_result_compression SMALLINT, -- exercise
   representative_package_id INTEGER, -- create events

   -- contract related columns
   contract_id BINARY LARGE OBJECT,
   internal_contract_id BIGINT,
   template_id INTEGER,
   package_id INTEGER,
   ledger_effective_time BIGINT
);

-- sequential_id index
CREATE INDEX lapi_events_various_sequential_id_idx ON lapi_events_various_witnessed USING btree (event_sequential_id);
-- event_offset index
CREATE INDEX lapi_events_various_offset_idx ON lapi_events_various_witnessed USING btree (event_offset);
-- internal_contract_id index
CREATE INDEX lapi_events_various_internal_contract_id_idx ON lapi_events_various_witnessed USING btree (internal_contract_id, event_sequential_id);

-- filter table for additional witnesses
CREATE TABLE lapi_filter_various_witness (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL,
    first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_filter_various_witness_ps_idx  ON lapi_filter_various_witness USING btree (party_id, event_sequential_id);
CREATE INDEX lapi_filter_various_witness_pts_idx ON lapi_filter_various_witness USING btree (party_id, template_id, event_sequential_id);
CREATE INDEX lapi_filter_various_witness_ts_idx  ON lapi_filter_various_witness USING btree (template_id, event_sequential_id);
CREATE INDEX lapi_filter_various_witness_s_idx   ON lapi_filter_various_witness USING btree (event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: create
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
CREATE TABLE lapi_events_create (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time BIGINT NOT NULL,    -- transaction metadata
    node_id INTEGER NOT NULL,                 -- event metadata

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    submitters BINARY LARGE OBJECT,

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_id INTEGER NOT NULL,
    representative_package_id INTEGER NOT NULL,
    flat_event_witnesses BINARY LARGE OBJECT NOT NULL, -- stakeholders
    tree_event_witnesses BINARY LARGE OBJECT NOT NULL, -- informees

    -- * contract data
    create_argument BINARY LARGE OBJECT NOT NULL,
    create_signatories BINARY LARGE OBJECT NOT NULL,
    create_observers BINARY LARGE OBJECT NOT NULL,
    create_key_value BINARY LARGE OBJECT,
    create_key_hash VARCHAR,
    create_key_maintainers BINARY LARGE OBJECT,

    -- * compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT,

    -- * contract authentication data
    authentication_data BINARY LARGE OBJECT NOT NULL,

    synchronizer_id INTEGER NOT NULL,
    trace_context BINARY LARGE OBJECT NOT NULL,
    record_time BIGINT NOT NULL,
    external_transaction_hash  BINARY LARGE OBJECT
);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_create_event_offset_idx ON lapi_events_create (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_create_event_sequential_id_idx ON lapi_events_create (event_sequential_id);

-- lookup by contract_id
CREATE INDEX lapi_events_create_contract_id_idx ON lapi_events_create (contract_id);

-- lookup by contract_key
CREATE INDEX lapi_events_create_create_key_hash_idx ON lapi_events_create (create_key_hash, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: consuming exercise
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
CREATE TABLE lapi_events_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time BIGINT NOT NULL,    -- transaction metadata
    node_id INTEGER NOT NULL,                 -- event metadata

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    submitters BINARY LARGE OBJECT,

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_id INTEGER NOT NULL,
    flat_event_witnesses BINARY LARGE OBJECT NOT NULL, -- stakeholders
    tree_event_witnesses BINARY LARGE OBJECT NOT NULL, -- informees

    -- * choice data
    exercise_choice INTEGER NOT NULL,
    exercise_choice_interface INTEGER,
    exercise_argument BINARY LARGE OBJECT NOT NULL,
    exercise_result BINARY LARGE OBJECT,
    exercise_actors BINARY LARGE OBJECT NOT NULL,
    exercise_last_descendant_node_id INTEGER NOT NULL,

    -- * compression flags
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT,

    synchronizer_id INTEGER NOT NULL,
    trace_context BINARY LARGE OBJECT NOT NULL,
    record_time BIGINT NOT NULL,
    external_transaction_hash  BINARY LARGE OBJECT,
    deactivated_event_sequential_id bigint
);

-- deactivations
CREATE INDEX lapi_events_consuming_exercise_deactivated_idx ON lapi_events_consuming_exercise (deactivated_event_sequential_id, event_sequential_id);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_consuming_exercise_event_offset_idx ON lapi_events_consuming_exercise (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_consuming_exercise_event_sequential_id_idx ON lapi_events_consuming_exercise (event_sequential_id);

-- lookup by contract id
CREATE INDEX lapi_events_consuming_exercise_contract_id_idx ON lapi_events_consuming_exercise (contract_id);

---------------------------------------------------------------------------------------------------
-- Events: non-consuming exercise
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
CREATE TABLE lapi_events_non_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time BIGINT NOT NULL,    -- transaction metadata
    node_id INTEGER NOT NULL,                 -- event metadata

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    submitters BINARY LARGE OBJECT,

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_id INTEGER NOT NULL,
    tree_event_witnesses BINARY LARGE OBJECT NOT NULL, -- informees

    -- * choice data
    exercise_choice INTEGER NOT NULL,
    exercise_choice_interface INTEGER,
    exercise_argument BINARY LARGE OBJECT NOT NULL,
    exercise_result BINARY LARGE OBJECT,
    exercise_actors BINARY LARGE OBJECT NOT NULL,
    exercise_last_descendant_node_id INTEGER NOT NULL,

    -- * compression flags
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT,

    synchronizer_id INTEGER NOT NULL,
    trace_context BINARY LARGE OBJECT NOT NULL,
    record_time BIGINT NOT NULL,
    external_transaction_hash  BINARY LARGE OBJECT
);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_non_consuming_exercise_event_offset_idx ON lapi_events_non_consuming_exercise (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_non_consuming_exercise_event_sequential_id_idx ON lapi_events_non_consuming_exercise (event_sequential_id);

CREATE TABLE lapi_string_interning (
    internal_id INTEGER PRIMARY KEY NOT NULL,
    external_string VARCHAR
);

---------------------------------------------------------------------------------------------------
-- Events: Unassign
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
CREATE TABLE lapi_events_unassign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,

    submitter INTEGER NOT NULL,
    node_id INTEGER NOT NULL,                 -- event metadata

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_id INTEGER NOT NULL,
    flat_event_witnesses BINARY LARGE OBJECT NOT NULL, -- stakeholders

    -- * common reassignment
    source_synchronizer_id INTEGER NOT NULL,
    target_synchronizer_id INTEGER NOT NULL,
    reassignment_id VARCHAR NOT NULL,
    reassignment_counter BIGINT NOT NULL,

    -- * unassigned specific
    assignment_exclusivity BIGINT,

    trace_context BINARY LARGE OBJECT NOT NULL,
    record_time BIGINT NOT NULL,
    deactivated_event_sequential_id bigint
);

-- sequential_id index for paging
CREATE INDEX lapi_events_unassign_deactivated_idx ON lapi_events_unassign (deactivated_event_sequential_id, event_sequential_id);

-- sequential_id index for paging
CREATE INDEX lapi_events_unassign_event_sequential_id_idx ON lapi_events_unassign (event_sequential_id);

-- multi-column index supporting per contract per synchronizer lookup before/after sequential id query
CREATE INDEX lapi_events_unassign_contract_id_composite_idx ON lapi_events_unassign (contract_id, source_synchronizer_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX lapi_events_unassign_event_offset_idx ON lapi_events_unassign (event_offset, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Assign
---------------------------------------------------------------------------------------------------
-- deprecated, to be removed. See #28008
CREATE TABLE lapi_events_assign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id BIGINT NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,

    submitter INTEGER NOT NULL,
    node_id INTEGER NOT NULL,                 -- event metadata

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_id INTEGER NOT NULL,
    flat_event_witnesses BINARY LARGE OBJECT NOT NULL, -- stakeholders

    -- * common reassignment
    source_synchronizer_id INTEGER NOT NULL,
    target_synchronizer_id INTEGER NOT NULL,
    reassignment_id VARCHAR NOT NULL,
    reassignment_counter BIGINT NOT NULL,

    -- * assigned specific
    create_argument BINARY LARGE OBJECT NOT NULL,
    create_signatories BINARY LARGE OBJECT NOT NULL,
    create_observers BINARY LARGE OBJECT NOT NULL,
    create_key_value BINARY LARGE OBJECT,
    create_key_hash VARCHAR,
    create_key_maintainers BINARY LARGE OBJECT,
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT,
    ledger_effective_time BIGINT NOT NULL,
    authentication_data BINARY LARGE OBJECT NOT NULL,

    trace_context BINARY LARGE OBJECT NOT NULL,
    record_time BIGINT NOT NULL
);

-- sequential_id index for paging
CREATE INDEX lapi_events_assign_event_sequential_id_idx ON lapi_events_assign (event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX lapi_events_assign_event_offset_idx ON lapi_events_assign (event_offset, event_sequential_id);

-- index for queries resolving contract ID to sequential IDs.
CREATE INDEX lapi_events_assign_event_contract_id_idx ON lapi_events_assign (contract_id, event_sequential_id);

-- index for queries resolving (contract ID, synchronizer id, sequential ID) to sequential IDs.
CREATE INDEX lapi_events_assign_event_contract_id_synchronizer_id_seq_id_idx ON lapi_events_assign (contract_id, target_synchronizer_id, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Topology (participant authorization mappings)
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_party_to_participant (
    event_sequential_id BIGINT NOT NULL,
    event_offset BIGINT NOT NULL,
    update_id VARCHAR NOT NULL,
    party_id INTEGER NOT NULL,
    participant_id INTEGER NOT NULL,
    participant_permission INTEGER NOT NULL,
    participant_authorization_event INTEGER NOT NULL,
    synchronizer_id INTEGER NOT NULL,
    record_time BIGINT NOT NULL,
    trace_context BINARY LARGE OBJECT NOT NULL
);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_party_to_participant_event_offset_idx ON lapi_events_party_to_participant (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_party_to_participant_event_sequential_id_idx ON lapi_events_party_to_participant (event_sequential_id);

-- party_id with event_sequential_id for id queries
CREATE INDEX lapi_events_party_to_participant_event_party_sequential_id_idx ON lapi_events_party_to_participant (party_id, event_sequential_id);

-- party_id with event_sequential_id for id queries
CREATE INDEX lapi_events_party_to_participant_event_did_recordt_idx ON lapi_events_party_to_participant (synchronizer_id, record_time);
-----------------------------
-- Filter tables for events
-----------------------------

-- create stakeholders
-- deprecated, to be removed. See #28008
CREATE TABLE lapi_pe_create_id_filter_stakeholder (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL,
    first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_pts_idx ON lapi_pe_create_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_ps_idx ON lapi_pe_create_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_ts_idx ON lapi_pe_create_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_s_idx ON lapi_pe_create_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
CREATE TABLE lapi_pe_create_id_filter_non_stakeholder_informee (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL,
   first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_pts_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_ps_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_ts_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_s_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
CREATE TABLE lapi_pe_consuming_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL,
   first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_pts_idx ON lapi_pe_consuming_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_ps_idx  ON lapi_pe_consuming_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_ts_idx  ON lapi_pe_consuming_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_s_idx   ON lapi_pe_consuming_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
CREATE TABLE lapi_pe_reassignment_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL,
   first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_pts_idx ON lapi_pe_reassignment_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_ps_idx  ON lapi_pe_reassignment_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_ts_idx  ON lapi_pe_reassignment_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_s_idx   ON lapi_pe_reassignment_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
CREATE TABLE lapi_pe_assign_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL,
   first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_pts_idx ON lapi_pe_assign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_ps_idx  ON lapi_pe_assign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_ts_idx  ON lapi_pe_assign_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_s_idx   ON lapi_pe_assign_id_filter_stakeholder(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
CREATE TABLE lapi_pe_consuming_id_filter_non_stakeholder_informee (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL,
   first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_pts_idx ON lapi_pe_consuming_id_filter_non_stakeholder_informee(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_ps_idx  ON lapi_pe_consuming_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_ts_idx  ON lapi_pe_consuming_id_filter_non_stakeholder_informee(template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_s_idx   ON lapi_pe_consuming_id_filter_non_stakeholder_informee(event_sequential_id, first_per_sequential_id);

-- deprecated, to be removed. See #28008
CREATE TABLE lapi_pe_non_consuming_id_filter_informee (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL,
   first_per_sequential_id BOOLEAN
);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_pts_idx ON lapi_pe_non_consuming_id_filter_informee(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_ps_idx  ON lapi_pe_non_consuming_id_filter_informee(party_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_ts_idx  ON lapi_pe_non_consuming_id_filter_informee(template_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_s_idx   ON lapi_pe_non_consuming_id_filter_informee(event_sequential_id, first_per_sequential_id);

---------------------------------------------------------------------------------------------------
-- Transaction meta information
--
-- This table is used in point-wise lookups.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_update_meta(
    update_id VARCHAR NOT NULL,
    event_offset BIGINT NOT NULL,
    publication_time BIGINT NOT NULL,
    record_time BIGINT NOT NULL,
    synchronizer_id INTEGER NOT NULL,
    event_sequential_id_first BIGINT NOT NULL,
    event_sequential_id_last BIGINT NOT NULL
);
CREATE INDEX lapi_update_meta_uid_idx ON lapi_update_meta(update_id);
CREATE INDEX lapi_update_meta_event_offset_idx ON lapi_update_meta(event_offset);
CREATE INDEX lapi_update_meta_publication_time_idx ON lapi_update_meta USING btree (publication_time, event_offset);
CREATE INDEX lapi_update_meta_synchronizer_record_time_idx ON lapi_update_meta USING btree (synchronizer_id, record_time);
CREATE INDEX lapi_update_meta_synchronizer_offset_idx ON lapi_update_meta USING btree (synchronizer_id, event_offset);

-- NOTE: We keep participant user and party record tables independent from indexer-based tables, such that
--       we maintain a property that they can be moved to a separate database without any extra schema changes.
---------------------------------------------------------------------------------------------------
-- Identity provider configs
--
-- This table stores identity provider records used in the ledger api identity provider config
-- service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_identity_provider_config
(
    identity_provider_id VARCHAR PRIMARY KEY NOT NULL,
    issuer VARCHAR NOT NULL UNIQUE,
    jwks_url VARCHAR NOT NULL,
    is_deactivated BOOLEAN NOT NULL,
    audience VARCHAR NULL
);

---------------------------------------------------------------------------------------------------
-- User entries
--
-- This table stores user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_users (
    internal_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id VARCHAR NOT NULL UNIQUE,
    primary_party VARCHAR,
    identity_provider_id VARCHAR REFERENCES lapi_identity_provider_config (identity_provider_id),
    is_deactivated BOOLEAN NOT NULL,
    resource_version BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

---------------------------------------------------------------------------------------------------
-- User rights
--
-- This table stores user rights used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_user_rights (
    user_internal_id INTEGER NOT NULL REFERENCES lapi_users (internal_id) ON DELETE CASCADE,
    user_right INTEGER NOT NULL,
    for_party VARCHAR,
    for_party2 VARCHAR GENERATED ALWAYS AS (CASE
                                                     WHEN for_party IS NOT NULL
                                                     THEN for_party
                                                     ELSE ''
                                                     END),
    granted_at BIGINT NOT NULL,
    UNIQUE (user_internal_id, user_right, for_party2)
);

---------------------------------------------------------------------------------------------------
-- User annotations
--
-- This table stores additional per user data used in the ledger api user management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_user_annotations (
    internal_id INTEGER NOT NULL REFERENCES lapi_users (internal_id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    val VARCHAR,
    updated_at BIGINT NOT NULL,
    UNIQUE (internal_id, name)
);

INSERT INTO lapi_users(user_id, primary_party, identity_provider_id, is_deactivated, resource_version, created_at)
    VALUES ('participant_admin', NULL, NULL, false, 0,  0);
INSERT INTO lapi_user_rights(user_internal_id, user_right, for_party, granted_at)
    SELECT internal_id, 1, NULL, 0
    FROM lapi_users
    WHERE user_id = 'participant_admin';

---------------------------------------------------------------------------------------------------
-- Party records
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_records (
    internal_id INTEGER GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    party VARCHAR NOT NULL UNIQUE,
    identity_provider_id VARCHAR REFERENCES lapi_identity_provider_config (identity_provider_id),
    resource_version BIGINT NOT NULL,
    created_at BIGINT NOT NULL
);

---------------------------------------------------------------------------------------------------
-- Party record annotations
--
-- This table stores additional per party data used in the ledger api party management service.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_party_record_annotations (
    internal_id INTEGER NOT NULL REFERENCES lapi_party_records (internal_id) ON DELETE CASCADE,
    name VARCHAR NOT NULL,
    val VARCHAR,
    updated_at BIGINT NOT NULL,
    UNIQUE (internal_id, name)
);
