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
  participant_pruned_up_to_inclusive BIGINT,
  participant_all_divulged_contracts_pruned_up_to_inclusive BIGINT
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
    user_id VARCHAR NOT NULL,
    submitters INTEGER ARRAY NOT NULL,
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
    trace_context BINARY LARGE OBJECT
);

CREATE INDEX lapi_command_completions_user_id_offset_idx ON lapi_command_completions USING btree (user_id, completion_offset);
CREATE INDEX lapi_command_completions_offset_idx ON lapi_command_completions USING btree (completion_offset);
CREATE INDEX lapi_command_completions_publication_time_idx ON lapi_command_completions USING btree (publication_time, completion_offset);
CREATE INDEX lapi_command_completions_synchronizer_record_time_idx ON lapi_command_completions USING btree (synchronizer_id, record_time);
CREATE INDEX lapi_command_completions_synchronizer_offset_idx ON lapi_command_completions USING btree (synchronizer_id, completion_offset);

---------------------------------------------------------------------------------------------------
-- Events: create
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_create (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint NOT NULL,    -- transaction metadata
    node_id integer NOT NULL,                 -- event metadata

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    user_id VARCHAR,
    submitters INTEGER ARRAY,

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_name INTEGER NOT NULL,
    flat_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * contract data
    create_argument BINARY LARGE OBJECT NOT NULL,
    create_signatories INTEGER ARRAY NOT NULL,
    create_observers INTEGER ARRAY NOT NULL,
    create_key_value BINARY LARGE OBJECT,
    create_key_hash VARCHAR,
    create_key_maintainers INTEGER ARRAY,

    -- * compression flags
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT,

    -- * contract driver metadata
    driver_metadata BINARY LARGE OBJECT NOT NULL,

    synchronizer_id INTEGER NOT NULL,
    trace_context BINARY LARGE OBJECT,
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
CREATE TABLE lapi_events_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint NOT NULL,    -- transaction metadata
    node_id integer NOT NULL,                 -- event metadata

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    user_id VARCHAR,
    submitters INTEGER ARRAY,

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_name INTEGER NOT NULL,
    flat_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders
    tree_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * information about the corresponding create event
    create_key_value BINARY LARGE OBJECT,        -- used for the mutable state cache

    -- * choice data
    exercise_choice VARCHAR NOT NULL,
    exercise_argument BINARY LARGE OBJECT NOT NULL,
    exercise_result BINARY LARGE OBJECT,
    exercise_actors INTEGER ARRAY NOT NULL,
    exercise_last_descendant_node_id INTEGER NOT NULL,

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT,

    synchronizer_id INTEGER NOT NULL,
    trace_context BINARY LARGE OBJECT,
    record_time BIGINT NOT NULL,
    external_transaction_hash  BINARY LARGE OBJECT
);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_consuming_exercise_event_offset_idx ON lapi_events_consuming_exercise (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_consuming_exercise_event_sequential_id_idx ON lapi_events_consuming_exercise (event_sequential_id);

-- lookup by contract id
CREATE INDEX lapi_events_consuming_exercise_contract_id_idx ON lapi_events_consuming_exercise (contract_id);

---------------------------------------------------------------------------------------------------
-- Events: non-consuming exercise
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_non_consuming_exercise (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset
    ledger_effective_time bigint NOT NULL,    -- transaction metadata
    node_id integer NOT NULL,                 -- event metadata

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,
    user_id VARCHAR,
    submitters INTEGER ARRAY,

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_name INTEGER NOT NULL,
    tree_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- informees

    -- * information about the corresponding create event
    create_key_value BINARY LARGE OBJECT,        -- used for the mutable state cache

    -- * choice data
    exercise_choice VARCHAR NOT NULL,
    exercise_argument BINARY LARGE OBJECT NOT NULL,
    exercise_result BINARY LARGE OBJECT,
    exercise_actors INTEGER ARRAY NOT NULL,
    exercise_last_descendant_node_id INTEGER NOT NULL,

    -- * compression flags
    create_key_value_compression SMALLINT,
    exercise_argument_compression SMALLINT,
    exercise_result_compression SMALLINT,

    synchronizer_id INTEGER NOT NULL,
    trace_context BINARY LARGE OBJECT,
    record_time BIGINT NOT NULL,
    external_transaction_hash  BINARY LARGE OBJECT
);

-- offset index: used to translate to sequential_id
CREATE INDEX lapi_events_non_consuming_exercise_event_offset_idx ON lapi_events_non_consuming_exercise (event_offset);

-- sequential_id index for paging
CREATE INDEX lapi_events_non_consuming_exercise_event_sequential_id_idx ON lapi_events_non_consuming_exercise (event_sequential_id);

CREATE TABLE lapi_string_interning (
    internal_id integer PRIMARY KEY NOT NULL,
    external_string VARCHAR
);

---------------------------------------------------------------------------------------------------
-- Events: Unassign
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_unassign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,

    submitter INTEGER NOT NULL,
    node_id integer NOT NULL,                 -- event metadata

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_name INTEGER NOT NULL,
    flat_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders

    -- * common reassignment
    source_synchronizer_id INTEGER NOT NULL,
    target_synchronizer_id INTEGER NOT NULL,
    reassignment_id VARCHAR NOT NULL,
    reassignment_counter BIGINT NOT NULL,

    -- * unassigned specific
    assignment_exclusivity BIGINT,

    trace_context BINARY LARGE OBJECT,
    record_time BIGINT NOT NULL
);

-- sequential_id index for paging
CREATE INDEX lapi_events_unassign_event_sequential_id_idx ON lapi_events_unassign (event_sequential_id);

-- multi-column index supporting per contract per synchronizer lookup before/after sequential id query
CREATE INDEX lapi_events_unassign_contract_id_composite_idx ON lapi_events_unassign (contract_id, source_synchronizer_id, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX lapi_events_unassign_event_offset_idx ON lapi_events_unassign (event_offset, event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Events: Assign
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_events_assign (
    -- * fixed-size columns first to avoid padding
    event_sequential_id bigint NOT NULL,      -- event identification: same ordering as event_offset

    -- * event identification
    event_offset BIGINT NOT NULL,

    -- * transaction metadata
    update_id VARCHAR NOT NULL,
    workflow_id VARCHAR,

    -- * submitter info (only visible on submitting participant)
    command_id VARCHAR,

    submitter INTEGER NOT NULL,
    node_id integer NOT NULL,                 -- event metadata

    -- * shared event information
    contract_id BINARY VARYING NOT NULL,
    template_id INTEGER NOT NULL,
    package_name INTEGER NOT NULL,
    flat_event_witnesses INTEGER ARRAY NOT NULL DEFAULT ARRAY[], -- stakeholders

    -- * common reassignment
    source_synchronizer_id INTEGER NOT NULL,
    target_synchronizer_id INTEGER NOT NULL,
    reassignment_id VARCHAR NOT NULL,
    reassignment_counter BIGINT NOT NULL,

    -- * assigned specific
    create_argument BINARY LARGE OBJECT NOT NULL,
    create_signatories INTEGER ARRAY NOT NULL,
    create_observers INTEGER ARRAY NOT NULL,
    create_key_value BINARY LARGE OBJECT,
    create_key_hash VARCHAR,
    create_key_maintainers INTEGER ARRAY,
    create_argument_compression SMALLINT,
    create_key_value_compression SMALLINT,
    ledger_effective_time BIGINT NOT NULL,
    driver_metadata BINARY LARGE OBJECT NOT NULL,

    trace_context BINARY LARGE OBJECT,
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
    participant_id VARCHAR NOT NULL,
    participant_permission INTEGER NOT NULL,
    participant_authorization_event INTEGER NOT NULL,
    synchronizer_id INTEGER NOT NULL,
    record_time BIGINT NOT NULL,
    trace_context BINARY LARGE OBJECT
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
CREATE TABLE lapi_pe_create_id_filter_stakeholder (
    event_sequential_id BIGINT NOT NULL,
    template_id INTEGER NOT NULL,
    party_id INTEGER NOT NULL
);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_pts_idx ON lapi_pe_create_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_ps_idx ON lapi_pe_create_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_ts_idx ON lapi_pe_create_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_stakeholder_s_idx ON lapi_pe_create_id_filter_stakeholder(event_sequential_id);

CREATE TABLE lapi_pe_create_id_filter_non_stakeholder_informee (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_pts_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_ps_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_ts_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(template_id, event_sequential_id);
CREATE INDEX lapi_pe_create_id_filter_non_stakeholder_informee_s_idx ON lapi_pe_create_id_filter_non_stakeholder_informee(event_sequential_id);

CREATE TABLE lapi_pe_consuming_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_pts_idx ON lapi_pe_consuming_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_ps_idx  ON lapi_pe_consuming_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_ts_idx  ON lapi_pe_consuming_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_stakeholder_s_idx   ON lapi_pe_consuming_id_filter_stakeholder(event_sequential_id);

CREATE TABLE lapi_pe_reassignment_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_pts_idx ON lapi_pe_reassignment_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_ps_idx  ON lapi_pe_reassignment_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_ts_idx  ON lapi_pe_reassignment_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_reassignment_id_filter_stakeholder_s_idx   ON lapi_pe_reassignment_id_filter_stakeholder(event_sequential_id);

CREATE TABLE lapi_pe_assign_id_filter_stakeholder (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_pts_idx ON lapi_pe_assign_id_filter_stakeholder(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_ps_idx  ON lapi_pe_assign_id_filter_stakeholder(party_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_ts_idx  ON lapi_pe_assign_id_filter_stakeholder(template_id, event_sequential_id);
CREATE INDEX lapi_pe_assign_id_filter_stakeholder_s_idx   ON lapi_pe_assign_id_filter_stakeholder(event_sequential_id);

CREATE TABLE lapi_pe_consuming_id_filter_non_stakeholder_informee (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_pts_idx ON lapi_pe_consuming_id_filter_non_stakeholder_informee(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_ps_idx  ON lapi_pe_consuming_id_filter_non_stakeholder_informee(party_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_ts_idx  ON lapi_pe_consuming_id_filter_non_stakeholder_informee(template_id, event_sequential_id);
CREATE INDEX lapi_pe_consuming_id_filter_non_stakeholder_informee_s_idx   ON lapi_pe_consuming_id_filter_non_stakeholder_informee(event_sequential_id);

CREATE TABLE lapi_pe_non_consuming_id_filter_informee (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_pts_idx ON lapi_pe_non_consuming_id_filter_informee(party_id, template_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_ps_idx  ON lapi_pe_non_consuming_id_filter_informee(party_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_ts_idx  ON lapi_pe_non_consuming_id_filter_informee(template_id, event_sequential_id);
CREATE INDEX lapi_pe_non_consuming_id_filter_informee_s_idx   ON lapi_pe_non_consuming_id_filter_informee(event_sequential_id);

---------------------------------------------------------------------------------------------------
-- Transaction meta information
--
-- This table is used in point-wise lookups.
---------------------------------------------------------------------------------------------------
CREATE TABLE lapi_transaction_meta(
    update_id VARCHAR NOT NULL,
    event_offset BIGINT NOT NULL,
    publication_time BIGINT NOT NULL,
    record_time BIGINT NOT NULL,
    synchronizer_id INTEGER NOT NULL,
    event_sequential_id_first BIGINT NOT NULL,
    event_sequential_id_last BIGINT NOT NULL
);
CREATE INDEX lapi_transaction_meta_uid_idx ON lapi_transaction_meta(update_id);
CREATE INDEX lapi_transaction_meta_event_offset_idx ON lapi_transaction_meta(event_offset);
CREATE INDEX lapi_transaction_meta_publication_time_idx ON lapi_transaction_meta USING btree (publication_time, event_offset);
CREATE INDEX lapi_transaction_meta_synchronizer_record_time_idx ON lapi_transaction_meta USING btree (synchronizer_id, record_time);
CREATE INDEX lapi_transaction_meta_synchronizer_offset_idx ON lapi_transaction_meta USING btree (synchronizer_id, event_offset);

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
