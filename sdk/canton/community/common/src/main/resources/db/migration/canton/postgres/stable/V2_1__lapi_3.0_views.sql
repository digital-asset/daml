-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- -------------------
--  HELPER FUNCTIONS
-- -------------------

-- convert the integer google.rpc.Code proto enum to a textual representation
create or replace function debug.lapi_rejection_status_code(integer) returns varchar as
$$
select
  case
    when $1 = 0 then 'Ok'
    when $1 = 1 then 'Cancelled'
    when $1 = 2 then 'Unknown'
    when $1 = 3 then 'Invalid Argument'
    when $1 = 4 then 'Deadline Exceeded'
    when $1 = 5 then 'Not Found'
    when $1 = 6 then 'Already Exists'
    when $1 = 7 then 'Permission_Denied'
    when $1 = 8 then 'Resource Exhausted'
    when $1 = 9 then 'Failed Precondition'
    when $1 = 10 then 'Aborted'
    when $1 = 11 then 'Out of Range'
    when $1 = 12 then 'Unimplemented'
    when $1 = 13 then 'Internal'
    when $1 = 14 then 'Unavailable'
    when $1 = 15 then 'Data Loss'
    when $1 = 16 then 'Unauthenticated'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

-- convert the integer representation of the compression algorithm to textual
create or replace function debug.lapi_compression(integer) returns varchar as
$$
select
  case
    when $1 = 1 then 'GZIP'
    when $1 is null then 'None'
    else $1::text
  end;
$$
  language sql
  immutable
  called on null input;

-- convert the integer id of a user right to its textual representation
create or replace function debug.lapi_user_right(integer) returns varchar as
$$
select
  case
    when $1 = 1 then 'ParticipantAdmin'
    when $1 = 2 then 'CanActAs'
    when $1 = 3 then 'CanReadAs'
    when $1 = 4 then 'IdentityProviderAdmin'
    when $1 = 5 then 'CanReadAsAnyParty'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

-- convert the byte event_type representation to textual
create or replace function debug.lapi_event_type(smallint) returns varchar as
$$
select
  case
    when $1 = 1 then 'Activate-Create'
    when $1 = 2 then 'Activate-Assign'
    when $1 = 3 then 'Deactivate-Consuming-Exercise'
    when $1 = 4 then 'Deactivate-Unassign'
    when $1 = 5 then 'Witnessed-Non-Consuming-Exercise'
    when $1 = 6 then 'Witnessed-Create'
    when $1 = 7 then 'Witnessed-Consuming-Exercise'
    when $1 = 8 then 'Topology-PartyToParticipant'
    when $1 is null then 'None'
    else $1::text
  end;
$$
  language sql
  immutable
  called on null input;

-- resolve a ledger api interned string
create or replace function debug.resolve_lapi_interned_string(integer) returns varchar as
$$
select substring(external_string, 3) from lapi_string_interning where internal_id = $1;
$$
  language sql
  stable
  returns null on null input;

-- resolve multiple ledger api interned strings
create or replace function debug.resolve_lapi_interned_strings(input bytea) returns varchar[] as
$$
select array_agg(debug.resolve_lapi_interned_string(
        get_byte(input, i)::int << 24 |
           get_byte(input, i + 1)::int << 16 |
           get_byte(input, i + 2)::int << 8 |
           get_byte(input, i + 3)::int
                 ))
from generate_series(1, length(input) - 1, 4) as s(i);
$$
  language sql
  stable
  returns null on null input;

-- resolve an interned ledger api user id
create or replace function debug.resolve_lapi_user(integer) returns varchar as
$$
select user_id from lapi_users where internal_id = $1;
$$
  language sql
  stable
  returns null on null input;


-- -------------------
--  Views
-- -------------------


create or replace view debug.lapi_parameters as
  select
    ledger_end,
    participant_id,
    participant_pruned_up_to_inclusive,
    ledger_end_sequential_id,
    ledger_end_string_interning_id,
    debug.canton_timestamp(ledger_end_publication_time) as ledger_end_publication_time
  from lapi_parameters;

create or replace view debug.lapi_command_completions as
  select
    completion_offset,
    debug.canton_timestamp(record_time) as record_time,
    debug.canton_timestamp(publication_time) as publication_time,
    user_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    command_id,
    lower(encode(update_id, 'hex')) as update_id,
    submission_id,
    deduplication_offset,
    deduplication_duration_seconds,
    deduplication_duration_nanos,
    debug.lapi_rejection_status_code(rejection_status_code) as rejection_status_code,
    rejection_status_message,
    lower(encode(rejection_status_details, 'hex')) as rejection_status_details,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    message_uuid,
    is_transaction,
    lower(encode(trace_context, 'hex')) as trace_context
  from lapi_command_completions;

create or replace view debug.lapi_events_activate_contract as
  select
    -- update related columns
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    lower(encode(trace_context, 'hex')) as trace_context,
    lower(encode(external_transaction_hash, 'hex')) as external_transaction_hash,

    -- event related columns
    debug.lapi_event_type(event_type) as event_type,
    event_sequential_id,
    node_id,
    debug.resolve_lapi_interned_strings(additional_witnesses) as additional_witnesses,
    debug.resolve_lapi_interned_string(source_synchronizer_id) as source_synchronizer_id,
    reassignment_counter,
    lower(encode(reassignment_id, 'hex')) as reassignment_id,
    debug.resolve_lapi_interned_string(representative_package_id) as representative_package_id,

    -- contract related columns
    internal_contract_id,
    create_key_hash
  from lapi_events_activate_contract;

create or replace view debug.lapi_filter_activate_stakeholder as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id,
    first_per_sequential_id
  from lapi_filter_activate_stakeholder;

create or replace view debug.lapi_filter_activate_witness as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id,
    first_per_sequential_id
  from lapi_filter_activate_witness;

create or replace view debug.lapi_filter_achs_stakeholder as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id,
    first_per_sequential_id
  from lapi_filter_achs_stakeholder;

create or replace view debug.lapi_achs_state as
  select
    valid_at,
    last_removed,
    last_populated
  from lapi_achs_state;

create or replace view debug.lapi_events_deactivate_contract as
  select
    -- update related columns
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    lower(encode(trace_context, 'hex')) as trace_context,
    lower(encode(external_transaction_hash, 'hex')) as external_transaction_hash,

    -- event related columns
    debug.lapi_event_type(event_type) as event_type,
    event_sequential_id,
    node_id,
    deactivated_event_sequential_id,
    debug.resolve_lapi_interned_strings(additional_witnesses) as additional_witnesses,
    debug.resolve_lapi_interned_string(exercise_choice) as exercise_choice,
    debug.resolve_lapi_interned_string(exercise_choice_interface) as exercise_choice_interface,
    lower(encode(exercise_argument, 'hex')) as exercise_argument,
    lower(encode(exercise_result, 'hex')) as exercise_result,
    debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
    exercise_last_descendant_node_id,
    debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
    debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
    lower(encode(reassignment_id, 'hex')) as reassignment_id,
    assignment_exclusivity,
    debug.resolve_lapi_interned_string(target_synchronizer_id) as target_synchronizer_id,
    reassignment_counter,

    -- contract related columns
    lower(encode(contract_id, 'hex')) as contract_id,
    internal_contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_id) as package_id,
    debug.resolve_lapi_interned_strings(stakeholders) as stakeholders,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time
  from lapi_events_deactivate_contract;

create or replace view debug.lapi_filter_deactivate_stakeholder as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id,
    first_per_sequential_id
  from lapi_filter_deactivate_stakeholder;

create or replace view debug.lapi_filter_deactivate_witness as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id,
    first_per_sequential_id
  from lapi_filter_deactivate_witness;

create or replace view debug.lapi_events_various_witnessed as
  select
    -- update related columns
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    lower(encode(trace_context, 'hex')) as trace_context,
    lower(encode(external_transaction_hash, 'hex')) as external_transaction_hash,

    -- event related columns
    debug.lapi_event_type(event_type) as event_type,
    event_sequential_id,
    node_id,
    debug.resolve_lapi_interned_strings(additional_witnesses) as additional_witnesses,
    consuming,
    debug.resolve_lapi_interned_string(exercise_choice) as exercise_choice,
    debug.resolve_lapi_interned_string(exercise_choice_interface) as exercise_choice_interface,
    lower(encode(exercise_argument, 'hex')) as exercise_argument,
    lower(encode(exercise_result, 'hex')) as exercise_result,
    debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
    exercise_last_descendant_node_id,
    debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
    debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
    debug.resolve_lapi_interned_string(representative_package_id) as representative_package_id,

    -- contract related columns
    lower(encode(contract_id, 'hex')) as contract_id,
    internal_contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_id) as package_id,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time
  from lapi_events_various_witnessed;

create or replace view debug.lapi_filter_various_witness as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id,
    first_per_sequential_id
  from lapi_filter_various_witness;

create or replace view debug.lapi_events_party_to_participant as
select
    event_sequential_id,
    event_offset,
    lower(encode(update_id, 'hex')) as update_id,
    debug.resolve_lapi_interned_string(party_id) as party_id,
    participant_id,
    participant_permission,
    participant_authorization_event,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    debug.canton_timestamp(record_time) as record_time,
    lower(encode(trace_context, 'hex')) as trace_context
  from lapi_events_party_to_participant;

create or replace view debug.lapi_identity_provider_config as
  select
    identity_provider_id,
    issuer,
    jwks_url,
    is_deactivated,
    audience
  from lapi_identity_provider_config;

create or replace view debug.lapi_party_records as
  select
    internal_id,
    party,
    resource_version,
    debug.canton_timestamp(created_at) as created_at,
    identity_provider_id
  from lapi_party_records;

create or replace view debug.lapi_party_record_annotations as
  select
    internal_id,
    name,
    val,
    debug.canton_timestamp(updated_at) as updated_at
  from lapi_party_record_annotations;

create or replace view debug.lapi_update_meta as
  select
    lower(encode(update_id, 'hex')) as update_id,
    event_offset,
    debug.canton_timestamp(publication_time) as publication_time,
    debug.canton_timestamp(record_time) as record_time,
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    event_sequential_id_first,
    event_sequential_id_last
  from lapi_update_meta;

create or replace view debug.lapi_users as
  select
    internal_id,
    user_id,
    primary_party,
    debug.canton_timestamp(created_at) as created_at,
    is_deactivated,
    resource_version,
    identity_provider_id
  from lapi_users;

create or replace view debug.lapi_user_rights as
  select
    debug.resolve_lapi_user(user_internal_id) as user_internal_id,
    debug.lapi_user_right(user_right) as user_right,
    for_party,
    debug.canton_timestamp(granted_at) as granted_at
  from lapi_user_rights;

create or replace view debug.lapi_user_annotations as
  select
    debug.resolve_lapi_user(internal_id) as internal_id,
    name,
    val,
    debug.canton_timestamp(updated_at) as updated_at
  from lapi_user_annotations;

create or replace view debug.lapi_party_entries as
  select
    ledger_offset,
    debug.canton_timestamp(recorded_at) as recorded_at,
    submission_id,
    party,
    typ,
    rejection_reason,
    is_local,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_party_entries;

create or replace view debug.lapi_string_interning as
  select
    internal_id,
    external_string
  from lapi_string_interning;

create or replace view debug.lapi_ledger_end_synchronizer_index as
  select
    debug.resolve_lapi_interned_string(synchronizer_id) as synchronizer_id,
    debug.canton_timestamp(sequencer_timestamp) as sequencer_timestamp,
    debug.canton_timestamp(repair_timestamp) as repair_timestamp,
    repair_counter,
    debug.canton_timestamp(record_time) as record_time
  from lapi_ledger_end_synchronizer_index;

create or replace view debug.lapi_post_processing_end as
  select
    post_processing_end
  from lapi_post_processing_end;
