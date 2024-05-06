-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- -------------------
--  HELPER FUNCTIONS
-- -------------------

-- convert the integer google.rpc.Code proto enum to a text representation
create or replace function debug.lapi_rejection_status_code(integer) returns varchar(300) as
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

-- convert the integer representation of the compression algorithm to text
create or replace function debug.lapi_compression(integer) returns varchar(300) as
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

-- convert the integer id of a user right to its text representation
create or replace function debug.lapi_user_right(integer) returns varchar(300) as
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

-- resolve a ledger api interned string
create or replace function debug.resolve_lapi_interned_string(integer) returns varchar(300) as
$$
select substring(external_string, 3) from lapi_string_interning where internal_id = $1;
$$
  language sql
  stable
  returns null on null input;

-- resolve multiple ledger api interned strings
create or replace function debug.resolve_lapi_interned_strings(integer[]) returns varchar(300)[] as
$$
select array_agg(debug.resolve_lapi_interned_string(s)) from unnest($1) as s;
$$
  language sql
  stable
  returns null on null input;

-- resolve an interned ledger api user id
create or replace function debug.resolve_lapi_user(integer) returns varchar(300) as
$$
select user_id from lapi_users where internal_id = $1;
$$
  language sql
  stable
  returns null on null input;


-- -------------------
--  Views
-- -------------------


create or replace view debug.lapi_metering_parameters as
  select
    ledger_metering_end,
    ledger_metering_timestamp
  from lapi_metering_parameters;

create or replace view debug.lapi_participant_metering as
  select
    application_id,
    debug.canton_timestamp(from_timestamp) as from_timestamp,
    debug.canton_timestamp(to_timestamp) as to_timestamp,
    action_count,
    ledger_offset
  from lapi_participant_metering;

create or replace view debug.lapi_transaction_metering as
  select
    application_id,
    action_count,
    debug.canton_timestamp(metering_timestamp) as metering_timestamp,
    ledger_offset
  from lapi_transaction_metering;

create or replace view debug.lapi_package_entries as
  select
    ledger_offset,
    debug.canton_timestamp(recorded_at) as recorded_at,
    submission_id,
    typ,
    rejection_reason
  from lapi_package_entries;

create or replace view debug.lapi_packages as
  select
    package_id,
    upload_id,
    source_description,
    package_size,
    debug.canton_timestamp(known_since) as known_since,
    ledger_offset,
    package
  from lapi_packages;

create or replace view debug.lapi_parameters as
  select
    ledger_end,
    participant_id,
    participant_pruned_up_to_inclusive,
    ledger_end_sequential_id,
    participant_all_divulged_contracts_pruned_up_to_inclusive,
    ledger_end_string_interning_id
  from lapi_parameters;

create or replace view debug.lapi_command_completions as
  select
    completion_offset,
    debug.canton_timestamp(record_time) as record_time,
    application_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    command_id,
    transaction_id,
    submission_id,
    deduplication_offset,
    deduplication_duration_seconds,
    deduplication_duration_nanos,
    debug.canton_timestamp(deduplication_start) as deduplication_start,
    debug.lapi_rejection_status_code(rejection_status_code) as rejection_status_code,
    rejection_status_message,
    rejection_status_details,
    debug.resolve_lapi_interned_string(domain_id) as domain_id,
    trace_context
  from lapi_command_completions;

create or replace view debug.lapi_events_assign as
  select
    event_sequential_id,
    event_offset,
    update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_string(submitter) as submitter,
    contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_name) as package_name,
    debug.resolve_lapi_interned_strings(flat_event_witnesses) as flat_event_witnesses,
    debug.resolve_lapi_interned_string(source_domain_id) as source_domain_id,
    debug.resolve_lapi_interned_string(target_domain_id) as target_domain_id,
    unassign_id,
    reassignment_counter,
    create_argument,
    debug.resolve_lapi_interned_strings(create_signatories) as create_signatories,
    debug.resolve_lapi_interned_strings(create_observers) as create_observers,
    create_key_value,
    create_key_hash,
    debug.lapi_compression(create_argument_compression) as create_argument_compression,
    debug.lapi_compression(create_key_value_compression) as create_key_value_compression,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
    driver_metadata,
    debug.resolve_lapi_interned_strings(create_key_maintainers) as create_key_maintainers,
    trace_context,
    debug.canton_timestamp(record_time) as record_time
  from lapi_events_assign;


create or replace view debug.lapi_events_consuming_exercise as
  select
    event_sequential_id,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
    node_index,
    event_offset,
    transaction_id,
    workflow_id,
    command_id,
    application_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    event_id,
    contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_name) as package_name,
    debug.resolve_lapi_interned_strings(flat_event_witnesses) as flat_event_witnesses,
    debug.resolve_lapi_interned_strings(tree_event_witnesses) as tree_event_witnesses,
    create_key_value,
    exercise_choice,
    exercise_argument,
    exercise_result,
    debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
    exercise_child_event_ids,
    debug.lapi_compression(create_key_value_compression) as create_key_value_compression,
    debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
    debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
    debug.resolve_lapi_interned_string(domain_id) as domain_id,
    trace_context,
    debug.canton_timestamp(record_time) as record_time
  from lapi_events_consuming_exercise;

create or replace view debug.lapi_events_create as
  select
    event_sequential_id,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
    node_index,
    event_offset,
    transaction_id,
    workflow_id,
    command_id,
    application_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    event_id,
    contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_name) as package_name,
    debug.resolve_lapi_interned_strings(flat_event_witnesses) as flat_event_witnesses,
    debug.resolve_lapi_interned_strings(tree_event_witnesses) as tree_event_witnesses,
    create_argument,
    debug.resolve_lapi_interned_strings(create_signatories) as create_signatories,
    debug.resolve_lapi_interned_strings(create_observers) as create_observers,
    create_key_value,
    create_key_hash,
    debug.lapi_compression(create_argument_compression) as create_argument_compression,
    debug.lapi_compression(create_key_value_compression) as create_key_value_compression,
    driver_metadata,
    debug.resolve_lapi_interned_string(domain_id) as domain_id,
    debug.resolve_lapi_interned_strings(create_key_maintainers) as create_key_maintainers,
    trace_context,
    debug.canton_timestamp(record_time) as record_time
  from lapi_events_create;

create or replace view debug.lapi_events_non_consuming_exercise as
  select
    event_sequential_id,
    debug.canton_timestamp(ledger_effective_time) as ledger_effective_time,
    node_index,
    event_offset,
    transaction_id,
    workflow_id,
    command_id,
    application_id,
    debug.resolve_lapi_interned_strings(submitters) as submitters,
    event_id,
    contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_name) as package_name,
    debug.resolve_lapi_interned_strings(flat_event_witnesses) as flat_event_witnesses,
    debug.resolve_lapi_interned_strings(tree_event_witnesses) as tree_event_witnesses,
    create_key_value,
    exercise_choice,
    exercise_argument,
    exercise_result,
    debug.resolve_lapi_interned_strings(exercise_actors) as exercise_actors,
    exercise_child_event_ids,
    debug.lapi_compression(create_key_value_compression) as create_key_value_compression,
    debug.lapi_compression(exercise_argument_compression) as exercise_argument_compression,
    debug.lapi_compression(exercise_result_compression) as exercise_result_compression,
    debug.resolve_lapi_interned_string(domain_id) as domain_id,
    trace_context,
    debug.canton_timestamp(record_time) as record_time
  from lapi_events_non_consuming_exercise;


create or replace view debug.lapi_events_unassign as
  select
    event_sequential_id,
    event_offset,
    update_id,
    workflow_id,
    command_id,
    debug.resolve_lapi_interned_string(submitter) as submitter,
    contract_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(package_name) as package_name,
    debug.resolve_lapi_interned_strings(flat_event_witnesses) as flat_event_witnesses,
    debug.resolve_lapi_interned_string(source_domain_id) as source_domain_id,
    debug.resolve_lapi_interned_string(target_domain_id) as target_domain_id,
    unassign_id,
    reassignment_counter,
    assignment_exclusivity,
    trace_context,
    debug.canton_timestamp(record_time) as record_time
  from lapi_events_unassign;


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

create or replace view debug.lapi_transaction_meta as
  select
    transaction_id,
    event_offset,
    event_sequential_id_first,
    event_sequential_id_last
  from lapi_transaction_meta;

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
    display_name,
    typ,
    rejection_reason,
    is_local,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_party_entries;

create or replace view debug.lapi_pe_assign_id_filter_stakeholder as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_pe_assign_id_filter_stakeholder;

create or replace view debug.lapi_pe_consuming_id_filter_non_stakeholder_informee as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_pe_consuming_id_filter_non_stakeholder_informee;

create or replace view debug.lapi_pe_consuming_id_filter_stakeholder as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_pe_consuming_id_filter_stakeholder;

create or replace view debug.lapi_pe_create_id_filter_non_stakeholder_informee as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_pe_create_id_filter_non_stakeholder_informee;

create or replace view debug.lapi_pe_create_id_filter_stakeholder as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_pe_create_id_filter_stakeholder;

create or replace view debug.lapi_pe_non_consuming_id_filter_informee as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_pe_non_consuming_id_filter_informee;

create or replace view debug.lapi_pe_unassign_id_filter_stakeholder as
  select
    event_sequential_id,
    debug.resolve_lapi_interned_string(template_id) as template_id,
    debug.resolve_lapi_interned_string(party_id) as party_id
  from lapi_pe_unassign_id_filter_stakeholder;

create or replace view debug.lapi_string_interning as
  select
    internal_id,
    external_string
  from lapi_string_interning;
