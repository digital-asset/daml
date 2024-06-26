-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create schema debug;

-- -------------------
--  HELPER FUNCTIONS
-- -------------------

-- convert bigint to the time format used in canton logs
create or replace function debug.canton_timestamp(bigint) returns varchar(300) as
$$
select to_char(to_timestamp($1/1000000.0) at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.US"Z"');
$$
  language sql
  immutable
  returns null on null input;

-- convert the integer representation to the name of the topology mapping
create or replace function debug.topology_mapping(integer) returns char as
$$
select
  case
    when $1 = 1 then 'NamespaceDelegation'
    when $1 = 2 then 'IdentifierDelegation'
    when $1 = 3 then 'DecentralizedNamespaceDefinition'
    when $1 = 4 then 'OwnerToKeyMapping'
    when $1 = 5 then 'DomainTrustCertificate'
    when $1 = 6 then 'ParticipantDomainPermission'
    when $1 = 7 then 'PartyHostingLimits'
    when $1 = 8 then 'VettedPackages'
    when $1 = 9 then 'PartyToParticipant'
    when $1 = 10 then 'AuthorityOf'
    when $1 = 11 then 'DomainParameters'
    when $1 = 12 then 'MediatorDomainState'
    when $1 = 13 then 'SequencerDomainState'
    when $1 = 14 then 'OffboardParticipant'
    when $1 = 15 then 'PurgeTopologyTransaction'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

-- convert the integer representation to the TopologyChangeOp name.
create or replace function debug.topology_change_op(integer) returns varchar(300) as
$$
select
  case
    when $1 = 1 then 'Remove'
    when $1 = 2 then 'Replace'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

-- convert the integer representation to the name of the key purpose
create or replace function debug.key_purpose(integer) returns varchar(300) as
$$
select
  case
    when $1 = 0 then 'Signing'
    when $1 = 1 then 'Encryption'
    else $1::text
  end;
$$
  language sql
  immutable
  returns null on null input;

-- resolve an interned string to the text representation
create or replace function debug.resolve_common_static_string(integer) returns varchar(300) as
$$
select string from common_static_strings where id = $1;
$$
  language sql
  stable
  returns null on null input;

-- resolve an interned sequencer member id to the text representation
create or replace function debug.resolve_sequencer_member(integer) returns varchar(300) as
$$
select member from sequencer_members where id = $1;
$$
  language sql
  stable
  returns null on null input;

-- resolve multiple interned sequencer member ids to the text representation
create or replace function debug.resolve_sequencer_members(integer[]) returns varchar(300)[] as
$$
select array_agg(debug.resolve_sequencer_member(m)) from unnest($1) as m;
$$
  language sql
  stable
  returns null on null input;

-- -------------------
-- VIEWS
-- -------------------

-- Each regular canton table also has a view representation in the debug schema.
-- This way, when debugging, one doesn't have to think, whether there is a more convenient
-- debug view or just the regular table.
-- There are views also for tables that don't yet have columns that warrant a conversion (eg canton timestamp),
-- but future changes to tables should be consciously made to the debug views as well.

create or replace view debug.par_daml_packages as
  select
    package_id,
    data,
    source_description,
    uploaded_at,
    package_size
  from par_daml_packages;

create or replace view debug.par_dars as
  select
    hash_hex,
    hash,
    data,
    name
  from par_dars;

create or replace view debug.par_dar_packages as
select dar_hash_hex, package_id from par_dar_packages;

create or replace view debug.common_crypto_private_keys as
  select
    key_id,
    wrapper_key_id,
    debug.key_purpose(purpose) as purpose,
    data,
    name
  from common_crypto_private_keys;

create or replace view debug.common_kms_metadata_store as
  select
    fingerprint,
    kms_key_id,
    debug.key_purpose(purpose) as purpose
  from common_kms_metadata_store;

create or replace view debug.common_crypto_public_keys as
  select
    key_id,
    debug.key_purpose(purpose) as purpose,
    data,
    name
  from common_crypto_public_keys;

create or replace view debug.par_contracts as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    contract_id,
    instance,
    metadata,
    ledger_create_time,
    request_counter,
    creating_transaction_id,
    package_id,
    template_id,
    contract_salt
  from par_contracts;

create or replace view debug.seq_state_manager_members as
  select
    member,
    debug.canton_timestamp(added_at) as added_at,
    enabled,
    debug.canton_timestamp(latest_acknowledgement) as latest_acknowledgement
  from seq_state_manager_members;

create or replace view debug.seq_state_manager_events as
  select
    member,
    counter,
    debug.canton_timestamp(ts) as ts,
    content,
    trace_context
  from seq_state_manager_events;

create or replace view debug.seq_state_manager_lower_bound as
  select
    single_row_lock,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(ts_initial_topology) as ts_initial_topology
  from seq_state_manager_lower_bound;

create or replace view debug.common_node_id as
  select
    identifier,
    namespace
  from common_node_id;

create or replace view debug.common_party_metadata as
  select
    party_id,
    display_name,
    participant_id,
    submission_id,
    notified,
    debug.canton_timestamp(effective_at) as effective_at
  from common_party_metadata;

create or replace view debug.common_topology_dispatching as
  select
    store_id,
    debug.canton_timestamp(watermark_ts) as watermark_ts
  from common_topology_dispatching;

create or replace view debug.par_active_contracts as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    contract_id,
    change, operation,
    debug.canton_timestamp(ts) as ts,
    request_counter,
    debug.resolve_common_static_string(remote_domain_idx) as remote_domain_idx, transfer_counter
  from par_active_contracts;

create or replace view debug.par_fresh_submitted_transaction as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    root_hash_hex,
    debug.canton_timestamp(request_id) as request_id,
    debug.canton_timestamp(max_sequencing_time) as max_sequencing_time
  from par_fresh_submitted_transaction;

create or replace view debug.par_fresh_submitted_transaction_pruning as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    phase,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(succeeded) as succeeded
  from par_fresh_submitted_transaction_pruning;

create or replace view debug.med_response_aggregations as
  select
    debug.canton_timestamp(request_id) as request_id,
    mediator_confirmation_request,
    version,
    verdict,
    request_trace_context
  from med_response_aggregations;

create or replace view debug.common_sequenced_events as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    sequenced_event,
    type,
    debug.canton_timestamp(ts) as ts,
    sequencer_counter,
    trace_context,
    ignore
  from common_sequenced_events;

create or replace view debug.sequencer_client_pending_sends as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    message_id,
    debug.canton_timestamp(max_sequencing_time) as max_sequencing_time
  from sequencer_client_pending_sends;

create or replace view debug.par_domain_connection_configs as
  select
    domain_alias,
    config,
    status
  from par_domain_connection_configs;

create or replace view debug.par_domains as
  select
    order_number,
    alias,
    domain_id,
    status
  from par_domains;

create or replace view debug.par_event_log as
  select
    log_id,
    debug.canton_timestamp(ts) as ts,
    request_sequencer_counter,
    event_id,
    debug.resolve_common_static_string(associated_domain) as associated_domain,
    local_offset_tie_breaker,
    debug.canton_timestamp(local_offset_effective_time) as local_offset_effective_time,
    local_offset_discriminator,
    content, trace_context
  from par_event_log;

create or replace view debug.par_linearized_event_log as
  select
    global_offset,
    log_id,
    local_offset_tie_breaker,
    debug.canton_timestamp(publication_time) as publication_time,
    debug.canton_timestamp(local_offset_effective_time) as local_offset_effective_time,
    local_offset_discriminator
  from par_linearized_event_log;

create or replace view debug.par_transfers as
  select
    target_domain,
    origin_domain,
    transfer_out_global_offset,
    transfer_in_global_offset,
    debug.canton_timestamp(transfer_out_timestamp) as transfer_out_timestamp,
    transfer_out_request_counter,
    transfer_out_request,
    debug.canton_timestamp(transfer_out_decision_time) as transfer_out_decision_time,
    contract,
    creating_transaction_id,
    transfer_out_result,
    submitter_lf,
    debug.canton_timestamp(time_of_completion_request_counter) as time_of_completion_request_counter,
    debug.canton_timestamp(time_of_completion_timestamp) as time_of_completion_timestamp,
    source_protocol_version
  from par_transfers;

create or replace view debug.par_journal_requests as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    request_counter,
    request_state_index,
    debug.canton_timestamp(request_timestamp) as request_timestamp,
    debug.canton_timestamp(commit_time) as commit_time,
    repair_context
  from par_journal_requests;

create or replace view debug.par_head_clean_counters as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    prehead_counter,
    debug.canton_timestamp(ts) as ts
  from par_head_clean_counters;

create or replace view debug.par_computed_acs_commitments as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    counter_participant,
    debug.canton_timestamp(from_exclusive) as from_exclusive,
    debug.canton_timestamp(to_inclusive) as to_inclusive,
    commitment
  from par_computed_acs_commitments;


create or replace view debug.par_received_acs_commitments as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    sender,
    debug.canton_timestamp(from_exclusive) as from_exclusive,
    debug.canton_timestamp(to_inclusive) as to_inclusive,
    signed_commitment
  from par_received_acs_commitments;

create or replace view debug.par_outstanding_acs_commitments as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    counter_participant,
    debug.canton_timestamp(from_exclusive) as from_exclusive,
    debug.canton_timestamp(to_inclusive) as to_inclusive
  from par_outstanding_acs_commitments;

create or replace view debug.par_last_computed_acs_commitments as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    debug.canton_timestamp(ts) as ts
  from par_last_computed_acs_commitments;

create or replace view debug.par_commitment_snapshot as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    stakeholders_hash,
    stakeholders,
    commitment
  from par_commitment_snapshot;

create or replace view debug.par_commitment_snapshot_time as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    debug.canton_timestamp(ts) as ts,
    tie_breaker
  from par_commitment_snapshot_time;

create or replace view debug.par_commitment_queue as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    sender,
    counter_participant,
    debug.canton_timestamp(from_exclusive) as from_exclusive,
    debug.canton_timestamp(to_inclusive) as to_inclusive,
    commitment,
    commitment_hash
  from par_commitment_queue;

create or replace view debug.par_static_domain_parameters as
  select
    domain_id,
    params
  from par_static_domain_parameters;

create or replace view debug.par_pruning_operation as
  select
    name,
    debug.canton_timestamp(started_up_to_inclusive) as started_up_to_inclusive,
    debug.canton_timestamp(completed_up_to_inclusive) as completed_up_to_inclusive
  from par_pruning_operation;

create or replace view debug.seq_block_height as
  select
    height,
    debug.canton_timestamp(latest_event_ts) as latest_event_ts,
    debug.canton_timestamp(latest_sequencer_event_ts) as latest_sequencer_event_ts
  from seq_block_height;

create or replace view debug.seq_initial_state as
  select
    member,
    counter
  from seq_initial_state;

create or replace view debug.par_active_contract_pruning as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    phase,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(succeeded) as succeeded
  from par_active_contract_pruning;

create or replace view debug.par_commitment_pruning as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    phase,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(succeeded) as succeeded
  from par_commitment_pruning;

create or replace view debug.par_contract_key_pruning as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    phase,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(succeeded) as succeeded
  from par_contract_key_pruning;

create or replace view debug.common_sequenced_event_store_pruning as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    phase,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(succeeded) as succeeded
  from common_sequenced_event_store_pruning;

create or replace view debug.mediator_domain_configuration as
  select
    lock,
    domain_id,
    static_domain_parameters,
    sequencer_connection
  from mediator_domain_configuration;

create or replace view debug.common_head_sequencer_counters as
  select
    debug.resolve_common_static_string(domain_id) as domain_id,
    prehead_counter,
    debug.canton_timestamp(ts) as ts
  from common_head_sequencer_counters;

create or replace view debug.sequencer_members as
  select
    member,
    id,
    debug.canton_timestamp(registered_ts) as registered_ts,
    enabled
  from sequencer_members;

create or replace view debug.sequencer_payloads as
  select
    id,
    instance_discriminator,
    content
  from sequencer_payloads;

create or replace view debug.sequencer_watermarks as
  select
    node_index,
    debug.canton_timestamp(watermark_ts) as watermark_ts,
    sequencer_online
  from sequencer_watermarks;

create or replace view debug.sequencer_counter_checkpoints as
  select
    debug.resolve_sequencer_member(member) as member,
    counter,
    debug.canton_timestamp(ts) as ts,
    debug.canton_timestamp(latest_sequencer_event_ts) as latest_sequencer_event_ts
  from sequencer_counter_checkpoints;

create or replace view debug.sequencer_acknowledgements as
  select
    debug.resolve_sequencer_member(member) as member,
    debug.canton_timestamp(ts) as ts
  from sequencer_acknowledgements;

create or replace view debug.sequencer_lower_bound as
  select
    single_row_lock,
    debug.canton_timestamp(ts) as ts
  from sequencer_lower_bound;

create or replace view debug.sequencer_events as
  select
    debug.canton_timestamp(ts) as ts,
    node_index,
    event_type,
    message_id,
    debug.resolve_sequencer_member(sender) as sender,
    debug.resolve_sequencer_members(recipients) as recipients,
    payload_id,
    debug.canton_timestamp(topology_timestamp) as topology_timestamp,
    trace_context,
    error
  from sequencer_events;

create or replace view debug.par_pruning_schedules as
  select
    lock,
    cron,
    max_duration,
    retention,
    prune_internally_only
  from par_pruning_schedules;

create or replace view debug.sequencer_authentication_nonces as
  select
    nonce,
    member,
    debug.canton_timestamp(generated_at_ts) as generated_at_ts,
    debug.canton_timestamp(expire_at_ts) as expire_at_ts
  from sequencer_authentication_nonces;

create or replace view debug.sequencer_authentication_tokens as
  select
    token,
    member,
    debug.canton_timestamp(expire_at_ts) as expire_at_ts
  from sequencer_authentication_tokens;

create or replace view debug.par_in_flight_submission as
  select
    change_id_hash,
    submission_id,
    submission_domain,
    message_id,
    debug.canton_timestamp(sequencing_timeout) as sequencing_timeout,
    sequencer_counter,
    debug.canton_timestamp(sequencing_time) as sequencing_time,
    tracking_data,
    root_hash_hex,
    trace_context
from par_in_flight_submission;

create or replace view debug.par_settings as
  select
    client,
    max_infight_validation_requests,
    max_submission_rate,
    max_deduplication_duration,
    max_submission_burst_factor
  from par_settings;

create or replace view debug.par_command_deduplication as
  select
    change_id_hash,
    application_id,
    command_id,
    act_as,
    offset_definite_answer,
    debug.canton_timestamp(publication_time_definite_answer) as publication_time_definite_answer,
    submission_id_definite_answer,
    trace_context_definite_answer,
    offset_acceptance,
    debug.canton_timestamp(publication_time_acceptance) as publication_time_acceptance,
    submission_id_acceptance,
    trace_context_acceptance
  from par_command_deduplication;

create or replace view debug.par_command_deduplication_pruning as
  select
    client,
    pruning_offset,
    debug.canton_timestamp(publication_time) as publication_time
  from par_command_deduplication_pruning;

create or replace view debug.sequencer_domain_configuration as
  select
    lock,
    domain_id,
    static_domain_parameters
  from sequencer_domain_configuration;

create or replace view debug.mediator_deduplication_store as
  select
    mediator_id,
    uuid,
    debug.canton_timestamp(request_time) as request_time,
    debug.canton_timestamp(expire_after) as expire_after
  from mediator_deduplication_store;

create or replace view debug.common_pruning_schedules as
  select
    node_type,
    cron,
    max_duration,
    retention
  from common_pruning_schedules;

create or replace view debug.seq_in_flight_aggregation as
  select
    aggregation_id,
    debug.canton_timestamp(max_sequencing_time) as max_sequencing_time,
    aggregation_rule
  from seq_in_flight_aggregation;

create or replace view debug.seq_in_flight_aggregated_sender as
  select
    aggregation_id,
    sender,
    debug.canton_timestamp(sequencing_timestamp) as sequencing_timestamp,
    signatures
  from seq_in_flight_aggregated_sender;

create or replace view debug.common_topology_transactions as
  select
    id,
    store_id,
    debug.canton_timestamp(sequenced) as sequenced,
    debug.topology_mapping(transaction_type) as transaction_type,
    namespace,
    identifier,
    mapping_key_hash,
    serial_counter,
    debug.canton_timestamp(valid_from) as valid_from,
    debug.canton_timestamp(valid_until) as valid_until,
    debug.topology_change_op(operation) as operation,
    instance,
    tx_hash,
    rejection_reason,
    is_proposal,
    representative_protocol_version,
    hash_of_signatures
  from common_topology_transactions;

create or replace view debug.seq_traffic_control_balance_updates as
  select
    member,
    debug.canton_timestamp(sequencing_timestamp) as sequencing_timestamp,
    balance,
    serial
  from seq_traffic_control_balance_updates;

create or replace view debug.seq_traffic_control_consumed_journal as
  select
    member,
    debug.canton_timestamp(sequencing_timestamp) as sequencing_timestamp,
    extra_traffic_consumed,
    base_traffic_remainder,
    last_consumed_cost
  from seq_traffic_control_consumed_journal;

create or replace view debug.seq_traffic_control_initial_timestamp as
  select
    debug.canton_timestamp(initial_timestamp) as initial_timestamp
  from seq_traffic_control_initial_timestamp;

create or replace view debug.ord_completed_epochs as
  select
    epoch_number,
    start_block_number,
    epoch_length
  from ord_completed_epochs;

create or replace view debug.ord_active_epoch as
  select
    epoch_number,
    block_number
  from ord_active_epoch;

create or replace view debug.ord_availability_batch as
  select
    id,
    batch
  from ord_availability_batch;

create or replace view debug.ord_pbft_messages as
  select
    block_number,
    message,
    discriminator,
    from_sequencer_id
  from ord_pbft_messages;

create or replace view debug.ord_metadata_output_blocks as
  select
    block_number,
    bft_ts,
    last_topology_ts
  from ord_metadata_output_blocks;

create or replace view debug.common_static_strings as
  select
    id,
    string,
    source
  from common_static_strings;
