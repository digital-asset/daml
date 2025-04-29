-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table par_daml_packages (
  package_id varchar collate "C" not null primary key,
  data bytea not null,
  name varchar collate "C" not null,
  version varchar collate "C" not null,
    -- UTC timestamp is stored in microseconds relative to EPOCH
  uploaded_at bigint not null,
    -- The size of the archive payload (i.e., the serialized DAML-LF package), in bytes
  package_size bigint not null
);

create table par_dars (

  main_package_id varchar collate "C" not null primary key,
  data bytea not null,
  description varchar collate "C" null,
  name varchar collate "C" not null,
  version varchar collate "C" not null

);

-- This table tracks the packages contained in the uploaded DARs
-- The table contains a (main_package_id, package_id) pair iff:
--  * The main_package_id is in the dars table
--  * The package_id is in the daml_packages table
--  * The corresponding DAR from the dars table contains the package with id package_id
create table par_dar_packages (
  main_package_id varchar collate "C" not null,
  package_id varchar collate "C" not null,

  foreign key (main_package_id) references par_dars(main_package_id) on delete cascade,
  foreign key (package_id) references par_daml_packages(package_id) on delete cascade,

  primary key (main_package_id, package_id)
);

create table common_crypto_private_keys (
  -- fingerprint of the key
  key_id varchar collate "C" primary key,
  -- Add a new optional wrapper_key_id field to store the encryption key id for the encrypted private store
  wrapper_key_id varchar collate "C",
  -- key purpose identifier
  purpose smallint not null,
  -- Protobuf serialized key including metadata
  data bytea not null,
  -- optional name of the key
  name varchar collate "C"
);

-- Store metadata information about KMS keys
create table common_kms_metadata_store (
  fingerprint varchar collate "C" not null,
  kms_key_id varchar collate "C" not null,
  purpose smallint not null,
  key_usage integer array null,
  primary key (fingerprint)
);


create table common_crypto_public_keys (
  -- fingerprint of the key
  key_id varchar collate "C" primary key,
  -- key purpose identifier
  purpose smallint not null,
  -- Protobuf serialized key
  data bytea not null,
  -- optional name of the key
  name varchar collate "C"
);

-- Stores the immutable contracts, however a creation of a contract can be rolled back.
create table par_contracts (
  contract_id bytea not null,
  -- The contract is serialized using the LF contract proto serializer.
  instance bytea not null,
  -- Metadata: signatories, stakeholders, keys
  -- Stored as a Protobuf blob as H2 will only support typed arrays in 1.4.201
  metadata bytea not null,
  -- The ledger time when the contract was created.
  ledger_create_time varchar collate "C" not null,
  -- We store metadata of the contract instance for inspection
  package_id varchar collate "C" not null,
  template_id varchar collate "C" not null,
  contract_salt bytea not null,
  primary key (contract_id)
);

-- Index to speedup ContractStore.find
-- package_id comes before template_id, because queries with package_id and without template_id make more sense than vice versa.
-- contract_id is left out, because a query with contract_id can be served with the primary key.
create index idx_par_contracts_find on par_contracts(package_id, template_id);

-- provides a serial enumeration of static strings so we don't store the same string over and over in the db
-- currently only storing uids
create table common_static_strings (
  -- serial identifier of the string (local to this node)
  id serial not null primary key,
  -- the expression
  string varchar collate "C" not null,
  -- the source (what kind of string are we storing here)
  source integer not null,
  unique(string, source)
);

-- Stores the identity of the node - its assigned member identity and its instance
-- This table should always have at most one entry which is a unique identifier for the member which consists of a string identifier and a fingerprint of a signing key
create table common_node_id(
  identifier varchar collate "C" not null,
  namespace varchar collate "C" not null,
  primary key (identifier, namespace)
);

-- Stores the local party metadata
create table common_party_metadata (
  -- party id as string
  party_id varchar collate "C" not null,
  -- the main participant id of this party which is our participant if the party is on our node (preferred) or the remote participant
  participant_id varchar collate "C" null,
  -- the submission id used to synchronise the ledger api server
  submission_id varchar collate "C" null,
  -- notification flag used to keep track about pending synchronisations
  notified boolean not null default false,
  -- the time when this change will be or became effective
  effective_at bigint not null,
  primary key (party_id)
);
create index idx_common_party_metadata_notified on common_party_metadata(notified);

-- Stores the dispatching watermarks
create table common_topology_dispatching (
  -- the target store we are dispatching to (from is always authorized)
  store_id varchar collate "C" not null primary key,
  -- the dispatching watermark
  watermark_ts bigint not null
);

-- change type: activation [create, assign], deactivation [archive, unassign]
-- `deactivation` comes before `activation` so that comparisons `(timestamp, change) <= (bound, 'deactivation')`
-- select only deactivations if the timestamp matches the bound.
create type change_type as enum ('deactivation', 'activation');

-- The specific operation type that introduced a contract change.
create type operation_type as enum ('create', 'add', 'assign', 'archive', 'purge', 'unassign');

-- Maintains the status of contracts
create table par_active_contracts (
  -- As a participant can be connected to multiple synchronizers, the active contracts are stored under a synchronizer id.
  synchronizer_idx integer not null,
  contract_id bytea not null,
  change change_type not null,
  operation operation_type not null,
  -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
  ts bigint not null,
  -- Repair counter of the time of change lexicographically relative to ts, min-value for non-repairs
  repair_counter bigint not null,
  -- optional remote synchronizer index in case of reassignments
  remote_synchronizer_idx integer,
  reassignment_counter bigint default null,
  primary key (synchronizer_idx, contract_id, ts, repair_counter, change)
);

create index idx_par_active_contracts_contract_id on par_active_contracts (contract_id);
create index idx_par_active_contracts_ts_synchronizer_idx on par_active_contracts (ts, synchronizer_idx);
create index idx_par_active_contracts_pruning on par_active_contracts (synchronizer_idx, ts) where change = 'deactivation';

-- Tables for new submission tracker
create table par_fresh_submitted_transaction (
  synchronizer_idx integer not null,
  root_hash_hex varchar collate "C" not null,
  request_id bigint not null,
  max_sequencing_time bigint not null,
  primary key (synchronizer_idx, root_hash_hex)
);

create type pruning_phase as enum ('started', 'completed');

create table par_fresh_submitted_transaction_pruning (
  synchronizer_idx integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (synchronizer_idx)
);

create table med_response_aggregations (
  -- identified by the sequencer timestamp (UTC timestamp in microseconds relative to EPOCH)
  request_id bigint not null primary key,
  mediator_confirmation_request bytea not null,
  -- UTC timestamp is stored in microseconds relative to EPOCH
  finalization_time bigint not null,
  verdict bytea not null,
  request_trace_context bytea not null
);

-- Stores the received sequencer messages
create table common_sequenced_events (
  -- discriminate between different users of the sequenced events tables
  synchronizer_idx integer not null,
  -- Proto serialized signed message
  sequenced_event bytea not null,
  -- Explicit fields to query the messages, which are stored as blobs
  type varchar collate "C" not null check(type in ('del', 'err', 'ign')),
  -- Timestamp of the time of change in microsecond precision relative to EPOCH
  ts bigint not null,
  -- Sequencer counter of the time of change
  sequencer_counter bigint not null,
  -- serialized trace context associated with the event
  trace_context bytea not null,
  -- flag to skip problematic events
  ignore boolean not null,
  -- The sequencer ensures that the timestamp is unique
  primary key (synchronizer_idx, ts)
);

create unique index idx_common_sequenced_events_sequencer_counter on common_sequenced_events(synchronizer_idx, sequencer_counter);

-- Track what send requests we've made but have yet to observe being sequenced.
-- If events are not observed by the max sequencing time we know that the send will never be processed.
create table sequencer_client_pending_sends (
  -- synchronizer (index) for distinguishing between different sequencer clients in the same node
  synchronizer_idx integer not null,

  -- the message id of the send being tracked (expected to be unique for the sequencer client while the send is in-flight)
  message_id varchar collate "C" not null,

  -- the message id should be unique for the sequencer client
  primary key (synchronizer_idx, message_id),

  -- the max sequencing time of the send request (UTC timestamp in microseconds relative to EPOCH)
  max_sequencing_time bigint not null
);

create table par_synchronizer_connection_configs(
  synchronizer_alias varchar collate "C" not null primary key,
  config bytea, -- the protobuf-serialized versioned synchronizer connection config
  status char(1) default 'A' not null
);

-- used to register all synchronizers that a participant connects to
create table par_synchronizers(
  -- to keep track of the order synchronizers were registered
  order_number serial not null primary key,
  -- synchronizer human readable alias
  alias varchar collate "C" not null unique,
  -- synchronizer id
  synchronizer_id varchar collate "C" not null unique,
  status char(1) default 'A' not null,
  unique (alias, synchronizer_id)
);

create table par_reassignments (
  -- reassignment id
  target_synchronizer_idx integer not null,
  source_synchronizer_idx integer not null,

  primary key (target_synchronizer_idx, source_synchronizer_idx, unassignment_timestamp),

  unassignment_global_offset bigint,
  assignment_global_offset bigint,

  -- UTC timestamp in microseconds relative to EPOCH
  unassignment_timestamp bigint not null,
  unassignment_request bytea,

  -- defined if reassignment was completed
  -- UTC timestamp in microseconds relative to EPOCH
  assignment_timestamp bigint,
  contracts bytea array not null
);

-- stores all requests for the request journal
create table par_journal_requests (
  synchronizer_idx integer not null,
  request_counter bigint not null,
  request_state_index smallint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  request_timestamp bigint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  -- is set only if the request is clean
  commit_time bigint,
  primary key (synchronizer_idx, request_counter)
);
create unique index idx_journal_request_timestamp on par_journal_requests (synchronizer_idx, request_timestamp);
create index idx_journal_request_commit_time on par_journal_requests (synchronizer_idx, commit_time);

-- locally computed ACS commitments to a specific period, counter-participant and synchronizer
create table par_computed_acs_commitments (
  synchronizer_idx integer not null,
  counter_participant varchar collate "C" not null,
  -- UTC timestamp in microseconds relative to EPOCH
  from_exclusive bigint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  to_inclusive bigint not null,
  -- the "raw" cryptographic commitment (AcsCommitment.CommitmentType) in its serialized format
  commitment bytea not null,
  primary key (synchronizer_idx, counter_participant, from_exclusive, to_inclusive),
  constraint check_nonempty_interval_computed check(to_inclusive > from_exclusive)
);

-- ACS commitments received from counter-participants
create table par_received_acs_commitments (
  synchronizer_idx integer not null,
  -- the counter-participant who sent the commitment
  sender varchar collate "C" not null,
  -- UTC timestamp in microseconds relative to EPOCH
  from_exclusive bigint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  to_inclusive bigint not null,
  -- the received signed commitment message (including all the other fields) in a serialized form
  signed_commitment bytea not null,
  constraint check_to_after_from check(to_inclusive > from_exclusive)
);

create index idx_par_full_commitment on par_received_acs_commitments (synchronizer_idx, sender, from_exclusive, to_inclusive);

-- the participants whose remote commitments are outstanding
create table par_outstanding_acs_commitments (
  synchronizer_idx integer not null,
  -- UTC timestamp in microseconds relative to EPOCH
  from_exclusive bigint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  to_inclusive bigint not null,
  counter_participant varchar collate "C" not null,
  matching_state smallint not null,
  multi_hosted_cleared bool not null default false,
  constraint check_nonempty_interval_outstanding check(to_inclusive > from_exclusive)
);

create unique index idx_par_acs_unique_interval on par_outstanding_acs_commitments(synchronizer_idx, counter_participant, from_exclusive, to_inclusive);

create index idx_par_outstanding_acs_commitments_by_time on par_outstanding_acs_commitments (synchronizer_idx, from_exclusive);

-- the last timestamp for which the commitments have been computed, stored and sent
create table par_last_computed_acs_commitments (
  synchronizer_idx integer primary key,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null
);

-- Stores the snapshot ACS commitments (per stakeholder set)
create table par_commitment_snapshot (
  synchronizer_idx integer not null,
  -- A stable reference to a stakeholder set, that doesn't rely on the Protobuf encoding being deterministic
  -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
  stakeholders_hash varchar collate "C" not null,
  stakeholders bytea not null,
  commitment bytea not null,
  primary key (synchronizer_idx, stakeholders_hash)
);

-- Stores the time (along with a tie-breaker) of the ACS commitment snapshot
create table par_commitment_snapshot_time (
  synchronizer_idx integer not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  tie_breaker bigint not null,
  primary key (synchronizer_idx)
);

-- Remote commitments that were received but could not yet be checked because the local participant is lagging behind
create table par_commitment_queue (
  synchronizer_idx integer not null,
  sender varchar collate "C" not null,
  counter_participant varchar collate "C" not null,
  -- UTC timestamp in microseconds relative to EPOCH
  from_exclusive bigint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  to_inclusive bigint not null,
  commitment bytea not null,
  constraint check_nonempty_interval_queue check(to_inclusive > from_exclusive),
  primary key (synchronizer_idx, sender, counter_participant, from_exclusive, to_inclusive, commitment)
);

create index idx_par_commitment_queue_by_time on par_commitment_queue (synchronizer_idx, to_inclusive);

-- the (current) synchronizer parameters for the given synchronizer
create table par_static_synchronizer_parameters (
  synchronizer_id varchar collate "C" primary key,
  -- serialized form
  params bytea not null
);

-- 'unassigned' comes before 'assigned' so that comparisons `(timestamp, change) <= (bound, 'unassigned')`
-- -- select only unassignments if the timestamp matches the bound.
create type key_status as enum ('unassigned', 'assigned');

-- Data about the last pruning operations
create table par_pruning_operation (
  -- dummy field to enforce a single row
  name varchar collate "C" not null primary key,
  started_up_to_inclusive bigint,
  completed_up_to_inclusive bigint
);

-- this table is meant to be used by blockchain based external sequencers
-- in order to keep track of the blocks processed associating block heights
-- with the timestamp of the last event in it
create table seq_block_height (
  height bigint primary key check(height >= -1),
  latest_event_ts bigint not null,
  -- The column latest_sequencer_event_ts denotes the sequencing timestamp of an event
  -- addressed to the sequencer such that
  -- there is no further event addressed to the sequencer between this timestamp
  -- and the last event in the block.
  -- NULL if no such timestamp is known, e.g., because this block was added before this column was added.
  latest_sequencer_event_ts bigint
);

-- Maintains the latest timestamp (by synchronizer) for which ACS pruning has started or finished
create table par_active_contract_pruning (
  synchronizer_idx integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (synchronizer_idx)
);

-- Maintains the latest timestamp (by synchronizer) for which ACS commitment pruning has started or finished
create table par_commitment_pruning (
  synchronizer_idx integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (synchronizer_idx)
);

-- Maintains the latest timestamp (by sequencer client) for which the sequenced event store pruning has started or finished
create table common_sequenced_event_store_pruning (
  synchronizer_idx integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (synchronizer_idx)
);

-- table to contain the values provided by the synchronizer to the mediator node for initialization.
-- we persist these values to ensure that the mediator can always initialize itself with these values
-- even if it was to crash during initialization.
create table mediator_synchronizer_configuration (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  synchronizer_id varchar collate "C" not null,
  static_synchronizer_parameters bytea not null,
  sequencer_connection bytea not null
);

-- the last recorded head clean sequencer counter for each synchronizer
create table common_head_sequencer_counters (
  -- discriminate between different users of the sequencer counter tracker tables
  synchronizer_idx integer not null primary key,
  prehead_counter bigint not null, -- sequencer counter before the first unclean sequenced event
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null
);


-- we create a unique integer id for each member that is more efficient to use in the events table
-- members can read all events from `registered_ts`
create table sequencer_members (
    member varchar collate "C" primary key,
    id serial unique,
    registered_ts bigint not null,
    -- we keep the latest event's timestamp below the pruning timestamp,
    -- so that we can produce a valid first event above the pruning timestamp with previousTimestamp populated
    -- also used in sequencer state transfer
    pruned_previous_event_timestamp bigint,
    enabled bool not null default true
);

-- stores payloads for send events
-- callers should ensure that a unique id is assigned
create table sequencer_payloads (
  -- expected to be a timestamp assigned using the partitioned timestamp generator based on the writer index
  id bigint primary key,
  -- identifier used by writers to determine on conflicts whether it was the payload they wrote or if another writer
  -- is active with the same writer index (can happen as we use the non-locked unsafe storage method for payloads).
  -- sized to hold a uuid.
  instance_discriminator varchar collate "C" not null,
  content bytea not null
);

-- sequencer instances maintain the lowest timestamp for which it is safe to read from
create table sequencer_watermarks (
  node_index integer primary key,
  watermark_ts bigint not null,
  sequencer_online bool not null
);

-- record the latest acknowledgement sent by a sequencer client of a member for the latest event they have successfully
-- processed and will not re-read.
create table sequencer_acknowledgements (
  member integer primary key,
  ts bigint not null
);

-- exclusive lower bound of when events can be read
-- if empty it means all events from epoch can be read
-- is updated when sequencer is pruned meaning that earlier events can no longer be read (and likely no longer exist)
create table sequencer_lower_bound (
  single_row_lock char(1) not null default 'X' primary key check(single_row_lock = 'X'),
  ts bigint not null,
  latest_topology_client_timestamp bigint
);

-- postgres events table (differs from h2 in the recipients array definition)
create table sequencer_events (
  ts bigint primary key,
  node_index smallint not null,
  -- single char to indicate the event type: D for deliver event, E for deliver error, R for deliver receipt
  event_type char(1) not null
      constraint event_type_enum check (event_type in ('D', 'E', 'R')),
  message_id varchar collate "C" null,
  sender integer null,
  -- null if event goes to everyone, otherwise specify member ids of recipients
  recipients integer array null,
  -- null if the event is a deliver error
  -- intentionally not creating a foreign key here for performance reasons
  payload_id bigint null,
  -- optional topology timestamp for deliver if supplied
  topology_timestamp bigint null,
  -- trace context associated with the event
  trace_context bytea not null,
  error bytea,
  -- the last cost consumed at sequencing_timestamp
  consumed_cost bigint,
  -- total extra traffic consumed at the time of the event
  extra_traffic_consumed bigint,
  -- extra traffic remainder at the time of the event
  base_traffic_remainder bigint
);

-- Sequence of local offsets used by the participant event publisher
create sequence participant_event_publisher_local_offsets minvalue 0 start with 0;

-- pruning_schedules with pruning flag specific to participant pruning
create table par_pruning_schedules (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  cron varchar collate "C" not null,
  max_duration bigint not null, -- positive number of seconds
  retention bigint not null, -- positive number of seconds
  prune_internally_only boolean not null default false -- whether to prune only canton-internal stores not visible to ledger api
);

-- store in-flight submissions
create table par_in_flight_submission (
  -- hash of the change ID as a hex string
  change_id_hash varchar collate "C" primary key,

  submission_id varchar collate "C" null,

  submission_synchronizer_id varchar collate "C" not null,
  message_id varchar collate "C" not null,

  -- Sequencer timestamp after which this submission will not be sequenced any more, in microsecond precision relative to EPOCH
  -- If set, this submission is considered unsequenced.
  sequencing_timeout bigint,
  -- Sequencer timestamp assigned to this submission, in microsecond precision relative to EPOCH
  -- Must be set iff sequencing_timeout is not set.
  sequencing_time bigint,

  -- Tracking data for producing a rejection event.
  -- Optional; omitted if other code paths ensure that an event is produced
  -- Must be null if sequencing_timeout is not set.
  tracking_data bytea,

  -- Add root hash to in-flight submission tracker store
  root_hash_hex varchar collate "C" default null,

  trace_context bytea not null
);

create index idx_par_in_flight_submission_root_hash on par_in_flight_submission (root_hash_hex);
create index idx_par_in_flight_submission_timeout on par_in_flight_submission (submission_synchronizer_id, sequencing_timeout);
create index idx_par_in_flight_submission_sequencing on par_in_flight_submission (submission_synchronizer_id, sequencing_time);
create index idx_par_in_flight_submission_message_id on par_in_flight_submission (submission_synchronizer_id, message_id);

create table par_settings(
  client integer primary key, -- dummy field to enforce at most one row
  max_infight_validation_requests integer,
  max_submission_rate integer,
  max_deduplication_duration bytea, -- non-negative finite duration
  max_submission_burst_factor double precision not null default 0.5
);

create table par_command_deduplication (
  -- hash of the change ID (user_id + command_id + act_as) as a hex string
  change_id_hash varchar collate "C" primary key,

  -- the user ID that requested the change
  user_id varchar collate "C" not null,
  -- the command ID
  command_id varchar collate "C" not null,
  -- the act as parties serialized as a Protobuf blob
  act_as bytea not null,

  -- the highest Ledger API offset that yields a completion event with a definite answer for the change ID hash
  offset_definite_answer bigint not null,
  -- the publication time of the offset_definite_answer, measured in microseconds relative to EPOCH
  publication_time_definite_answer bigint not null,
  submission_id_definite_answer varchar collate "C", -- always optional
  trace_context_definite_answer bytea not null,

  -- the highest Ledger API offset that yields an accepting completion event for the change ID hash
  -- always at most the offset_definite_answer
  -- null if there have only been rejections since the last pruning
  offset_acceptance bigint,
  -- the publication time of the offset_acceptance, measured in microseconds relative to EPOCH
  publication_time_acceptance bigint,
  submission_id_acceptance varchar collate "C",
  trace_context_acceptance bytea
);
create index idx_par_command_dedup_offset on par_command_deduplication(offset_definite_answer);

create table par_command_deduplication_pruning (
  client integer primary key, -- dummy field to enforce at most one row
  -- The highest offset pruning has been started at.
  pruning_offset varchar collate "C" not null,
  -- An upper bound to publication times of pruned offsets.
  publication_time bigint not null
);

-- table to contain the values provided by the synchronizer to the sequencer node for initialization.
-- we persist these values to ensure that the sequencer can always initialize itself with these values
-- even if it was to crash during initialization.
create table sequencer_synchronizer_configuration (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  synchronizer_id varchar collate "C" not null,
  static_synchronizer_parameters bytea not null
);


create table mediator_deduplication_store (
  mediator_id varchar collate "C" not null,
  uuid varchar collate "C" not null,
  request_time bigint not null,
  expire_after bigint not null
);
create index idx_mediator_deduplication_store_expire_after on mediator_deduplication_store(expire_after, mediator_id);

create table common_pruning_schedules(
  -- node_type is one of "MED", or "SEQ"
  -- since mediator and sequencer sometimes share the same db
  node_type varchar collate "C" not null primary key,
  cron varchar collate "C" not null,
  max_duration bigint not null, -- positive number of seconds
  retention bigint not null -- positive number of seconds
);

create table seq_in_flight_aggregation(
  aggregation_id varchar collate "C" not null primary key,
  -- UTC timestamp in microseconds relative to EPOCH
  first_sequencing_timestamp bigint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  max_sequencing_time bigint not null,
  -- serialized aggregation rule,
  aggregation_rule bytea not null
);

-- NB: Do not add other indexes to this table, as this will confuse the planner to ignore the BRIN index
create index idx_seq_in_flight_aggregation_temporal_brin
    on seq_in_flight_aggregation
    using brin (first_sequencing_timestamp, max_sequencing_time);

create table seq_in_flight_aggregated_sender(
  aggregation_id varchar collate "C" not null,
  sender varchar collate "C" not null,
  -- UTC timestamp in microseconds relative to EPOCH
  sequencing_timestamp bigint not null,
  -- UTC timestamp in microseconds relative to EPOCH
  max_sequencing_time bigint not null,
  signatures bytea not null,
  primary key (aggregation_id, sender),
  constraint foreign_key_seq_in_flight_aggregated_sender foreign key (aggregation_id) references seq_in_flight_aggregation(aggregation_id) on delete cascade
);

-- NB: Do not add other indexes to this table, as this will confuse the planner to ignore the BRIN index
create index idx_seq_in_flight_aggregated_sender_temporal_brin
    on seq_in_flight_aggregated_sender
        using brin (sequencing_timestamp, max_sequencing_time);

-- stores the topology-x state transactions
create table common_topology_transactions (
  -- serial identifier used to preserve insertion order
  id bigserial not null primary key,
  -- the id of the store
  store_id varchar collate "C" not null,
  -- the timestamp at which the transaction is sequenced by the sequencer
  -- UTC timestamp in microseconds relative to EPOCH
  sequenced bigint not null,
  -- type of transaction (refer to TopologyMapping.Code)
  transaction_type integer not null,
  -- the namespace this transaction is operating on
  namespace varchar collate "C" not null,
  -- the optional identifier this transaction is operating on (yields a uid together with namespace)
  -- a null value is represented as "", as null is never equal in indexes for postgres, which would
  -- break the unique index
  identifier varchar collate "C" not null,
  -- The topology mapping key hash, to uniquify and aid efficient lookups.
  -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
  mapping_key_hash varchar collate "C" not null,
  -- the serial_counter describes the change order within transactions of the same mapping_key_hash
  -- (redundant also embedded in instance)
  serial_counter integer not null,
  -- validity window, UTC timestamp in microseconds relative to EPOCH
  -- so `TopologyChangeOp.Replace` transactions have an effect for valid_from < t <= valid_until
  -- a `TopologyChangeOp.Remove` will have valid_from = valid_until
  valid_from bigint not null,
  valid_until bigint null,
  -- operation
  -- 1: Remove
  -- 2: Replace (upsert/merge semantics)
  operation integer not null,
  -- The raw transaction, serialized using the proto serializer.
  instance bytea not null,
  -- The transaction hash, to uniquify and aid efficient lookups.
  -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
  tx_hash varchar collate "C" not null,
  -- flag / reason why this transaction is being rejected
  -- therefore: if this field is NULL, then the transaction is included. if it is non-null shows the reason why it is invalid
  rejection_reason varchar collate "C" null,
  -- is_proposal indicates whether the transaction still needs BFT-consensus
  -- false if and only if the transaction is accepted
  -- (redundant also embedded in instance)
  is_proposal boolean not null,
  representative_protocol_version integer not null,
  -- the hash of the transaction's signatures. this disambiguates multiple transactions/proposals with the same
  -- tx_hash but different signatures
  hash_of_signatures varchar collate "C" not null,
  -- index used for idempotency during crash recovery
  unique (store_id, mapping_key_hash, serial_counter, valid_from, operation, representative_protocol_version, hash_of_signatures, tx_hash)
);
create index idx_common_topology_transactions on common_topology_transactions (store_id, transaction_type, namespace, identifier, valid_until, valid_from);

-- Stores the traffic purchased entry updates
create table seq_traffic_control_balance_updates (
  -- member the traffic purchased entry update is for
  member varchar collate "C" not null,
  -- timestamp at which the update was sequenced
  sequencing_timestamp bigint not null,
  -- total traffic purchased entry after the update
  balance bigint not null,
  -- used to keep balance updates idempotent
  serial bigint not null,
  -- traffic states have a unique sequencing_timestamp per member
  primary key (member, sequencing_timestamp)
);

-- Stores the initial timestamp during onboarding. Allows to survive a restart immediately after onboarding
create table seq_traffic_control_initial_timestamp (
  -- Timestamp used to initialize the sequencer during onboarding, and the balance manager as well
  initial_timestamp bigint not null,
  primary key (initial_timestamp)
);

-- Stores the traffic consumed as a journal
create table seq_traffic_control_consumed_journal (
    -- member the traffic consumed entry is for
       member varchar collate "C" not null,
    -- timestamp at which the event that caused traffic to be consumed was sequenced
       sequencing_timestamp bigint not null,
    -- total traffic consumed at sequencing_timestamp
       extra_traffic_consumed bigint not null,
    -- base traffic remainder at sequencing_timestamp
       base_traffic_remainder bigint not null,
    -- the last cost consumed at sequencing_timestamp
       last_consumed_cost bigint not null,
    -- traffic entries have a unique sequencing_timestamp per member
       primary key (member, sequencing_timestamp)
);

--   BFT Ordering Tables

-- Stores metadata for epochs
-- Individual blocks/transactions exist in separate table
create table ord_epochs (
  -- strictly-increasing, contiguous epoch number
  epoch_number bigint not null primary key,
  -- first block sequence number (globally) of the epoch
  start_block_number bigint not null,
  -- number of total blocks in the epoch
  epoch_length bigint not null,
  -- Sequencing instant of the topology snapshot in force for the epoch
  topology_ts bigint not null,
  -- whether the epoch is in progress
  in_progress bool not null
);

create table ord_availability_batch (
  id varchar collate "C" not null,
  batch bytea not null,
  -- assigned at batch creation and used to calculate epoch expiration
  epoch_number bigint not null,
  primary key (id)
);

-- messages stored during the progress of a block possibly across different pbft views
create table ord_pbft_messages_in_progress(
  -- global sequence number of the ordered block
  block_number bigint not null,

  -- epoch number of the block
  epoch_number bigint not null,

  -- view number
  view_number smallint not null,

  -- pbft message for the block
  message bytea not null,

  -- pbft message discriminator (0 = pre-prepare, 1 = prepare, 2 = commit)
  discriminator smallint not null,

  -- sender of the message
  from_sequencer_id varchar collate "C" not null,

  -- for each block number, we only expect one message of each kind for the same sender and view number.
  primary key (block_number, view_number, from_sequencer_id, discriminator)
);

-- final pbft messages stored only once for each block when it completes
-- currently only commit messages and the pre-prepare used for that block
-- overwrite attempts with different commit sets can happen during catch-up for already completed blocks and are ignored
create table ord_pbft_messages_completed(
  -- global sequence number of the ordered block
  block_number bigint not null,

  -- epoch number of the block
  epoch_number bigint not null,

  -- pbft message for the block
  message bytea not null,

  -- pbft message discriminator (0 = pre-prepare, 2 = commit)
  discriminator smallint not null,

  -- sender of the message
  from_sequencer_id varchar collate "C" not null,

  -- for each completed block number, we only expect one message of each kind for the same sender.
  -- in the case of pre-prepare, we only expect one message for the whole block, but for simplicity
  -- we won't differentiate that at the database level.
  primary key (block_number, epoch_number, from_sequencer_id, discriminator)
);

-- Stores metadata for blocks that have been assigned timestamps in the output module
create table ord_metadata_output_blocks (
  epoch_number bigint not null,
  block_number bigint not null primary key,
  bft_ts bigint not null
);

-- Stores output metadata for epochs
create table ord_metadata_output_epochs (
  epoch_number bigint not null primary key,
  could_alter_ordering_topology bool not null
);

-- inclusive lower bound of when blocks can be read.
-- if empty it means all blocks can be read.
-- is updated when bft-sequencer is pruned meaning that
-- only events from later epochs can be served (earlier events have probably been pruned)
create table ord_output_lower_bound (
  single_row_lock char(1) not null default 'X' primary key check(single_row_lock = 'X'),
  epoch_number bigint not null,
  block_number bigint not null
);

-- Stores P2P endpoints from the configuration or admin command
create table ord_p2p_endpoints (
  address varchar collate "C" not null,
  port smallint not null,
  transport_security bool not null,
  custom_server_trust_certificates bytea null, -- PEM string
  client_certificate_chain bytea null, -- PEM string
  client_private_key_file varchar collate "C" null, -- path to a file containing the client private key
  primary key (address, port, transport_security)
);

-- Auto-vacuum settings for large sequencer tables: these are defaults set based on testing of CN CILR test deployment
alter table sequencer_events
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
        );

alter table sequencer_payloads
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
        );

alter table seq_block_height
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
        );

alter table seq_traffic_control_consumed_journal
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
        );

alter table seq_in_flight_aggregated_sender
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
        );

alter table seq_in_flight_aggregation
    set (
        autovacuum_vacuum_scale_factor = 0.0,
        autovacuum_vacuum_threshold = 10000,
        autovacuum_vacuum_cost_limit = 2000,
        autovacuum_vacuum_cost_delay = 5,
        autovacuum_vacuum_insert_scale_factor = 0.0,
        autovacuum_vacuum_insert_threshold = 100000
        );

-- Stores participants we should not wait for before pruning when handling ACS commitment
Create TABLE acs_no_wait_counter_participants
(
    synchronizer_id varchar collate "C" not null,
    participant_id varchar collate "C" not null,
    primary key(synchronizer_id,participant_id)
);

-- Stores configuration for metrics around slow participants
CREATE TABLE acs_slow_participant_config
(
   synchronizer_id varchar collate "C" not null,
   threshold_distinguished integer not null,
   threshold_default integer not null,
   primary key(synchronizer_id)
);

-- Stores distinguished or specifically measured counter participants for ACS commitment metrics
CREATE TABLE acs_slow_counter_participants
(
   synchronizer_id varchar collate "C" not null,
   participant_id varchar  collate "C" not null,
   is_distinguished boolean not null,
   is_added_to_metrics boolean not null,
   primary key(synchronizer_id,participant_id)
);
