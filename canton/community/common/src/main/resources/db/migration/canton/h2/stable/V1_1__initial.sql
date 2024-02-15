-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table daml_packages (package_id varchar(300) not null primary key, data binary large object not null, source_description varchar not null default 'default');

create table dars (
    hash_hex varchar(300) not null  primary key,
    hash binary large object not null,
    data binary large object not null,
    name varchar(300) not null
);

-- This table tracks the packages contained in the uploaded DARs
-- The table contains a (dar_hash_hex, package_id) pair iff:
--  * The dar_hash_hex is in the dars table
--  * The package_id is in the daml_packages table
--  * The corresponding DAR from the dars table contains the package with id package_id
create table dar_packages (
    dar_hash_hex varchar(300) not null,
    package_id varchar(300) not null,

    foreign key (dar_hash_hex) references dars(hash_hex) on delete cascade,
    foreign key (package_id) references daml_packages(package_id) on delete cascade,

    primary key (dar_hash_hex, package_id)
);

create table crypto_private_keys (
    -- fingerprint of the key
    key_id varchar(300) primary key,
    -- key purpose identifier
    purpose smallint not null,
    -- Protobuf serialized key including metadata
    data binary large object not null,
    -- optional name of the key
    name varchar(300),
    -- Add a new optional wrapper_key_id field to store the encryption key id for the encrypted private store
    wrapper_key_id varchar(300)
);

-- Store metadata information about KMS keys
CREATE TABLE kms_metadata_store (
    fingerprint varchar(300) not null,
    kms_key_id varchar(300) not null,
    purpose smallint not null,
    primary key (fingerprint)
);

create table crypto_public_keys (
    -- fingerprint of the key
    key_id varchar(300) primary key,
    -- key purpose identifier
    purpose smallint not null,
    -- Protobuf serialized key
    data binary large object not null,
    -- optional name of the key
    name varchar(300)
);



-- Stores the immutable contracts, however a creation of a contract can be rolled back.
create table contracts (
    -- As a participant can be connected to multiple domains, the transactions are stored under a domain id.
    domain_id integer not null,
    contract_id varchar(300) not null,
    -- The contract is serialized using the LF contract proto serializer.
    instance binary large object not null,
    contract_salt binary large object,
    -- Metadata: signatories, stakeholders, keys
    -- Stored as a Protobuf blob as H2 will only support typed arrays in 1.4.201
    -- TODO(#3256): change when H2 is upgraded
    metadata binary large object not null,
    -- The ledger time when the contract was created.
    ledger_create_time varchar(300) not null,
    -- The request counter of the request that created or divulged the contract
    request_counter bigint not null,
    -- The transaction that created the contract; null for divulged contracts
    creating_transaction_id binary large object,
    -- We store metadata of the contract instance for inspection
    package_id varchar(300) not null,
    template_id varchar not null,
    primary key (domain_id, contract_id));

-- Index to speedup ContractStore.find
-- domain_id comes first, because there is always a constraint on it.
-- package_id comes before template_id, because queries with package_id and without template_id make more sense than vice versa.
-- contract_id is left out, because a query with domain_id and contract_id can be served with the primary key.
create index idx_contracts_find on contracts(domain_id, package_id, template_id);

-- Index for pruning
-- Using an index on all elements because H2 does not support partial indices.
create index idx_contracts_request_counter on contracts(domain_id, request_counter);

-- Stores members registered with the sequencer for this domain.
-- This table in addition to the member_indexes table as members can be registered for a period without receiving any events.
create table sequencer_state_manager_members (
    member varchar(300) not null,
    -- Keep track of when the member was added so we can determine whether they're stale regardless of whether they've acknowledged anything
    -- UTC timestamp is stored in microseconds relative to EPOCH
    added_at bigint not null,
    enabled bool not null default true,
    latest_acknowledgement bigint,
    primary key (member)
);

-- Stores events for members that can be read with a sequencer subscription.
create table sequencer_state_manager_events (
    -- Registered member
    member varchar(300) not null,
    foreign key (member) references sequencer_state_manager_members (member),
    -- Counter of the event - the first event for a member will be given value 0
    -- Should be monotonically increasing value for a member however this is not enforced within the database and is left as a responsibility for the sequencer writer
    counter bigint not null check(counter >= 0),
    -- Should be an increasing value for a member however this is also not enforced within the database
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null,
    -- serialized signed serialized deliver event
    content binary large object not null,
    -- serialized trace context associated with the event
    trace_context binary large object not null,
    -- Both ensures members have only a single row for a counter value and provides an efficient index for reading based on member and key (subscriptions read events for a member between a counter range)
    primary key (member, counter)
);

-- Index to speed up fetching the latest timestamp for a store.
-- Is a little redundant to capture all timestamps in an index as we only need the latest one but doesn't seem to negatively
-- impact writes and significantly improves reading our head state that makes startup on a large events table almost
-- instant.
create index idx_sequencer_state_manager_events_ts on sequencer_state_manager_events (ts desc);

-- inclusive lower bound of when events can be read
-- if empty it means all events from epoch can be read
-- is updated when sequencer is pruned meaning that earlier events can no longer be read (and likely no longer exist)
create table sequencer_state_manager_lower_bound (
    single_row_lock char(1) not null default 'X' primary key check(single_row_lock = 'X'),
    ts bigint not null,
    -- track the age of the initial topology's timestamp or NULL if not available
    -- when available, the timestamp limits how long the sequencer creates tombstones.
    ts_initial_topology BIGINT NULL
);

-- provides a serial enumeration of static strings so we don't store the same string over and over in the db
-- currently only storing uids
create table static_strings (
    -- serial identifier of the string (local to this node)
    id serial not null primary key,
    -- the expression
    string varchar(300) not null,
    -- the source (what kind of string are we storing here)
    source int NOT NULL,
    unique(string, source)
);

-- Stores the identity of the node - its assigned member identity and its instance
-- This table should always have at most one entry which is a unique identifier for the member which consists of a string identifier and a fingerprint of a signing key
create table node_id(
  identifier varchar(300) not null,
  namespace varchar(300) not null,
  primary key (identifier, namespace)
);

-- Stores the local party metadata
create table party_metadata (
  -- party id as string
  party_id varchar(300) not null,
  -- the display name which should be exposed via the ledger-api server
  display_name varchar(300) null,
  -- the main participant id of this party which is our participant if the party is on our node (preferred) or the remote participant
  participant_id varchar(300) null,
  -- the submission id used to synchronise the ledger api server
  submission_id varchar(300) null,
  -- notification flag used to keep track about pending synchronisations
  notified boolean not null default false,
  -- the time when this change will be or became effective
  effective_at bigint not null,
  primary key (party_id)
);
create index idx_party_metadata_notified on party_metadata(notified);

-- Stores the dispatching watermarks
create table topology_dispatching (
  -- the target store we are dispatching to (from is always authorized)
  store_id varchar(300) not null primary key,
  -- the dispatching watermark
  watermark_ts bigint not null
);

-- change type: activation [create, transfer-in], deactivation [archive, transfer-out]
-- `deactivation` comes before `activation` so that comparisons `(timestamp, change) <= (bound, 'deactivation')`
-- select only deactivations if the timestamp matches the bound.
create type change_type as enum ('deactivation', 'activation');

-- The specific operation type that introduced a contract change.
create type operation_type as enum ('create', 'transfer-in', 'archive', 'transfer-out');

-- Maintains the status of contracts
create table active_contracts (
    -- As a participant can be connected to multiple domains, the active contracts are stored under a domain id.
    domain_id int not null,
    contract_id varchar(300) not null,
    change change_type not null,
    operation operation_type not null,
    -- UTC timestamp of the time of change in microsecond precision relative to EPOCH
    ts bigint not null,
    -- Request counter of the time of change
    request_counter bigint not null,
    -- optional remote domain id in case of transfers
    remote_domain_id int,
    transfer_counter bigint default null,
    primary key (domain_id, contract_id, ts, request_counter, change)
);

CREATE index active_contracts_dirty_request_reset_idx ON active_contracts (domain_id, request_counter);

CREATE index active_contracts_contract_id_idx ON active_contracts (contract_id);

CREATE index active_contracts_ts_domain_id_idx ON active_contracts (ts, domain_id);

create table response_aggregations (
  -- identified by the sequencer timestamp (UTC timestamp in microseconds relative to EPOCH)
  request_id bigint not null primary key,
  mediator_request binary large object not null,
  -- UTC timestamp is stored in microseconds relative to EPOCH
  version bigint not null,
  verdict binary large object not null,
  request_trace_context binary large object not null
);

-- Stores the received sequencer messages
create table sequenced_events (
    -- discriminate between different users of the sequenced events tables
    client integer not null,
    -- Proto serialized signed message
    sequenced_event binary large object not null,
    -- Explicit fields to query the messages, which are stored as blobs
    type varchar(3) not null check(type IN ('del', 'err', 'ign')),
    -- Timestamp of the time of change in microsecond precision relative to EPOCH
    ts bigint not null,
    -- Sequencer counter of the time of change
    sequencer_counter bigint not null,
    -- serialized trace context associated with the event
    trace_context binary large object not null,
    -- flag to skip problematic events
    ignore boolean not null,
    -- The sequencer ensures that the timestamp is unique
    primary key (client, ts)
);

create unique index idx_sequenced_events_sequencer_counter on sequenced_events(client, sequencer_counter);

-- Track what send requests we've made but have yet to observe being sequenced.
-- If events are not observed by the max sequencing time we know that the send will never be processed.
create table sequencer_client_pending_sends (
    -- ids for distinguishing between different sequencer clients in the same node
    client integer not null,

    -- the message id of the send being tracked (expected to be unique for the sequencer client while the send is in flight)
    message_id varchar(300) not null,

    -- the message id should be unique for the sequencer client
    primary key (client, message_id),

    -- the max sequencing time of the send request (UTC timestamp in microseconds relative to EPOCH)
    max_sequencing_time bigint not null
);

create table participant_domain_connection_configs(
    domain_alias varchar(300) not null primary key,
    config binary large object, -- the protobuf-serialized versioned domain connection config
    status char(1) default 'A' not null
);

-- used to register all domains that a participant connects to
create table participant_domains(
    -- to keep track of the order domains were registered
    order_number serial not null primary key,
    -- domain human readable alias
    alias varchar(300) not null unique,
    -- domain node id
    domain_id varchar(300) not null unique,
    status char(1) default 'A' not null,
    CONSTRAINT participant_domains_unique unique (alias, domain_id)
);

create table event_log (
    -- Unique log_id for each instance of the event log.
    -- If positive, it is an index referring to a domain id stored in static_strings.
    -- If zero, it refers to the production participant event log.
    -- If negative, it refers to a participant event log used in tests only.
    log_id int not null,
    local_offset_tie_breaker bigint not null,
    -- timestamp, micros from epoch
    local_offset_effective_time bigint NOT NULL DEFAULT 0,
    -- 0 for requests, 1 for topology events
    local_offset_discriminator smallint NOT NULL DEFAULT 0,
    primary key (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker),
    -- UTC timestamp of the event in microseconds relative to EPOCH
    ts bigint not null,
    -- sequencer counter corresponding to the underlying request, if there is one
    request_sequencer_counter bigint,
    -- Optional event ID:
    -- If the event is an Update.TransactionAccepted, this is the transaction id prefixed by `T`, to enable efficient lookup
    -- of the domain that the transaction was executed on.
    -- For timely-rejected transaction (not an Update.TransactionAccepted),
    -- this is the message UUID of the SubmissionRequest, prefixed by `M`, the domain ID, and the separator `#`.
    -- NULL if the event is neither an Update.TransactionAccepted nor was created as part of a timely rejection.
    event_id varchar(300),
    -- Optional domain ID:
    -- For timely-rejected transactions (not an Update.TransactionAccepted) in the participant event log,
    -- this is the domain ID to which the transaction was supposed to be submitted.
    -- NULL if this is not a timely rejection in the participant event log.
    associated_domain integer,
    -- LedgerSyncEvent serialized using protobuf
    content binary large object not null,
    -- TraceContext is serialized using protobuf
    trace_context binary large object not null

);
-- Not strictly required, but sometimes useful.
create index idx_event_log_timestamp on event_log (log_id, ts);
create unique index idx_event_log_event_id on event_log (event_id);
CREATE INDEX idx_event_log_local_offset ON event_log (local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker);

-- Persist a single, linearized, multi-domain event log for the local participant
create table linearized_event_log (
    -- Global offset
    global_offset bigserial not null primary key,
    -- Corresponds to an entry in the event_log table.
    log_id int not null,
    -- Offset in the event log instance designated by log_id
    local_offset_tie_breaker bigint not null,
    -- The participant's local time when the event was published, in microseconds relative to EPOCH.
    -- Increases monotonically with the global offset
    publication_time bigint not null,
    local_offset_effective_time bigint NOT NULL DEFAULT 0, -- timestamp, micros from epoch
    local_offset_discriminator smallint NOT NULL DEFAULT 0, -- 0 for requests, 1 for topology events
    constraint foreign_key_event_log foreign key (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker) references event_log(log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker) on delete cascade
);
create unique index idx_linearized_event_log_offset ON linearized_event_log (log_id, local_offset_effective_time, local_offset_discriminator, local_offset_tie_breaker);
create index idx_linearized_event_log_publication_time on linearized_event_log (publication_time, global_offset);

-- H2 does not (yet) support partial indices, so we index everything
create index idx_event_log_associated_domain on event_log (log_id, associated_domain, ts);

create table transfers (
    -- transfer id
    target_domain varchar(300) not null,
    origin_domain varchar(300) not null,

    -- transfer data
    source_protocol_version integer not null,
    -- UTC timestamp in microseconds relative to EPOCH
    transfer_out_timestamp bigint not null,
    transfer_out_request_counter bigint not null,
    transfer_out_request binary large object not null,
    transfer_out_global_offset bigint,
    transfer_in_global_offset bigint,
    -- UTC timestamp in microseconds relative to EPOCH
    transfer_out_decision_time bigint not null,
    contract binary large object not null,
    creating_transaction_id binary large object not null,
    transfer_out_result binary large object,
    submitter_lf varchar(300) not null,

    -- defined if transfer was completed
    time_of_completion_request_counter bigint,
    -- UTC timestamp in microseconds relative to EPOCH
    time_of_completion_timestamp bigint,
    primary key (target_domain, origin_domain, transfer_out_timestamp)
);

-- stores all requests for the request journal
create table journal_requests (
    domain_id integer not null,
    request_counter bigint not null,
    request_state_index smallint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    request_timestamp bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    -- is set only if the request is clean
    commit_time bigint,
    repair_context varchar(300), -- only set on manual repair requests outside of sync protocol
    primary key (domain_id, request_counter));
create index idx_journal_request_timestamp on journal_requests (domain_id, request_timestamp);
create index idx_journal_request_commit_time on journal_requests (domain_id, commit_time);

-- the last recorded head clean counter for each domain
create table head_clean_counters (
    client integer not null primary key,
    prehead_counter bigint not null, -- request counter of the prehead request
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null
);

-- locally computed ACS commitments to a specific period, counter-participant and domain
create table computed_acs_commitments (
    domain_id int not null,
    counter_participant varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    -- the "raw" cryptographic commitment (AcsCommitment.CommitmentType) in its serialized format
    commitment binary large object not null,
    primary key (domain_id, counter_participant, from_exclusive, to_inclusive),
    constraint check_nonempty_interval_computed check(to_inclusive > from_exclusive)
);

-- ACS commitments received from counter-participants
create table received_acs_commitments (
    domain_id int not null,
    -- the counter-participant who sent the commitment
    sender varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    -- the received signed commitment message (including all the other fields) in a serialized form
    signed_commitment binary large object not null,
    constraint check_to_after_from check(to_inclusive > from_exclusive)
);

create index full_commitment_idx on received_acs_commitments (domain_id, sender, from_exclusive, to_inclusive);

-- the participants whose remote commitments are outstanding
create table outstanding_acs_commitments (
    domain_id int not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    counter_participant varchar not null,
    constraint check_nonempty_interval_outstanding check(to_inclusive > from_exclusive)
);

create unique index unique_interval_participant on outstanding_acs_commitments(domain_id, counter_participant, from_exclusive, to_inclusive);

create index outstanding_acs_commitments_by_time on outstanding_acs_commitments (domain_id, from_exclusive);

-- the last timestamp for which the commitments have been computed, stored and sent
create table last_computed_acs_commitments (
    domain_id int primary key,
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null
);

-- Stores the snapshot ACS commitments (per stakeholder set)
create table commitment_snapshot (
    domain_id int not null,
    -- A stable reference to a stakeholder set, that doesn't rely on the Protobuf encoding being deterministic
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    stakeholders_hash varchar(300) not null,
    stakeholders binary large object not null,
    commitment binary large object not null,
    primary key (domain_id, stakeholders_hash)
);

-- Stores the time (along with a tie-breaker) of the ACS commitment snapshot
create table commitment_snapshot_time (
    domain_id int not null,
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null,
    tie_breaker bigint not null,
    primary key (domain_id)
);

-- Remote commitments that were received but could not yet be checked because the local participant is lagging behind
create table commitment_queue (
    domain_id int not null,
    sender varchar(300) not null,
    counter_participant varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    from_exclusive bigint not null,
    -- UTC timestamp in microseconds relative to EPOCH
    to_inclusive bigint not null,
    commitment binary large object not null,
    commitment_hash varchar(300) not null, -- A shorter hash (SHA-256) of the commitment for the primary key instead of the binary large object
    constraint check_nonempty_interval_queue check(to_inclusive > from_exclusive),
    primary key (domain_id, sender, counter_participant, from_exclusive, to_inclusive, commitment_hash)
);

create index commitment_queue_by_time on commitment_queue (domain_id, to_inclusive);

-- the (current) domain parameters for the given domain
create table static_domain_parameters (
    domain_id varchar(300) primary key,
    -- serialized form
    params binary large object not null
);

-- Data about the last pruning operations
create table pruning_operation (
    -- dummy field to enforce a single row
    name varchar(40) not null primary key,
    started_up_to_inclusive bigint,
    completed_up_to_inclusive bigint
);

-- this table is meant to be used by blockchain based external sequencers
-- in order to keep track of the blocks processed associating block heights
-- with the timestamp of the last event in it
create table sequencer_block_height (
    height bigint primary key check(height >= -1),
    latest_event_ts bigint not null,
    -- The column latest_topology_client_ts denotes the sequencing timestamp of an event
    -- addressed to the sequencer's topology client such that
    -- there is no update to the domain topology state (by sequencing time) between this timestamp
    -- and the last event in the block.
    -- NULL if no such timestamp is known, e.g., because this block was added before this column was added.
    latest_topology_client_ts bigint
);

create table sequencer_initial_state (
    member varchar(300) primary key,
    counter bigint not null
);

create table mediator_deduplication_store (
    mediator_id varchar(300) not null,
    uuid varchar(36) not null,
    request_time bigint not null,
    expire_after bigint not null
);
create index idx_mediator_deduplication_store_expire_after on mediator_deduplication_store(expire_after, mediator_id);

create type pruning_phase as enum ('started', 'completed');

-- Maintains the latest timestamp (by domain) for which ACS pruning has started or finished
create table active_contract_pruning (
  domain_id integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (domain_id)
);

-- Maintains the latest timestamp (by domain) for which ACS commitment pruning has started or finished
create table commitment_pruning (
  domain_id integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (domain_id)
);

-- Maintains the latest timestamp (by domain) for which contract key journal pruning has started or finished
create table contract_key_pruning (
  domain_id integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (domain_id)
);

-- Maintains the latest timestamp (by sequencer client) for which the sequenced event store pruning has started or finished
create table sequenced_event_store_pruning (
  client integer not null,
  phase pruning_phase not null,
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null,
  succeeded bigint null,
  primary key (client)
);

-- table to contain the values provided by the domain to the mediator node for initialization.
-- we persist these values to ensure that the mediator can always initialize itself with these values
-- even if it was to crash during initialization.
create table mediator_domain_configuration (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  initial_key_context varchar(300) not null,
  domain_id varchar(300) not null,
  static_domain_parameters binary large object not null,
  sequencer_connection binary large object not null
);

-- the last recorded head clean sequencer counter for each domain
create table head_sequencer_counters (
  -- discriminate between different users of the sequencer counter tracker tables
  client integer not null primary key,
  prehead_counter bigint not null, -- sequencer counter before the first unclean sequenced event
  -- UTC timestamp in microseconds relative to EPOCH
  ts bigint not null
);


-- we create a unique integer id for each member that is more efficient to use in the events table
-- members can read all events from `registered_ts`
create table sequencer_members (
    member varchar(300) primary key,
    id serial unique,
    registered_ts bigint not null,
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
    instance_discriminator varchar(36) not null,
    content binary large object not null
);

-- sequencer instances maintain the lowest timestamp for which it is safe to read from
create table sequencer_watermarks (
    node_index integer primary key,
    watermark_ts bigint not null,
    sequencer_online bool not null
);

-- readers periodically write checkpoints that maps the calculated timer to a timestamp/event-id.
-- when a new subscription is requested from a counter the sequencer can use these checkpoints to find the closest
-- timestamp below the given counter to start the subscription from.
create table sequencer_counter_checkpoints (
   member integer not null,
   counter bigint not null,
   ts bigint not null,
   latest_topology_client_ts bigint,
   primary key (member, counter)
);

-- record the latest acknowledgement sent by a sequencer client of a member for the latest event they have successfully
-- processed and will not re-read.
create table sequencer_acknowledgements (
    member integer primary key,
    ts bigint not null
);

-- inclusive lower bound of when events can be read
-- if empty it means all events from epoch can be read
-- is updated when sequencer is pruned meaning that earlier events can no longer be read (and likely no longer exist)
create table sequencer_lower_bound (
    single_row_lock char(1) not null default 'X' primary key check(single_row_lock = 'X'),
    ts bigint not null
);

-- h2 events table (differs from postgres in the recipients array definition)
create table sequencer_events (
    ts bigint primary key,
    node_index smallint not null,
    -- single char to indicate the event type: D for deliver event, E for deliver error
    event_type char(1) not null
        constraint event_type_enum check (event_type = 'D' or event_type = 'E'),
    message_id varchar null,
    sender integer null,
    -- null if event goes to everyone, otherwise specify member ids of recipients
    recipients varchar array null,
    -- null if the event is a deliver error
    -- intentionally not creating a foreign key here for performance reasons
    payload_id bigint null,
    -- optional topology timestamp for deliver events
    topology_timestamp bigint null,
    -- optional error message for deliver error
    error binary large object,
    -- trace context associated with the event
    trace_context binary large object not null
);

-- Sequence of local offsets used by the participant event publisher
create sequence participant_event_publisher_local_offsets minvalue 0 start with 0;


-- store nonces that have been requested for authentication challenges
create table sequencer_authentication_nonces (
     nonce varchar(300) primary key,
     member varchar(300) not null,
     generated_at_ts bigint not null,
     expire_at_ts bigint not null
);

create index idx_nonces_for_member on sequencer_authentication_nonces (member, nonce);

-- store tokens that have been generated for successful authentication requests
create table sequencer_authentication_tokens (
     token varchar(300) primary key,
     member varchar(300) not null,
     expire_at_ts bigint not null
);

create index idx_tokens_for_member on sequencer_authentication_tokens (member);

-- store in-flight submissions
create table in_flight_submission (
    -- hash of the change ID as a hex string
    change_id_hash varchar(300) primary key,

    submission_id varchar(300) null,

    submission_domain varchar(300) not null,
    message_id varchar(300) not null,

    -- Sequencer timestamp after which this submission will not be sequenced any more, in microsecond precision relative to EPOCH
    -- If set, this submission is considered unsequenced.
    sequencing_timeout bigint,
    -- Sequencer counter assigned to this submission.
    -- Must be set iff sequencing_timeout is not set.
    sequencer_counter bigint,
    -- Sequencer timestamp assigned to this submission, in microsecond precision relative to EPOCH
    -- Must be set iff sequencing_timeout is not set.
    sequencing_time bigint,

    -- Tracking data for producing a rejection event.
    -- Optional; omitted if other code paths ensure that an event is produced
    -- Must be null if sequencing_timeout is not set.
    tracking_data binary large object,
    root_hash_hex varchar(300) DEFAULT NULL,
    trace_context binary large object not null
);
CREATE INDEX idx_in_flight_submission_root_hash ON in_flight_submission (root_hash_hex);
create index idx_in_flight_submission_timeout on in_flight_submission (submission_domain, sequencing_timeout);
create index idx_in_flight_submission_sequencing on in_flight_submission (submission_domain, sequencing_time);
create index idx_in_flight_submission_message_id on in_flight_submission (submission_domain, message_id);

create table participant_settings(
  client integer primary key, -- dummy field to enforce at most one row
  max_dirty_requests integer,
  max_rate integer,
  max_deduplication_duration binary large object, -- non-negative finite duration
  max_burst_factor double precision not null default 0.5
);

create table command_deduplication (
  -- hash of the change ID (application_id + command_id + act_as) as a hex string
  change_id_hash varchar(300) primary key,

  -- the application ID that requested the change
  application_id varchar(300) not null,
  -- the command ID
  command_id varchar(300) not null,
  -- the act as parties serialized as a Protobuf blob
  act_as binary large object not null,

  -- the highest offset in the MultiDomainEventLog that yields a completion event with a definite answer for the change ID hash
  offset_definite_answer bigint not null,
  -- the publication time of the offset_definite_answer, measured in microseconds relative to EPOCH
  publication_time_definite_answer bigint not null,
  submission_id_definite_answer varchar(300), -- always optional
  trace_context_definite_answer binary large object not null,

  -- the highest offset in the MultiDomainEventLog that yields an accepting completion event for the change ID hash
  -- always at most the offset_definite_answer
  -- null if there have only been rejections since the last pruning
  offset_acceptance bigint,
  -- the publication time of the offset_acceptance, measured in microseconds relative to EPOCH
  publication_time_acceptance bigint,
  submission_id_acceptance varchar(300),
  trace_context_acceptance binary large object
);
create index idx_command_dedup_offset on command_deduplication(offset_definite_answer);

create table command_deduplication_pruning (
  client integer primary key, -- dummy field to enforce at most one row

  -- The highest offset pruning has been started at.
  pruning_offset varchar not null,
  -- An upper bound to publication times of pruned offsets.
  publication_time bigint not null
);

-- table to contain the values provided by the domain to the sequencer node for initialization.
-- we persist these values to ensure that the sequencer can always initialize itself with these values
-- even if it was to crash during initialization.
create table sequencer_domain_configuration (
  -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
  lock char(1) not null default 'X' primary key check (lock = 'X'),
  domain_id varchar(300) not null,
  static_domain_parameters binary large object not null
);

CREATE TABLE pruning_schedules(
    -- node_type is one of "PAR", "MED", or "SEQ"
    -- since mediator and sequencer sometimes share the same db
    node_type varchar(3) not null primary key,
    cron varchar(300) not null,
    max_duration bigint not null, -- positive number of seconds
    retention bigint not null -- positive number of seconds
);

-- Tables for new submission tracker
CREATE TABLE fresh_submitted_transaction (
    domain_id integer not null,
    root_hash_hex varchar(300) not null,
    request_id bigint not null,
    max_sequencing_time bigint not null,
    primary key (domain_id, root_hash_hex)
);

CREATE TABLE fresh_submitted_transaction_pruning (
    domain_id integer not null,
    phase pruning_phase not null,
    -- UTC timestamp in microseconds relative to EPOCH
    ts bigint not null,
    succeeded bigint null,
    primary key (domain_id)
);

-- participant_pruning_schedules with pruning flag specific to participant pruning
CREATE TABLE participant_pruning_schedules (
    -- this lock column ensures that there can only ever be a single row: https://stackoverflow.com/questions/3967372/sql-server-how-to-constrain-a-table-to-contain-a-single-row
    lock char(1) not null default 'X' primary key check (lock = 'X'),
    cron varchar(300) not null,
    max_duration bigint not null, -- positive number of seconds
    retention bigint not null, -- positive number of seconds
    prune_internally_only boolean NOT NULL DEFAULT false -- whether to prune only canton-internal stores not visible to ledger api
);


-- TODO(#15155) Move this to stable when releasing BFT: BEGIN
CREATE TABLE in_flight_aggregation(
                                      aggregation_id varchar(300) not null primary key,
    -- UTC timestamp in microseconds relative to EPOCH
                                      max_sequencing_time bigint not null,
    -- serialized aggregation rule,
                                      aggregation_rule binary large object not null
);

CREATE INDEX in_flight_aggregation_max_sequencing_time on in_flight_aggregation(max_sequencing_time);

CREATE TABLE in_flight_aggregated_sender(
    aggregation_id varchar(300) not null,
    sender varchar(300) not null,
    -- UTC timestamp in microseconds relative to EPOCH
    sequencing_timestamp bigint not null,
    signatures binary large object not null,
    primary key (aggregation_id, sender),
    constraint foreign_key_in_flight_aggregated_sender foreign key (aggregation_id) references in_flight_aggregation(aggregation_id) on delete cascade
);

-- stores the topology-x state transactions
CREATE TABLE topology_transactions_x (
    -- serial identifier used to preserve insertion order
    id bigserial not null primary key,
    -- the id of the store
    store_id varchar(300) not null,
    -- the timestamp at which the transaction is sequenced by the sequencer
    -- UTC timestamp in microseconds relative to EPOCH
    sequenced bigint not null,
    -- type of transaction (refer to TopologyMappingX.Code)
    transaction_type int not null,
    -- the namespace this transaction is operating on
    namespace varchar(300) not null,
    -- the optional identifier this transaction is operating on (yields a uid together with namespace)
    -- a null value is represented as "", as null is never equal in indexes for postgres, which would
    -- break the unique index
    identifier varchar(300) not null,
    -- The topology mapping key hash, to uniquify and aid efficient lookups.
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    mapping_key_hash varchar(300) not null,
    -- the serial_counter describes the change order within transactions of the same mapping_key_hash
    -- (redundant also embedded in instance)
    serial_counter int not null,
    -- validity window, UTC timestamp in microseconds relative to EPOCH
    -- so `TopologyChangeOpX.Replace` transactions have an effect for valid_from < t <= valid_until
    -- a `TopologyChangeOpX.Remove` will have valid_from = valid_until
    valid_from bigint not null,
    valid_until bigint null,
    -- operation
    -- 1: Remove
    -- 2: Replace (upsert/merge semantics)
    operation int not null,
    -- The raw transaction, serialized using the proto serializer.
    instance binary large object not null,
    -- The transaction hash, to uniquify and aid efficient lookups.
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    tx_hash varchar(300) not null,
    -- flag / reason why this transaction is being rejected
    -- therefore: if this field is NULL, then the transaction is included. if it is non-null shows the reason why it is invalid
    rejection_reason varchar(300) null,
    -- is_proposal indicates whether the transaction still needs BFT-consensus
    -- false if and only if the transaction is accepted
    -- (redundant also embedded in instance)
    is_proposal boolean not null,
    representative_protocol_version integer not null,
    -- the hash of the transaction's signatures. this disambiguates multiple transactions/proposals with the same
    -- tx_hash but different signatures
    hash_of_signatures varchar(300) not null,
    -- index used for idempotency during crash recovery
    -- TODO(#12390) should mapping_key_hash rather be tx_hash?
    unique (store_id, mapping_key_hash, serial_counter, valid_from, operation, representative_protocol_version, hash_of_signatures)
);
CREATE INDEX topology_transactions_x_idx ON topology_transactions_x (store_id, transaction_type, namespace, identifier, valid_until, valid_from);
-- TODO(#14061): Decide whether we want additional indices by mapping_key_hash and tx_hash (e.g. for update/removal and lookups)
-- TODO(#14061): Come up with columns/indexing for efficient ParticipantId => Seq[PartyId] lookup

-- update the sequencer_state_manager_events to store traffic information per event
-- this will be needed to re-hydrate the sequencer from a specific point in time deterministically
-- adds extra traffic remainder at the time of the event
alter table sequencer_state_manager_events
    add column extra_traffic_remainder bigint;
-- adds total extra traffic consumed at the time of the event
alter table sequencer_state_manager_events
    add column extra_traffic_consumed bigint;
-- adds base traffic remainder at the time of the event
alter table sequencer_state_manager_events
    add column base_traffic_remainder bigint;

-- adds extra traffic remainder per event in sequenced event store
-- this way the participant can replay event and reconstruct the correct traffic state
-- adds extra traffic remainder at the time of the event
alter table sequenced_events
    add column extra_traffic_remainder bigint;
-- adds total extra traffic consumed at the time of the event
alter table sequenced_events
    add column extra_traffic_consumed bigint;

-- adds initial traffic info per member for when a sequencer gets onboarded
-- initial extra traffic remainder
alter table sequencer_initial_state
    add column extra_traffic_remainder bigint;
-- initial total extra traffic consumed
alter table sequencer_initial_state
    add column extra_traffic_consumed bigint;
-- initial base traffic remainder
alter table sequencer_initial_state
    add column base_traffic_remainder bigint;
-- timestamp of the initial traffic state
alter table sequencer_initial_state
    add column sequenced_timestamp bigint;

-- Store the top up events per member as they get sequenced in the topology state
-- Allows to efficiently query top ups without replaying the topology state
create table top_up_events (
    -- member the traffic limit belongs to
                               member varchar(300) not null,
    -- timestamp at which the limit is effective
                               effective_timestamp bigint not null,
    -- the total traffic limit at that time
                               extra_traffic_limit bigint not null,
    -- serial number of the topology transaction effecting the top up
    -- used to disambiguate between top ups with the same effective timestamp
                               serial bigint not null,
    -- top ups should have unique serial per member
                               primary key (member, serial)
);

create index top_up_events_idx ON top_up_events (member);

--   BFT Ordering Tables

-- Stores metadata for epochs completed in entirety
-- Individual blocks/transactions exist in separate table
create table completed_epochs (
    -- strictly-increasing, contiguous epoch number
    epoch_number bigint not null primary key ,
    -- first block sequence number (globally) of the epoch
    start_block_number bigint not null,
    -- number of total blocks in the epoch
    epoch_length integer not null
);

-- Stores consensus state for active epoch
create table active_epoch (
    -- epoch number that consensus is actively working on
    epoch_number bigint not null,
    -- global sequence number of the ordered block
    block_number bigint not null primary key
);

create table availability_batch(
    id varchar(300) not null,
    batch binary large object not null,
    primary key (id)
);
