-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- This is a dummy column we are adding in order to test that adding dev version migrations
-- works properly. DO NOT MOVE THIS TO STABLE
ALTER TABLE node_id ADD COLUMN test_column INT NOT NULL DEFAULT 0;

-- TODO(#15155) Move this to stable when releasing BFT: BEGIN
CREATE TABLE in_flight_aggregation(
    aggregation_id varchar(300) collate "C" not null primary key,
    -- UTC timestamp in microseconds relative to EPOCH
    max_sequencing_time bigint not null,
    -- serialized aggregation rule,
    aggregation_rule bytea not null
);

CREATE INDEX in_flight_aggregation_max_sequencing_time on in_flight_aggregation(max_sequencing_time);

CREATE TABLE in_flight_aggregated_sender(
    aggregation_id varchar(300) collate "C" not null,
    sender varchar(300) collate "C" not null,
    -- UTC timestamp in microseconds relative to EPOCH
    sequencing_timestamp bigint not null,
    signatures bytea not null,
    primary key (aggregation_id, sender),
    constraint foreign_key_in_flight_aggregated_sender foreign key (aggregation_id) references in_flight_aggregation(aggregation_id) on delete cascade
);

-- stores the topology-x state transactions
CREATE TABLE topology_transactions_x (
    -- serial identifier used to preserve insertion order
    id bigserial not null primary key,
    -- the id of the store
    store_id varchar(300) collate "C" not null,
    -- the timestamp at which the transaction is sequenced by the sequencer
    -- UTC timestamp in microseconds relative to EPOCH
    sequenced bigint not null,
    -- type of transaction (refer to TopologyMappingX.Code)
    transaction_type int not null,
    -- the namespace this transaction is operating on
    namespace varchar(300) collate "C" not null,
    -- the optional identifier this transaction is operating on (yields a uid together with namespace)
    -- a null value is represented as "", as null is never equal in indexes for postgres, which would
    -- break the unique index
    identifier varchar(300) collate "C" not null,
    -- The topology mapping key hash, to uniquify and aid efficient lookups.
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    mapping_key_hash varchar(300) collate "C" not null,
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
    instance bytea not null,
    -- The transaction hash, to uniquify and aid efficient lookups.
    -- a hex-encoded hash (not binary so that hash can be indexed in all db server types)
    tx_hash varchar(300) collate "C" not null,
    -- flag / reason why this transaction is being rejected
    -- therefore: if this field is NULL, then the transaction is included. if it is non-null shows the reason why it is invalid
    rejection_reason varchar(300) collate "C" null,
    -- is_proposal indicates whether the transaction still needs BFT-consensus
    -- false if and only if the transaction is accepted
    -- (redundant also embedded in instance)
    is_proposal boolean not null,
    representative_protocol_version integer not null,
    -- the hash of the transaction's signatures. this disambiguates multiple transactions/proposals with the same
    -- tx_hash but different signatures
    hash_of_signatures varchar(300) collate "C" not null,
    -- index used for idempotency during crash recovery
    -- TODO(#12390) should mapping_key_hash rather be tx_hash?
    unique (store_id, mapping_key_hash, serial_counter, valid_from, operation, representative_protocol_version, hash_of_signatures)
);
CREATE INDEX topology_transactions_x_idx ON topology_transactions_x (store_id, transaction_type, namespace, identifier, valid_until, valid_from);
-- TODO(#14061): Decide whether we want additional indices by mapping_key_hash and tx_hash (e.g. for update/removal and lookups)
-- TODO(#14061): Come up with columns/indexing for efficient ParticipantId => Seq[PartyId] lookup


-- TODO(#15155) Move this to stable when releasing BFT: END

-- TODO(#13104) Move traffic control to stable release: BEGIN
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
    member varchar(300) collate "C" not null,
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
-- TODO(#13104) Move traffic control to stable release: END

-- BFT Ordering tables
create table epochs (
  epoch_number bigint not null primary key,
  last_block_number bigint not null,
  constraint unique_epoch unique (epoch_number, last_block_number)
);
