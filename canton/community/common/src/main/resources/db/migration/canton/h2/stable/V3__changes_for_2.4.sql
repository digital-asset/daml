-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE transfers ADD source_protocol_version integer DEFAULT 2 NOT NULL;

-- The column latest_topology_client_ts denotes the sequencing timestamp of an event
-- addressed to the sequencer's topology client such that
-- there is no update to the domain topology state (by sequencing time) between this timestamp
-- and the last event in the block.
-- NULL if no such timestamp is known, e.g., because this block was added before this column was added.
alter table sequencer_block_height
    add column latest_topology_client_ts bigint;

create table mediator_deduplication_store (
  mediator_id varchar(300) not null,
  uuid varchar(36) not null,
  request_time bigint not null,
  expire_after bigint not null
);

create index idx_mediator_deduplication_store_expire_after on mediator_deduplication_store(expire_after, mediator_id);

-- On H2 without data continuity recreate the topology transactions table
drop table if exists topology_transactions;
create table topology_transactions (
    -- serial identifier used to preserve insertion order
    id serial not null primary key,
    -- the id of the store
    store_id varchar(300) not null,
    -- type of transaction (refer to DomainTopologyTransaction companion object)
    transaction_type int not null,
    -- the namespace this transaction is operating on
    namespace varchar(300) not null,
    -- the optional identifier this transaction is operating on (yields a uid together with namespace)
    -- a null value is represented as "", as null is never equal in indexes for postgres, which would
    -- break the unique index
    identifier varchar(300) not null,
    -- the optional element-id of this transaction (signed topology transactions have one)
    -- same not null logic as for identifier
    element_id varchar(300) not null,
    -- the optional secondary uid (only used by party to participant mappings to compute cascading updates)
    secondary_namespace varchar(300) null,
    secondary_identifier varchar(300) null,
    -- validity window, UTC timestamp in microseconds relative to EPOCH
    -- so Add transactions have an effect for valid_from < t <= valid_until
    -- a remove will have valid_from = valid_until
    valid_from bigint not null,
    valid_until bigint null,
    -- operation
    -- 1: Add
    -- 2: Remove
    operation int not null,
    -- The raw transaction, serialized using the proto serializer.
    instance binary large object not null,
    -- flag / reason why this transaction is being ignored
    -- therefore: if this field is NULL, then the transaction is included. if it is non-null, the reason why it is invalid is included
    ignore_reason varchar null,
    sequenced bigint null,
    -- the representative protocol version that was used to serialize the topology transaction
    representative_protocol_version integer not null,
    -- index used for idempotency during crash recovery
    unique (store_id, transaction_type, namespace, identifier, element_id, valid_from, operation, representative_protocol_version)
);

-- Add a new optional wrapper_key_id field to store the encryption key id for the encrypted private store
alter table crypto_private_keys add wrapper_key_id varchar(300);