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
  mediator_id varchar(300) collate "C" not null,
  uuid varchar(36) collate "C" not null,
  request_time bigint not null,
  expire_after bigint not null
);

create index idx_mediator_deduplication_store_expire_after on mediator_deduplication_store(expire_after, mediator_id);

-- Previous topology transactions belong to PV=2
alter table topology_transactions add representative_protocol_version integer default 2 not null;

-- Drop the existing unnamed unique constraint (conventional constraint name truncated to 63 bytes)
alter table topology_transactions drop constraint topology_transactions_store_id_transaction_type_namespace_i_key;

-- Include the protocol version as part of the unique constraint
alter table topology_transactions add constraint unique_topology_transactions
 unique (store_id, transaction_type, namespace, identifier, element_id, valid_from, operation, representative_protocol_version);

-- Add a new optional wrapper_key_id field to store the encryption key id for the encrypted private store
alter table crypto_private_keys add wrapper_key_id varchar(300) collate "C";