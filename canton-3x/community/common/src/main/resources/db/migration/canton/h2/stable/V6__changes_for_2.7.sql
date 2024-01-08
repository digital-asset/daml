-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE event_log DROP COLUMN causality_update;
DROP TABLE per_party_causal_dependencies;

ALTER TABLE transfers DROP PRIMARY KEY;
ALTER TABLE transfers DROP COLUMN request_timestamp;
ALTER TABLE transfers ADD PRIMARY KEY (target_domain, origin_domain, transfer_out_timestamp);
ALTER TABLE transfers ADD COLUMN transfer_out_global_offset bigint;
ALTER TABLE transfers ADD COLUMN transfer_in_global_offset bigint;

ALTER TABLE active_contracts ADD COLUMN transfer_counter bigint default null;

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
    primary key (domain_id)
);

-- Add root hash to in-flight submission tracker store
ALTER TABLE in_flight_submission ADD COLUMN root_hash_hex varchar(300) DEFAULT NULL;

CREATE INDEX idx_in_flight_submission_root_hash ON in_flight_submission (root_hash_hex);

-- Store metadata information about KMS keys
CREATE TABLE kms_metadata_store (
    fingerprint varchar(300) not null,
    kms_key_id varchar(300) not null,
    purpose smallint not null,
    primary key (fingerprint)
);
