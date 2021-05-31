-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table ${tables.prefix}_signed_payloads(
    command_id varchar(255),
    algorithm varchar(64) not null,
    fingerprint bytea not null,
    payload bytea not null,
    signature bytea not null,
    "timestamp" timestamptz not null
);

-- Supports fast retrieval of signed payloads by command identifier
create index ${tables.prefix}_signed_payloads_command_id_index on ${tables.prefix}_signed_payloads using hash(command_id);

create table ${tables.prefix}_certificates(
    fingerprint bytea primary key not null,
    certificate bytea not null
);
