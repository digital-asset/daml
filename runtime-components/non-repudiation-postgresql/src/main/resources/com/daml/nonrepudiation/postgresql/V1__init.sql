-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

create table ${tables.prefix}_signed_payloads(
    command_id text,
    algorithm text not null,
    fingerprint bytea not null,
    payload bytea not null,
    signature bytea not null,
    "timestamp" timestamptz not null,
);

create index ${tables.prefix}_signed_payloads_command_id_index on ${tables.prefix}_signed_payloads using hash(command_id);

create table ${tables.prefix}_certificates(
    fingerprint bytea not null,
    certificate bytea not null
);

create index ${tables.prefix}_certificates_fingerprint_index on ${tables.prefix}_keys using hash(fingerprint);
