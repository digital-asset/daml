-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- IMPORTANT: We can use and change this V1 schema definition script during the implementation of Postgres persistence.
--            As soon we released it, every schema change must be a separate file, so future migrations can work from any
--            released Sandbox version.

CREATE TABLE ledger_entries
(
  ledger_offset         bigint primary key           not null,
  -- one of 'transaction', 'rejection', or 'checkpoint' -- also see check_entry below
  typ                   varchar                      not null,
  transaction_id        varchar unique,
  command_id            varchar,
  application_id        varchar,
  submitter             varchar,
  workflow_id           varchar,
  effective_at          timestamptz,
  recorded_at           timestamptz                  not null,
  transaction           bytea, --this will be changed to a json representation later with flattened args
  rejection_type        varchar,
  rejection_description varchar,

  constraint check_entry
  check (
    (typ = 'transaction' and transaction_id is not null and command_id is not null and application_id is not null and
     submitter is not null and effective_at is not null and transaction is not null) or
    (typ = 'rejection' and command_id is not null and application_id is not null and submitter is not null and
     rejection_type is not null and rejection_description is not null) or
    (typ = 'checkpoint'))

);

CREATE UNIQUE INDEX idx_transactions_deduplication
  ON ledger_entries (command_id, application_id);

CREATE TABLE disclosures (
  transaction_id varchar references ledger_entries (transaction_id) not null,
  event_id       varchar                                     not null,
  party          varchar                                     not null
);

CREATE TABLE contracts (
  id             varchar primary key                         not null,
  transaction_id varchar references ledger_entries (transaction_id) not null,
  workflow_id    varchar,
  package_id     varchar                                     not null,
  module_name    varchar                                     not null,
  entity_name    varchar                                     not null,
  created_at     timestamptz                                 not null,
  archived_at    timestamptz,
  contract       bytea                                       not null --this will be changed to a json representation later with flattened args

);
-- These two indices below could be a source performance bottleneck. Every additional index slows
-- down insertion. The contracts table will grow endlessly and the sole purpose of these indices is
-- to make ACS queries performant, while sacrificing insertion speed.
CREATE INDEX idx_contract_created
  ON contracts (created_at);

CREATE INDEX idx_contract_archived
  ON contracts (archived_at);

CREATE TABLE contract_witnesses (
  contract_id varchar references contracts (id) not null,
  witness     varchar                           not null
);

CREATE UNIQUE INDEX contract_witnesses_idx
  ON contract_witnesses (contract_id, witness);

-- a generic table to store meta information such as: ledger id and ledger end
CREATE TABLE parameters (
  key   varchar primary key not null,
  value varchar             not null
);
