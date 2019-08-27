-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V7: Add table for ledger configuration changes
--
-- This schema version adds a table for ledger configuration changes and adds the latest
-- configuration to the parameters table.
---------------------------------------------------------------------------------------------------

-- Table for storing a log of ledger configuration changes and rejections.
CREATE TABLE configuration_entries (
  ledger_offset bigint primary key not null, -- FIXME(JM): What is it?
  submission_id varchar not null,
  -- The type of entry, one of 'accept' or 'reject'.
  typ varchar not null,
  -- The configuration that was proposed and either accepted
  -- or rejected depending on the type.
  -- Encoded according to participant-state/protobuf/ledger_configuration.proto.
  configuration bytea not null,

  -- If the type is 'rejection', then the rejection reason is set.
  -- Rejection reason is a human-readable description why the change was rejected.
  rejection_reason varchar,

  -- Check that fields are correctly set based on the type.
  constraint check_entry
  check (
    (typ = 'accept' and rejection_reason is null) or
    (typ = 'reject' and rejection_reason is not null))

  constraint submission_id_dedup UNIQUE(submission_id)
);

-- Alternatively:
ALTER TABLE ledger_entries
ADD COLUMN configuration bytea,
ADD COLUMN configuration_rejection_reason varchar,
ADD COLUMN configuration_submission_id varchar,
DROP CONSTRAINT check_entry,
ADD CONSTRAINT check_entry check (
    (typ = 'transaction' and transaction_id is not null and command_id is not null and application_id is not null and
     submitter is not null and effective_at is not null and transaction is not null) or
    (typ = 'rejection' and command_id is not null and application_id is not null and submitter is not null and
     rejection_type is not null and rejection_description is not null) or
    (typ = 'checkpoint') or
    (typ = 'configuration' and configuration is not null and configuration_submission_id is not null) or
    (typ = 'configuration_rejection' and configuration is not null and rejection_reason is not null and configuration_submission_id is not null))

-- ^ FIXME(JM): What about e.g. idx_transactions_deduplication and so on?

-- An alternative way to represent the ledger_entries would be to
-- store all data in protobuf and have:
-- message LedgerEntry {
--   oneof entry {
--     TxEntry transaction = 1;
--     Configuration configuration = 2;
--     PackageUpload package_upload = 3;
--     PartyAllocation party_alloc = 4;
--     ...
--   }
-- }
-- One would still keep the different indices on the table,
-- e.g. one index for transaction_ids, one for config submission_ids, etc.
-- "kvutils" payloads for these could be moved into participant-state
-- (same way as configuration)
--
-- Could look like:

CREATE TABLE ledger
(
  -- Participant-local ledger offset, assigned by indexer.
  ledger_offset bigint primary key not null,

  -- The type of the entry. Corresponds to LedgerEntry field number.
  -- (or repeat the field name)
  typ int not null,

  -- Entry payload defined by 'LedgerEntry' message.
  data bytea not null,

  -- Record time of the entry
  recorded_at timestamptz not null,

  -- "Indices" for typ = 1
  transaction_id varchar unique,
  command_id varchar,
  application_id varchar,
  workflow_id varchar, -- needed?
  effective_at timestamptz,

  -- "Indices" for typ = 2, 3 or 4
  submission_id varchar unique, -- FIXME(JM): not globally unique?
)

CREATE UNIQUE INDEX idx_transactions_deduplication
  ON ledger (command_id, application_id);


-- Add the current configuration column to parameters.
ALTER TABLE parameters
ADD configuration bytea;
