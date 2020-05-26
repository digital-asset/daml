-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V12: Add table for ledger configuration changes
--
-- This schema version adds a table for ledger configuration changes and adds the latest
-- configuration to the parameters table.
---------------------------------------------------------------------------------------------------

-- Table for storing a log of ledger configuration changes and rejections.
CREATE TABLE configuration_entries (
  ledger_offset bigint primary key not null,
  recorded_at timestamp not null, -- with time zone

  submission_id varchar not null,
  participant_id varchar not null,
  -- The type of entry, one of 'accept' or 'reject'.
  typ varchar not null,
  -- The configuration that was proposed and either accepted or rejected depending on the type.
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
);

-- Index for retrieving the configuration entry by submission identifier.
-- To be used for completing configuration submissions.
CREATE UNIQUE INDEX idx_configuration_submission
  ON configuration_entries (submission_id, participant_id);

-- Add the current configuration column to parameters.
ALTER TABLE parameters
ADD configuration bytea;
