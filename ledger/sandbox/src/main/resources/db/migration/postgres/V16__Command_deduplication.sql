-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V16: New command deduplication
--
-- Command deduplication has moved from ledger to participant
---------------------------------------------------------------------------------------------------


DROP INDEX idx_transactions_deduplication;


CREATE TABLE participant_command_submissions(
  -- The deduplication key
  deduplication_key  varchar primary key   not null,
  -- The time the command was first submitted
  submitted_at       timestamptz           not null,
  -- The time the command will stop being deduplicated
  ttl                timestamptz           not null,
  -- If set, then the command was successfully submitted
  success            boolean,
  -- If set, then the command submission failed with the given error
  error              varchar
);