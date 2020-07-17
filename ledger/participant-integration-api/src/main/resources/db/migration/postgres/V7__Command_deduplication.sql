-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V7: Command deduplication
--
-- This schema version changes the unique index for command deduplication
-- from (command_id, application_id) to (submitter, command_id, application_id).
---------------------------------------------------------------------------------------------------

DROP INDEX idx_transactions_deduplication;

-- This embodies the deduplication in the Ledger API.
CREATE UNIQUE INDEX idx_transactions_deduplication
  ON ledger_entries (submitter, command_id, application_id);
