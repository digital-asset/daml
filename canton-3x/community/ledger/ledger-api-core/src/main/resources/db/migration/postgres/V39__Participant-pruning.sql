-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V39: Pruned offset tracking
--
-- This schema version adds tracking of the pruned portion of the participant ledger to the
-- parameters table.
---------------------------------------------------------------------------------------------------

-- Add the column for most recent pruning offset to parameters.
-- A value of NULL means that the participant has not been pruned so far.
ALTER TABLE parameters ADD COLUMN participant_pruned_up_to_inclusive BYTEA;
