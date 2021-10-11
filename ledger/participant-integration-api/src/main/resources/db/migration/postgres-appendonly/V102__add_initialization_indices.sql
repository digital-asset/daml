-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V102: Add indices to speed up indexer initialization
--
-- At startup, the indexer deletes all entries with an offset beyond the stored ledger end.
-- Such entries can be written when the indexer crashes right before updating the ledger end.
-- This migration adds missing indices to speed up the deletion of such entries.
---------------------------------------------------------------------------------------------------

CREATE INDEX packages_ledger_offset_idx ON packages USING btree (ledger_offset);
CREATE INDEX parties_ledger_offset_idx ON parties USING btree (ledger_offset);
