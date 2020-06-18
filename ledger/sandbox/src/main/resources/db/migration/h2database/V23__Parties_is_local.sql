-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add is_local column to parties table.
--
-- This allows parties to be marked as to whether they were added locally as determined by whether
-- the specified participant_id matched the local participant_id.
--
-- To initialize parties.is_local properly, rely on the existence of party_entries.is_local
-- default to parties.is_local = true only if party_entries were somehow manually deleted.
---------------------------------------------------------------------------------------------------

ALTER TABLE parties ADD COLUMN is_local bool;

UPDATE parties SET is_local = false
WHERE party IN (SELECT party FROM party_entries WHERE is_local = false);

UPDATE parties SET is_local = true WHERE is_local IS NULL;

ALTER TABLE parties ALTER COLUMN is_local SET NOT NULL;
