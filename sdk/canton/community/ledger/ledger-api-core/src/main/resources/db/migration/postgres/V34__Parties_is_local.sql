-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add is_local column to parties table.
--
-- This marks parties as to whether they are local.
--
-- To initialize parties.is_local, rely on party_entries.is_local.
---------------------------------------------------------------------------------------------------

ALTER TABLE parties ADD COLUMN is_local bool;

UPDATE parties SET is_local = (
SELECT party_entries.is_local
FROM party_entries where party_entries.party = parties.party);

-- implicit party allocation produces local parties not present in party_entries
UPDATE parties SET is_local = true WHERE is_local IS NULL;

ALTER TABLE parties ALTER COLUMN is_local SET NOT NULL;
