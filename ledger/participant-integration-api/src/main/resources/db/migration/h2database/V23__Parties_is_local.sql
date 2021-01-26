-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add is_local column to parties table.
--
-- This marks parties as to whether they are local.
--
-- Initializes parties.is_local to true as migrations are not supported for h2database.
---------------------------------------------------------------------------------------------------

ALTER TABLE parties ADD COLUMN is_local bool NOT NULL DEFAULT true;
ALTER TABLE parties ALTER COLUMN is_local DROP DEFAULT;
