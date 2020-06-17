-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add is_local column to parties table.
--
-- This allows parties to be marked as to whether they were added locally as determined by whether
-- the specified participant_id matched the local participant_id.
---------------------------------------------------------------------------------------------------

ALTER TABLE parties ADD COLUMN is_local bool not null default true;
