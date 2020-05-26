-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V10.2: Extract event data
--
-- This schema version marks the contracts.create_event_id column as NOT NULL.

ALTER TABLE contracts ALTER COLUMN create_event_id SET NOT NULL;
