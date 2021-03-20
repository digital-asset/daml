-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V33: Add witness columns to participant_events table.
--
-- Add `flat_event_witnesses` and `tree_event_witnesses` columns to `participant_events` table
-- Move all data from `participant_event_flat_transaction_witnesses` and
--  `participant_event_transaction_tree_witnesses` into corresponding array columns
-- Drop unused tables
---------------------------------------------------------------------------------------------------

-- already added to V20__Events_new_schema

-- GIN does not exist as index strategy on oracle
--      -- create index on participant_events using GIN (flat_event_witnesses);
--      -- create index on participant_events using GIN (tree_event_witnesses);
-- create index participant_events_flat_event_witnesses_idx on participant_events (flat_event_witnesses);
-- create index participant_events_tree_event_witnesses_idx on participant_events (tree_event_witnesses);


