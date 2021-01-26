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

alter table participant_events add column flat_event_witnesses array not null default array[];
alter table participant_events add column tree_event_witnesses array not null default array[];

-- TODO(Leo): find out if we need it, don't know if it works the same way with arrays as Postgres GIN index
create index on participant_events(flat_event_witnesses);
create index on participant_events(tree_event_witnesses);

drop table participant_event_flat_transaction_witnesses;
drop table participant_event_transaction_tree_witnesses;

create alias array_intersection for "com.daml.platform.store.dao.events.SqlFunctions.arrayIntersection"
