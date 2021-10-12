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

alter table participant_events
  add column flat_event_witnesses varchar[] not null default '{}',
  add column tree_event_witnesses varchar[] not null default '{}'
;

create index on participant_events using GIN (flat_event_witnesses);
create index on participant_events using GIN (tree_event_witnesses);

update participant_events set flat_event_witnesses = o.warr
    from (select event_id, array_agg(event_witness) as warr from participant_event_flat_transaction_witnesses group by event_id) as o
    where participant_events.event_id = o.event_id;

update participant_events set tree_event_witnesses = o.warr
    from (select event_id, array_agg(event_witness) as warr from participant_event_transaction_tree_witnesses group by event_id) as o
    where participant_events.event_id = o.event_id;

drop table participant_event_flat_transaction_witnesses;
drop table participant_event_transaction_tree_witnesses;
