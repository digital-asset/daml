-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add a sequential row_id to participant_events
--
---------------------------------------------------------------------------------------------------

-- 1. add the column
alter table participant_events add column row_id bigint auto_increment;

-- 2. fix the row_id to be sequential according to the order of (event_offset, transaction_id, node_index)
-- don't need to migrate H2 datasets

-- 3. drop the now unused index
drop index INDEX_9;

-- 4. create a new index involving row_id
create index participant_events_row_id
    on participant_events (row_id);

-- 5. the second sub-query to find out the row_id needs this extra index to be fast
create index participant_events_event_offset
    on participant_events (event_offset);
