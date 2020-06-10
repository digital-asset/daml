-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add a sequential row_id to participant_events
--
---------------------------------------------------------------------------------------------------

-- 1. add the column
alter table participant_events add column row_id serial;

-- 2. fix the row_id to be sequential according to the order of (event_offset, transaction_id, node_index)
update participant_events
set row_id = t.rownum
from (select event_offset, node_index, row_number() over (order by event_offset, transaction_id, node_index) as rownum
      from participant_events) t
where participant_events.event_offset = t.event_offset and participant_events.node_index = t.node_index;

-- 3. drop the now unused index
drop index participant_events_event_offset_transaction_id_node_index_idx;

-- 4. create a new index involving row_id
create index on participant_events (row_id);

-- 5. the second sub-query to find out the row_id needs this extra index to be fast
create index on participant_events (event_offset);
