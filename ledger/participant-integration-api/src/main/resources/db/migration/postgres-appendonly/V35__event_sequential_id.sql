-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V34: Add event_sequential_id column to participant_events
--
---------------------------------------------------------------------------------------------------

-- 1. add the column
alter table participant_events add column event_sequential_id bigserial;

-- 2. fix the event_sequential_id to be sequential according to the order of (event_offset, transaction_id, node_index)
update participant_events
set event_sequential_id = t.rownum
from (select event_offset, node_index, row_number() over (order by event_offset, transaction_id, node_index) as rownum
      from participant_events) t
where participant_events.event_offset = t.event_offset and participant_events.node_index = t.node_index;

-- 3. drop the now unused index
drop index participant_events_event_offset_transaction_id_node_index_idx;

-- 4. create a new index involving event_sequential_id
create index participant_events_event_sequential_id
    on participant_events (event_sequential_id);

-- 5. we need this index to convert event_offset to event_sequential_id
create index participant_events_event_offset
    on participant_events (event_offset);
