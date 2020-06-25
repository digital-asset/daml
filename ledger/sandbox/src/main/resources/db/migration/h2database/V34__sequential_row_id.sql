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
-- looks like we never created this index in H2

-- 4. create a new index involving row_id
create index on participant_events (row_id);

-- 5. the second sub-query to find out the row_id needs this extra index to be fast
create index on participant_events (event_offset);
