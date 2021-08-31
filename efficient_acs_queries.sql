-- This file prototypes the queries for efficient ACS fetching

BEGIN;

-- fetching a page for all contracts visible to party 35 and not party 34 as of event_sequential_id 1000
EXPLAIN ANALYZE
SELECT create_evs.*
  FROM participant_events_create_filter filters 
         INNER JOIN participant_events_create_interned create_evs USING (event_sequential_id)
 WHERE filters.party_id = 35                                          -- fetching snapshot for party 35
   AND NOT (create_evs.flat_event_witnesses && array[34]::integer[])  -- for contracts without 34 as a stakeholder
   AND 10 < filters.template_id                                       -- page lower bound for templates
   AND 0 < event_sequential_id
   AND event_sequential_id <= 1000                                    -- snapshot event_sequential_id
   AND NOT EXISTS (                                                   -- check not archived as of snapshot
        SELECT 1 FROM participant_events_consuming_exercise consuming_evs
         WHERE create_evs.contract_id = consuming_evs.contract_id
           AND consuming_evs.event_sequential_id <= 1000)
 ORDER BY filters.party_id, filters.template_id, filters.event_sequential_id  -- deliver in index order
   LIMIT 1000;

-- fetching a page for all contracts of template 36, visible to party 35 and not party 34 as of event_sequential_id 1000
--
-- TODO: modify page queries to check that their snapshot's required data has not been pruned
--
EXPLAIN ANALYZE
SELECT create_evs.*
  FROM participant_events_create_filter filters 
         INNER JOIN participant_events_create_interned create_evs USING (event_sequential_id)
 WHERE filters.party_id = 35                                          -- fetching snapshot for party 35
   AND NOT (create_evs.flat_event_witnesses && array[34]::integer[])  -- for contracts without 34 as a stakeholder
   AND 36 = filters.template_id                                       -- page lower bound for templates
   AND 0 < event_sequential_id
   AND event_sequential_id <= 1000                                    -- snapshot event_sequential_id
   AND NOT EXISTS (                                                   -- check not archived as of snapshot
        SELECT 1 FROM participant_events_consuming_exercise consuming_evs
         WHERE create_evs.contract_id = consuming_evs.contract_id
           AND consuming_evs.event_sequential_id <= 1000)
 ORDER BY filters.party_id, filters.template_id, filters.event_sequential_id  -- deliver in index order
   LIMIT 1000;


-- applying the pruning changes from consuming exercises with 0 <= event_sequential_id <= 1000
EXPLAIN ANALYZE
WITH earliest_snapshot AS (SELECT coalesce(min(snapshot_event_sequential_id_incl), 1000) as snapshot_seq_id FROM participant_streams_active_contracts)
DELETE FROM participant_events_create_filter
 USING participant_events_create_interned create_evs 
         INNER JOIN participant_events_consuming_exercise consuming_evs USING (contract_id),
       earliest_snapshot
       -- TODO: make sure that the right kind of inclusive (<=) and non-inclusive (<) comparisons are used
 WHERE participant_events_create_filter.event_sequential_id = create_evs.event_sequential_id
   AND 0 <= consuming_evs.event_sequential_id
   AND consuming_evs.event_sequential_id <= earliest_snapshot.snapshot_seq_id;


ROLLBACK;