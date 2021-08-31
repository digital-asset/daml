-- This file prototypes the queries for efficient ACS fetching

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
  