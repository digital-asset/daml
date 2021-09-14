-- This file prototypes the queries for efficient ACS fetching

BEGIN;

\d+ participant_events_create

/*


EXPLAIN ANALYZE
SELECT create_evs.*
  FROM participant_events_create create_evs 
 WHERE (create_evs.flat_event_witnesses && array['ACSnoWitnessedContracts-alpha-967d940a71-party-1']::text[])         
   AND '452433dca0277fab914ac7400fb7c5373217b3a9a4a481d886b712f7cff6ea81:Test:Witnesses' = create_evs.template_id
   AND 0 < event_sequential_id                                        -- page start
   AND event_sequential_id <= 1000                                    -- snapshot event_sequential_id
   AND NOT EXISTS (                                                   -- check not archived as of snapshot
        SELECT 1 FROM participant_events_consuming_exercise consuming_evs
         WHERE create_evs.contract_id = consuming_evs.contract_id
           AND consuming_evs.event_sequential_id <= 1000)
 ORDER BY create_evs.event_sequential_id  -- deliver in index order
   LIMIT 1000;




DROP TABLE IF EXISTS participant_events_create_filter_non_interned;

CREATE TABLE participant_events_create_filter_non_interned AS
    SELECT event_sequential_id, template_id, party_id 
      FROM participant_events_create create_evs, 
           parameters,
           unnest(flat_event_witnesses) as party_id
     ORDER BY event_sequential_id;  -- use temporal odering
    
-- used for efficient pruning
CREATE INDEX participant_events_create_filter_ni_event_sequential_id_idx ON participant_events_create_filter_non_interned USING btree(event_sequential_id);
-- used for efficient filtering of the ACS
CREATE INDEX participant_events_create_filter_ni_party_template_seq_id_idx ON participant_events_create_filter_non_interned USING btree(party_id, template_id, event_sequential_id);

-- select * from participant_events_create_filter_non_interned;


EXPLAIN ANALYZE
SELECT create_evs.*
  FROM participant_events_create_filter_non_interned filters 
         INNER JOIN participant_events_create_interned create_evs USING (event_sequential_id)
 WHERE filters.party_id = 'ACSnoWitnessedContracts-alpha-967d940a71-party-1'
   AND '452433dca0277fab914ac7400fb7c5373217b3a9a4a481d886b712f7cff6ea81:Test:Witnesses' = filters.template_id 
   AND 0 < event_sequential_id
   AND event_sequential_id <= 1000                                    -- snapshot event_sequential_id
   AND NOT EXISTS (                                                   -- check not archived as of snapshot
        SELECT 1 FROM participant_events_consuming_exercise consuming_evs
         WHERE create_evs.contract_id = consuming_evs.contract_id
           AND consuming_evs.event_sequential_id <= 1000)
 ORDER BY filters.party_id, filters.template_id, filters.event_sequential_id  -- deliver in index order
   LIMIT 1000;


-- show interning
select * from participant_events_create_filter;

\d+

*/

-- \d+ participant_template_interning
-- \d+ participant_party_interning




COMMIT;