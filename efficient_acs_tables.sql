BEGIN;

-- The following code is meant to be applied to an IndexDB with the append-only schema.
-- It prototypes the filter tables and queries to fetch data from it.

DROP TABLE IF EXISTS participant_template_interning;
DROP TABLE IF EXISTS participant_party_interning;
DROP TABLE IF EXISTS participant_events_create_interned;
DROP TABLE IF EXISTS participant_events_create_filter;
DROP TABLE IF EXISTS participant_streams_active_contracts;


CREATE TABLE participant_template_interning 
  ( template_internal_id SERIAL PRIMARY KEY
  , template_external_id TEXT NOT NULL UNIQUE
  );

INSERT INTO participant_template_interning (template_external_id)
  SELECT template_id
    FROM participant_events
   GROUP BY template_id
   ORDER BY template_id;

CREATE TABLE participant_party_interning 
  ( party_internal_id SERIAL PRIMARY KEY
  , party_external_id TEXT NOT NULL UNIQUE
  );

INSERT INTO participant_party_interning (party_external_id)
  SELECT party_id
    FROM participant_events,
         unnest(array_cat(flat_event_witnesses, array_cat(tree_event_witnesses, array_cat(submitters, array_cat(create_signatories, create_observers))))) witnesses(party_id)
   GROUP BY party_id
   ORDER BY party_id;


-- EXPLAIN ANALYZE 
-- WITH 
--   flat_event_interning AS (
--     SELECT event_sequential_id, array_agg(party_internal_id) as flat_event_witnesses_internal
--       FROM participant_events, 
--            unnest(flat_event_witnesses) witnesses(party_external_id) INNER JOIN participant_party_interning USING (party_external_id)
--      GROUP BY event_sequential_id
--   ),
--   tree_event_interning AS (
--     SELECT event_sequential_id, array_agg(party_internal_id) as tree_event_witnesses_internal
--       FROM participant_events, 
--            unnest(tree_event_witnesses) witnesses(party_external_id) INNER JOIN participant_party_interning USING (party_external_id)
--      GROUP BY event_sequential_id
--   )
-- SELECT event_sequential_id, event_kind, 
--        template_id, template_internal_id, 
--        COALESCE(flat_event_witnesses_internal, array[]::integer[]),
--        COALESCE(tree_event_witnesses_internal, array[]::integer[])
--   FROM participant_events 
--          LEFT OUTER JOIN flat_event_interning USING (event_sequential_id)
--          LEFT OUTER JOIN tree_event_interning USING (event_sequential_id)
--          LEFT OUTER JOIN participant_template_interning ON (template_id = template_external_id);
     
EXPLAIN ANALYZE
CREATE TABLE participant_events_create_interned AS 
  SELECT  
        event_sequential_id,
        ledger_effective_time,
        node_index,
        event_offset,
        transaction_id,
        workflow_id,
        command_id,
        application_id,
        
        COALESCE(
            (SELECT array_agg(party_internal_id)
              FROM unnest(submitters) witnesses(party_external_id) 
                     INNER JOIN participant_party_interning USING (party_external_id)
            ), array[]::integer[]
        ) submitters,

        event_id,
        contract_id,
        template_internal_id template_id,

        COALESCE(
            (SELECT array_agg(party_internal_id)
              FROM unnest(flat_event_witnesses) witnesses(party_external_id) 
                     INNER JOIN participant_party_interning USING (party_external_id)
            ), array[]::integer[]
        ) flat_event_witnesses,

        COALESCE(
            (SELECT array_agg(party_internal_id)
              FROM unnest(tree_event_witnesses) witnesses(party_external_id) 
                     INNER JOIN participant_party_interning USING (party_external_id)
            ), array[]::integer[]
        ) tree_event_witnesses,

        create_argument,

        COALESCE(
            (SELECT array_agg(party_internal_id)
              FROM unnest(create_signatories) witnesses(party_external_id) 
                     INNER JOIN participant_party_interning USING (party_external_id)
            ), array[]::integer[]
        ) create_signatories,

        COALESCE(
            (SELECT array_agg(party_internal_id)
              FROM unnest(create_observers) witnesses(party_external_id) 
                     INNER JOIN participant_party_interning USING (party_external_id)
            ), array[]::integer[]
        ) create_observers,

        create_agreement_text,
        create_key_value,
        create_key_hash,
        create_argument_compression,
        create_key_value_compression

  FROM participant_events_create 
         LEFT OUTER JOIN participant_template_interning ON (template_id = template_external_id)
 ORDER BY event_sequential_id;  -- retain or re-establish temporal ordering

CREATE UNIQUE INDEX participant_events_create_interned_event_sequential_id_idx ON participant_events_create_interned USING btree(event_sequential_id);


EXPLAIN ANALYZE
CREATE TABLE participant_events_create_filter AS
    SELECT event_sequential_id, template_id, party_id 
      FROM participant_events_create_interned create_evs, 
           parameters,
           unnest(flat_event_witnesses) as party_id
    -- TODO: in real migration drop the archived ones. Here we leave it for the experiments.
    --  WHERE NOT EXISTS (
    --           SELECT 1 FROM participant_events_consuming_exercise consuming_evs
    --            WHERE create_evs.contract_id = consuming_evs.contract_id
    --              AND consuming_evs.event_offset <= parameters.ledger_end
    --        )
     ORDER BY event_sequential_id;  -- use temporal odering
    
-- used for efficient pruning
CREATE INDEX participant_events_create_filter_event_sequential_id_idx ON participant_events_create_filter USING btree(event_sequential_id);
-- used for efficient filtering of the ACS
CREATE INDEX participant_events_create_filter_party_template_seq_id_idx ON participant_events_create_filter USING btree(party_id, template_id, event_sequential_id);


-- a table to keep track of the ACS fetches that are ongoing
-- used to enable aggressive, but not too aggressive pruning of the filter table
CREATE TABLE participant_streams_active_contracts (
    -- request metadata
    correlation_id uuid PRIMARY KEY,           -- correlation-id of the gRPC request
    application_id text,                       -- application id of the gRPC request's claims
    transaction_filter jsonb NOT NULL,         -- the filter used in the gRPC request
    -- snapshot metadata
    snapshot_event_sequential_id_incl bigint,  -- the non-strict upper bound of what events to include in the snapshot 
    -- stream serving metadata
    last_heartbeat_at timestamptz              -- the last time the stream fetching code marked this entry as alive by setting
                                               -- its heartbeat to now()
);

CREATE INDEX participant_streams_active_contracts_snapshot_seq_id_idx ON participant_streams_active_contracts USING btree(snapshot_event_sequential_id_incl);
CREATE INDEX participant_streams_active_contracts_last_heartbeat_at_idx ON participant_streams_active_contracts USING btree(last_heartbeat_at);

INSERT INTO participant_streams_active_contracts
  VALUES ('a0eebc99-9c0b-4ef8-bb6d-6bb9bd380a11', 'test-app', '{}', 800, now());

END;