-- Experiment to move tx-meta information into a shared table to lower load on DB
BEGIN;

DROP TABLE IF EXISTS participant_transactions_meta;
DROP TABLE IF EXISTS participant_events_create_no_tx;
DROP TABLE IF EXISTS participant_events_create_interned_no_tx;

CREATE TABLE participant_transactions_meta ( 
  transaction_internal_id bigserial primary key,
  transaction_external_id text NOT NULL,
  transaction_offset      text NOT NULL,
  ledger_effective_time   timestamp without time zone NOT NULL,
  command_id              text,
  workflow_id             text,
  application_id          text,
  submitters              text[]
);

EXPLAIN ANALYZE
INSERT INTO participant_transactions_meta (
   transaction_offset,
   transaction_external_id,
   ledger_effective_time,
   command_id,
   workflow_id,
   application_id,
   submitters)
  SELECT 
    event_offset,
    max(transaction_id),
    max(ledger_effective_time),
    max(command_id),
    max(workflow_id),
    max(application_id),
    max(submitters) 
  FROM participant_events,
       parameters
  WHERE -- ignore divulgence events, as they do not have tx information and 
        -- they always occur together with a visible
        -- exercise node in the same transaction
        event_kind != 0 AND
        -- ignore create events before the pruning offset, they do not necessarily have
        -- transaction information
        ( parameters.participant_pruned_up_to_inclusive IS NULL 
          OR
          parameters.participant_pruned_up_to_inclusive < event_offset
        )
 GROUP BY event_offset
 ORDER BY event_offset;


CREATE TABLE participant_events_create_no_tx AS
  SELECT 
    transaction_internal_id,
    event_sequential_id,         
    node_index,                  
    contract_id,                 
    template_id,                 
    flat_event_witnesses,        
    tree_event_witnesses,        
    create_argument,             
    create_signatories,          
    create_observers,            
    create_agreement_text,       
    create_key_value,            
    create_key_hash,             
    create_argument_compression, 
    create_key_value_compression
  FROM participant_events_create LEFT OUTER JOIN
         participant_transactions_meta ON (event_offset = transaction_offset);



CREATE TABLE participant_events_create_interned_no_tx AS
  SELECT 
    transaction_internal_id,
    event_sequential_id,         
    node_index,                  
    contract_id,                 
    template_id,                 
    flat_event_witnesses,        
    tree_event_witnesses,        
    create_argument,             
    create_signatories,          
    create_observers,            
    create_agreement_text,       
    create_key_value,            
    create_key_hash,             
    create_argument_compression, 
    create_key_value_compression
  FROM participant_events_create_interned LEFT OUTER JOIN
         participant_transactions_meta ON (event_offset = transaction_offset);

SELECT count(event_offset) as num_transactions, sum(savings) as num_duplicates_saved
FROM (
  SELECT 
    event_offset,
    count(transaction_id) - 1 as savings
  FROM participant_events,
       parameters
  WHERE -- ignore divulgence events, as they do not have tx information and 
        -- they always occur together with a visible
        -- exercise node in the same transaction
        event_kind != 0 AND
        -- ignore create events before the pruning offset, they do not necessarily have
        -- transaction information
        ( parameters.participant_pruned_up_to_inclusive IS NULL 
          OR
          parameters.participant_pruned_up_to_inclusive < event_offset
        )
 GROUP BY event_offset
 ORDER BY event_offset) as foo;


 END