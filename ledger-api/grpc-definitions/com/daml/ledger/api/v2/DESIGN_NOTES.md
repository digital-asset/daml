# Notes from Exploring a Ledger API v2 design

** Code and notes are all WIP by Simon. Please ignore for now. **

## Design Goals

1. Show how to build a unified events stream for a single-domain deployment

   a. Supporting: interface subscriptions, topology management, stream forwarding

   b. While minimizing the changes required to IndexDB v1.18

2. Show how to extend the above with support for multi-domain subscriptions with transfer-in, transfer-out,
   the vector clocks required for consumers to reconstruct a causal order.


## Status

All work is done on files contained in the `com/daml/ledger/api/v2` folder.

The current focus is on exploring the design space. The might not be complete.

Features (to be) sketched:


- in `events_service_single_doamin.proto`
  - DONE:
    - supports serving a snapshot and delta on the same stream
    - serves transaction nodes in individual messages
    - supports retrieving the transaction of a transaction node
    - supports consumers in building the transaction tree
  - WIP:
    - include topology management
      - package vetting
      - party to participant mapping
      - package uploads
  - Next design goals:
    - supports TransactionFilter
    - provides clear docs for how the events are filtered
    - minimize differences to existing transaction_service and active_contracts_service
    - ensure that there is an efficient strategy for serving the stream
    - include completions
    - communicate pruning events
    - include all data required for Ledger API completeness
    - signal pruning
    - signal a data corruption to the application; telling it to reinitialize


- in `events_service_multi_domain.proto`
  - sketch multi-domain txs



# Scratchpad -- PLEASE IGNORE

## On Projections of Event Streams

Design requirements:
- union streams
- filter streams specific to what one party can see
- filter streams to only show actions on contracts of a specific template
- show subtrees of exercise nodes
- selectively include: exercise nodes, divulgence nodes, fetch nodes, lookup by key nodes, create nodes, topology actions


exercise nodes: filter on informees == stakeholders + actors + choice observers
create nodes: filter on informees == stakeholders


event-seq-id : Int64 = [ index : 54bit | tags : 10bit ]

[ domain_id : 20bit | event_idx : 40bit | kind : 4bit ]

[ | reserved: 4bit | tx_idx: 50bit | reserved: 2bit | event_idx : 14bit | kind: 3bit | audit: 1bit ]


TABLE audit_events {
  tx_idx bigint
  event_payload bytes[] NOT NULL
}

TABLE state_events {
  event_idx bigint
  event_payload bytes NOT NULL
}

TABLE state_archivals {
  create_event_idx bigint
  archive_event_idx bigint
}

TABLE state_ids {
  bytes identifier NOT NULL,
  event_idx bigint NOT NULL
}

-- party and template filtering, package filtering? implements interface filtering?
TABLE events_attributes {
  attribute                  bigint NOT NULL,
  domain_event_idx           bigint NOT NULL, 
  subtree_end_event_idx_off  integer
}

TABLE event_archivals {
  archive_domain_id  integer NOT NULL,
  create_domain_id   integer NOT NULL,
  archive_event_idx  bigint  NOT NULL,
  create_event_idx   bigint  NOT NULL,
}

CREATE INDEX archival_idx1 ON event_archivals USING btree (create_domain_id, create_event_idx, archive)
CREATE INDEX archival_idx2 ON event_archivals USING btree (create_domain_id, create_event_idx)



TABLE contract_id_interning {
  contract_id
  event_sequential_id
}


