-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


Params:
0: 0
1: 1
2: [4, 5]
3: [6, 7]
4: [8, 9]
5: 0
6: 1
7: [4, 5]
8: [6, 7]
9: [8, 9]
10: 0
11: 1
12: [4, 5]
13: [6, 7]
14: [8, 9]
15: 2
Query:

        
        SELECT
          event_offset, transaction_id, node_index, event_sequential_id, ledger_effective_time, workflow_id, event_id, contract_id, template_id, create_argument, create_argument_compression, create_signatories, create_observers, create_agreement_text, create_key_value, create_key_value_compression, submitters, flat_event_witnesses as event_witnesses, command_id
        FROM
          participant_events_create 
        WHERE
          
            event_sequential_id > ? AND
            event_sequential_id <= ? AND
          ((flat_event_witnesses::integer[] && ?::integer[]) or ( (flat_event_witnesses::integer[] && ?::integer[]) AND (template_id = ANY(?::integer[])) ))
       UNION ALL
        SELECT
          event_offset, transaction_id, node_index, event_sequential_id, ledger_effective_time, workflow_id, event_id, contract_id, template_id, NULL as create_argument, NULL as create_argument_compression, NULL as create_signatories, NULL as create_observers, NULL as create_agreement_text, create_key_value, create_key_value_compression, submitters, flat_event_witnesses as event_witnesses, command_id
        FROM
          participant_events_consuming_exercise 
        WHERE
          
            event_sequential_id > ? AND
            event_sequential_id <= ? AND
          ((flat_event_witnesses::integer[] && ?::integer[]) or ( (flat_event_witnesses::integer[] && ?::integer[]) AND (template_id = ANY(?::integer[])) ))
       UNION ALL
        SELECT
          event_offset, transaction_id, node_index, event_sequential_id, ledger_effective_time, workflow_id, event_id, contract_id, template_id, NULL as create_argument, NULL as create_argument_compression, NULL as create_signatories, NULL as create_observers, NULL as create_agreement_text, create_key_value, create_key_value_compression, submitters, flat_event_witnesses as event_witnesses, command_id
        FROM
          participant_events_non_consuming_exercise 
        WHERE
          
            event_sequential_id > ? AND
            event_sequential_id <= ? AND
          ((flat_event_witnesses::integer[] && ?::integer[]) or ( (flat_event_witnesses::integer[] && ?::integer[]) AND (template_id = ANY(?::integer[])) ))
      
        ORDER BY event_sequential_id
        fetch next ? rows only

