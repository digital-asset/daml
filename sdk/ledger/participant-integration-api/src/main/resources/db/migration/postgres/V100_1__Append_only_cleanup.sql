-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V100.1: Append-only schema
--
-- This step drops the now unused tables and creates a view that transparently replaces the old
-- participant_events table.
---------------------------------------------------------------------------------------------------


---------------------------------------------------------------------------------------------------
-- Drop old tables, at this point all data has been copied to the new tables
---------------------------------------------------------------------------------------------------

DROP TABLE participant_contracts CASCADE;
DROP TABLE participant_contract_witnesses CASCADE;
DROP TABLE participant_events CASCADE;


---------------------------------------------------------------------------------------------------
-- Events table: view of all events
---------------------------------------------------------------------------------------------------

-- This view is used to drive the transaction and transaction tree streams,
-- which will in the future also contain divulgence events.
-- The event_kind field defines the type of event (numbers allocated to leave some space for future additions):
--    0: divulgence event
--   10: create event
--   20: consuming exercise event
--   25: non-consuming exercise event
-- is not negatively affected by a long list of columns that are never used.
CREATE VIEW participant_events
AS
SELECT
    0::smallint as event_kind,
    event_sequential_id,
    NULL::text as event_offset,
    NULL::text as transaction_id,
    NULL::timestamp without time zone as ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    NULL::integer as node_index,
    NULL::text as event_id,
    contract_id,
    template_id,
    NULL::text[] as flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    NULL::text[] as create_signatories,
    NULL::text[] as create_observers,
    NULL::text as create_agreement_text,
    NULL::bytea as create_key_value,
    NULL::text as create_key_hash,
    NULL::text as exercise_choice,
    NULL::bytea as exercise_argument,
    NULL::bytea as exercise_result,
    NULL::text[] as exercise_actors,
    NULL::text[] as exercise_child_event_ids,
    create_argument_compression,
    NULL::smallint as create_key_value_compression,
    NULL::smallint as exercise_argument_compression,
    NULL::smallint as exercise_result_compression
FROM participant_events_divulgence
UNION ALL
SELECT
    10::smallint as event_kind,
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
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
    NULL::text as exercise_choice,
    NULL::bytea as exercise_argument,
    NULL::bytea as exercise_result,
    NULL::text[] as exercise_actors,
    NULL::text[] as exercise_child_event_ids,
    create_argument_compression,
    create_key_value_compression,
    NULL::smallint as exercise_argument_compression,
    NULL::smallint as exercise_result_compression
FROM participant_events_create
UNION ALL
SELECT
    20::smallint as event_kind,
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    NULL::bytea as create_argument,
    NULL::text[] as create_signatories,
    NULL::text[] as create_observers,
    NULL::text as create_agreement_text,
    create_key_value,
    NULL::text as create_key_hash,
    exercise_choice,
    exercise_argument,
    exercise_result,
    exercise_actors,
    exercise_child_event_ids,
    NULL::smallint as create_argument_compression,
    create_key_value_compression,
    exercise_argument_compression,
    exercise_result_compression
FROM participant_events_consuming_exercise
UNION ALL
SELECT
    25::smallint as event_kind,
    event_sequential_id,
    event_offset,
    transaction_id,
    ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    node_index,
    event_id,
    contract_id,
    template_id,
    flat_event_witnesses,
    tree_event_witnesses,
    NULL::bytea as create_argument,
    NULL::text[] as create_signatories,
    NULL::text[] as create_observers,
    NULL::text as create_agreement_text,
    create_key_value,
    NULL::text as create_key_hash,
    exercise_choice,
    exercise_argument,
    exercise_result,
    exercise_actors,
    exercise_child_event_ids,
    NULL::smallint as create_argument_compression,
    create_key_value_compression,
    exercise_argument_compression,
    exercise_result_compression
FROM participant_events_non_consuming_exercise
;


---------------------------------------------------------------------------------------------------
-- Parameters table
---------------------------------------------------------------------------------------------------

-- new field: the sequential_event_id up to which all events have been ingested
ALTER TABLE parameters ADD COLUMN ledger_end_sequential_id bigint;
UPDATE parameters SET ledger_end_sequential_id = (
    SELECT max(event_sequential_id) FROM participant_events
);

-- Note that ledger_end_sequential_id_before will not be equal to ledger_end_sequential_id_after,
-- as the append-only migration creates divulgence events.
UPDATE participant_migration_history_v100
SET ledger_end_sequential_id_after = (
    SELECT max(ledger_end_sequential_id) FROM parameters
);
