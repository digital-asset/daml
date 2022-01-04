SELECT 'Dropping and recreating participant_events view...';

DROP VIEW participant_events;

-- re-add the participant_events view
CREATE VIEW participant_events
AS
SELECT
    0::smallint as event_kind,
    event_sequential_id,
    event_offset,
    NULL::text as transaction_id,
    NULL::bigint as ledger_effective_time,
    command_id,
    workflow_id,
    application_id,
    submitters,
    NULL::integer as node_index,
    NULL::text as event_id,
    contract_id,
    template_id,
    NULL::INTEGER[] as flat_event_witnesses,
    tree_event_witnesses,
    create_argument,
    NULL::INTEGER[] as create_signatories,
    NULL::INTEGER[] as create_observers,
    NULL::text as create_agreement_text,
    NULL::bytea as create_key_value,
    NULL::text as create_key_hash,
    NULL::text as exercise_choice,
    NULL::bytea as exercise_argument,
    NULL::bytea as exercise_result,
    NULL::INTEGER[] as exercise_actors,
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
    NULL::INTEGER[] as exercise_actors,
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
    NULL::INTEGER[] as create_signatories,
    NULL::INTEGER[] as create_observers,
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
    NULL::INTEGER[] as create_signatories,
    NULL::INTEGER[] as create_observers,
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

SELECT 'Recreating participant_events view: Done.';