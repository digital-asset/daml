--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V111: Convert all TIMESTAMP columns to BIGINT to solve issues with time zones
---------------------------------------------------------------------------------------------------

ALTER TABLE configuration_entries
    ALTER COLUMN recorded_at
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM recorded_at) * 1000000;

ALTER TABLE packages
    ALTER COLUMN known_since
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM known_since) * 1000000;

ALTER TABLE package_entries
    ALTER COLUMN recorded_at
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM recorded_at) * 1000000;

ALTER TABLE party_entries
    ALTER COLUMN recorded_at
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM recorded_at) * 1000000;

ALTER TABLE participant_command_submissions
    ALTER COLUMN deduplicate_until
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM deduplicate_until) * 1000000;

ALTER TABLE participant_command_completions
    ALTER COLUMN record_time
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM record_time) * 1000000;

ALTER TABLE participant_command_completions
    ALTER COLUMN deduplication_start
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM deduplication_start) * 1000000;



DROP VIEW participant_events;

ALTER TABLE participant_events_create
    ALTER COLUMN ledger_effective_time
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM ledger_effective_time) * 1000000;

ALTER TABLE participant_events_consuming_exercise
    ALTER COLUMN ledger_effective_time
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM ledger_effective_time) * 1000000;

ALTER TABLE participant_events_non_consuming_exercise
    ALTER COLUMN ledger_effective_time
        SET DATA TYPE BIGINT USING EXTRACT(EPOCH FROM ledger_effective_time) * 1000000;

CREATE VIEW participant_events
AS
SELECT
    0::smallint as event_kind,
    event_sequential_id,
    NULL::text as event_offset,
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
