--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

SELECT 'Template Id interning data migration - Preparation...';

-- add temporary index to aid string lookups as migrating data
CREATE UNIQUE INDEX string_interning_external_string_temp_idx ON string_interning USING btree (external_string);

-- drop participant_events view (enables column type changes)
DROP VIEW participant_events;

-- add temporary migration function
CREATE FUNCTION internalize_string(prefix TEXT, string TEXT)
RETURNS INTEGER
LANGUAGE plpgsql
AS
$$
DECLARE
   result INTEGER;
BEGIN
   EXECUTE
   'SELECT CASE' ||
   '  WHEN $2 IS NULL THEN NULL' ||
   '  ELSE (' ||
   '    SELECT string_interning.internal_id' ||
   '      FROM string_interning' ||
   '      WHERE $1 || $2 = string_interning.external_string' ||
   '  )' ||
   'END'
   INTO result
   USING prefix, string;
   ASSERT
     (string IS NULL AND result IS NULL) OR (string IS NOT NULL AND result IS NOT NULL),
     'Could not internalize the string: ' || string;
   RETURN result;
END
$$;


SELECT 'Template Id interning data migration - Migrating participant_events_divulgence...';

DROP INDEX participant_events_divulgence_template_id_idx;

ALTER TABLE participant_events_divulgence
  ALTER COLUMN template_id TYPE INTEGER
  USING internalize_string('t|', template_id);



SELECT 'Template Id interning data migration - Migrating participant_events_create...';

DROP INDEX participant_events_create_template_id_idx;

ALTER TABLE participant_events_create
  ALTER COLUMN template_id TYPE INTEGER
  USING internalize_string('t|', template_id);



SELECT 'Template Id interning data migration - Migrating participant_events_consuming_exercise...';

DROP INDEX participant_events_consuming_exercise_template_id_idx;

ALTER TABLE participant_events_consuming_exercise
  ALTER COLUMN template_id TYPE INTEGER
  USING internalize_string('t|', template_id);



SELECT 'Template Id interning data migration - Migrating participant_events_non_consuming_exercise...';

DROP INDEX participant_events_non_consuming_exercise_template_id_idx;

ALTER TABLE participant_events_non_consuming_exercise
  ALTER COLUMN template_id TYPE INTEGER
  USING internalize_string('t|', template_id);



SELECT 'Template Id interning data migration - Cleanup...';

DROP INDEX string_interning_external_string_temp_idx;
DROP FUNCTION internalize_string(TEXT, TEXT);

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
FROM participant_events_non_consuming_exercise;



SELECT 'Template Id interning data migration - Done';
