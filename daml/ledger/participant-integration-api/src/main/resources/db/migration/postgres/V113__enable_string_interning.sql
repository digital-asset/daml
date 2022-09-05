--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- add string_interning ledger-end tracking column to parameters
ALTER TABLE parameters
  ADD COLUMN ledger_end_string_interning_id INTEGER;

-- create temporary sequence to populate the string_interning table as migrating data
CREATE SEQUENCE string_interning_seq_temp;
-- add temporary index to aid string lookups as migrating data
CREATE UNIQUE INDEX string_interning_external_string_temp_idx ON string_interning USING btree (external_string);

-- add temporary migration function
CREATE FUNCTION insert_to_string_interning(prefix TEXT, table_name TEXT, selector_expr TEXT)
RETURNS void
LANGUAGE plpgsql
AS
$$
BEGIN
   EXECUTE
   'INSERT INTO string_interning(internal_id, external_string) ' ||
   'SELECT nextval(''string_interning_seq_temp''), id ' ||
   'FROM (SELECT DISTINCT ''' || prefix || ''' || ' || selector_expr || ' id FROM ' || table_name || ') distinct_ids ' ||
   'WHERE NOT EXISTS (SELECT 1 FROM string_interning WHERE external_string = distinct_ids.id)' ||
   'AND id IS NOT NULL';
END
$$;

-- data migrations

-- template_id
SELECT insert_to_string_interning('t|', 'participant_events_create', 'template_id');
SELECT insert_to_string_interning('t|', 'participant_events_divulgence', 'template_id');
SELECT insert_to_string_interning('t|', 'participant_events_consuming_exercise', 'template_id');
SELECT insert_to_string_interning('t|', 'participant_events_non_consuming_exercise', 'template_id');

-- party
SELECT insert_to_string_interning('p|', 'participant_events_create', 'unnest(tree_event_witnesses)');
SELECT insert_to_string_interning('p|', 'participant_events_create', 'unnest(flat_event_witnesses)');
SELECT insert_to_string_interning('p|', 'participant_events_create', 'unnest(submitters)');
SELECT insert_to_string_interning('p|', 'participant_events_create', 'unnest(create_observers)');
SELECT insert_to_string_interning('p|', 'participant_events_create', 'unnest(create_signatories)');

SELECT insert_to_string_interning('p|', 'participant_events_divulgence', 'unnest(tree_event_witnesses)');
SELECT insert_to_string_interning('p|', 'participant_events_divulgence', 'unnest(submitters)');

SELECT insert_to_string_interning('p|', 'participant_events_consuming_exercise', 'unnest(tree_event_witnesses)');
SELECT insert_to_string_interning('p|', 'participant_events_consuming_exercise', 'unnest(flat_event_witnesses)');
SELECT insert_to_string_interning('p|', 'participant_events_consuming_exercise', 'unnest(submitters)');
SELECT insert_to_string_interning('p|', 'participant_events_consuming_exercise', 'unnest(exercise_actors)');

SELECT insert_to_string_interning('p|', 'participant_events_non_consuming_exercise', 'unnest(tree_event_witnesses)');
SELECT insert_to_string_interning('p|', 'participant_events_non_consuming_exercise', 'unnest(flat_event_witnesses)');
SELECT insert_to_string_interning('p|', 'participant_events_non_consuming_exercise', 'unnest(submitters)');
SELECT insert_to_string_interning('p|', 'participant_events_non_consuming_exercise', 'unnest(exercise_actors)');

SELECT insert_to_string_interning('p|', 'participant_command_completions', 'unnest(submitters)');

SELECT insert_to_string_interning('p|', 'party_entries', 'party');

-- fill ledger-end
UPDATE parameters
SET ledger_end_string_interning_id = (SELECT coalesce (max(internal_id),0) FROM string_interning);

-- remove temporary SQL objects
DROP SEQUENCE string_interning_seq_temp;
DROP INDEX string_interning_external_string_temp_idx;
DROP FUNCTION insert_to_string_interning(TEXT, TEXT, TEXT);

