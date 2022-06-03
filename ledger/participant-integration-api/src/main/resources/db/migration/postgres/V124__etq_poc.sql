
-- Completions

DROP INDEX participant_command_completion_offset_application_idx;
CREATE INDEX participant_command_completions_application_id_offset_idx ON participant_command_completions USING btree (application_id, completion_offset);

-- Flat transactions


CREATE TABLE pe_consuming_exercise_filter_stakeholders (
   event_sequential_id BIGINT NOT NULL,
   template_id INTEGER NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_pts_idx ON pe_consuming_exercise_filter_stakeholders(party_id, template_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_ps_idx  ON pe_consuming_exercise_filter_stakeholders(party_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_stakeholders_s_idx   ON pe_consuming_exercise_filter_stakeholders(event_sequential_id);