
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


--- Tree transactions

CREATE TABLE pe_create_filter_nonstakeholder_informees (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_create_filter_nonstakeholder_informees_ps_idx ON pe_create_filter_nonstakeholder_informees(party_id, event_sequential_id);
CREATE INDEX pe_create_filter_nonstakeholder_informees_s_idx ON pe_create_filter_nonstakeholder_informees(event_sequential_id);


CREATE TABLE pe_consuming_exercise_filter_nonstakeholder_informees (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_consuming_exercise_filter_nonstakeholder_informees_ps_idx ON pe_consuming_exercise_filter_nonstakeholder_informees(party_id, event_sequential_id);
CREATE INDEX pe_consuming_exercise_filter_nonstakeholder_informees_s_idx ON pe_consuming_exercise_filter_nonstakeholder_informees(event_sequential_id);


CREATE TABLE pe_non_consuming_exercise_filter_informees (
   event_sequential_id BIGINT NOT NULL,
   party_id INTEGER NOT NULL
);
CREATE INDEX pe_non_consuming_exercise_filter_informees_ps_idx ON pe_non_consuming_exercise_filter_informees(party_id, event_sequential_id);
CREATE INDEX pe_non_consuming_exercise_filter_informees_s_idx ON pe_non_consuming_exercise_filter_informees(event_sequential_id);


-- Point-wise lookup

CREATE TABLE participant_transaction_meta(
    transaction_id TEXT NOT NULL,
    event_offset TEXT NOT NULL,
    event_sequential_id_from BIGINT NOT NULL,
    event_sequential_id_to BIGINT NOT NULL
);
CREATE INDEX participant_transaction_meta_tid_idx ON participant_transaction_meta(transaction_id);
CREATE INDEX participant_transaction_meta_eventoffset_idx ON participant_transaction_meta(event_offset);

DROP INDEX participant_events_create_transaction_id_idx;
DROP INDEX participant_events_consuming_exercise_transaction_id_idx;
DROP INDEX participant_events_non_consuming_exercise_transaction_id_idx;