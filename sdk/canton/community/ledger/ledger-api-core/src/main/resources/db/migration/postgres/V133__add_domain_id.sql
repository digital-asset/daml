ALTER TABLE participant_command_completions ADD COLUMN domain_id INTEGER;
ALTER TABLE participant_events_divulgence ADD COLUMN domain_id INTEGER;
ALTER TABLE participant_events_create ADD COLUMN domain_id INTEGER;
ALTER TABLE participant_events_consuming_exercise ADD COLUMN domain_id INTEGER;
ALTER TABLE participant_events_non_consuming_exercise ADD COLUMN domain_id INTEGER;
