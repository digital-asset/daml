ALTER TABLE participant_events_create
    ALTER COLUMN domain_id SET NOT NULL;
ALTER TABLE participant_events_consuming_exercise
    ALTER COLUMN domain_id SET NOT NULL;
ALTER TABLE participant_events_non_consuming_exercise
    ALTER COLUMN domain_id SET NOT NULL;
ALTER TABLE participant_command_completions
    ALTER COLUMN domain_id SET NOT NULL;
