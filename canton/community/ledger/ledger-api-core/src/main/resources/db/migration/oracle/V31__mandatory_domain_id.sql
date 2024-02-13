ALTER TABLE participant_events_create MODIFY (domain_id NOT NULL);
ALTER TABLE participant_events_consuming_exercise MODIFY (domain_id NOT NULL);
ALTER TABLE participant_events_non_consuming_exercise MODIFY (domain_id NOT NULL);
ALTER TABLE participant_command_completions MODIFY (domain_id NOT NULL);
