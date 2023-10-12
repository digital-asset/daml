ALTER TABLE participant_events_unassign
    ALTER COLUMN submitter DROP NOT NULL;

ALTER TABLE participant_events_assign
    ALTER COLUMN submitter DROP NOT NULL;
