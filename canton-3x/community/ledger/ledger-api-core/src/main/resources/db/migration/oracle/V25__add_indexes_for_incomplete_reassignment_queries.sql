-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX participant_events_unassign_event_offset ON participant_events_unassign (event_offset, event_sequential_id);

-- covering index for queries resolving offsets to sequential IDs. For temporary incomplete reassignments implementation.
CREATE INDEX participant_events_assign_event_offset ON participant_events_assign (event_offset, event_sequential_id);

-- index for queries resolving contract ID to sequential IDs.
CREATE INDEX participant_events_assign_event_contract_id ON participant_events_assign (contract_id);
