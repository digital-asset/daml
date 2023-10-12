alter table participant_command_completions add trace_context BLOB;
alter table participant_events_assign add trace_context BLOB;
alter table participant_events_consuming_exercise add trace_context BLOB;
alter table participant_events_create add trace_context BLOB;
alter table participant_events_non_consuming_exercise add trace_context BLOB;
alter table participant_events_unassign add trace_context BLOB;
