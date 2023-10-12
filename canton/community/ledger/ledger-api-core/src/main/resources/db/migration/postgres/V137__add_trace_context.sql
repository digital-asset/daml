alter table participant_command_completions add column trace_context bytea;
alter table participant_events_assign add column trace_context bytea;
alter table participant_events_consuming_exercise add column trace_context bytea;
alter table participant_events_create add column trace_context bytea;
alter table participant_events_non_consuming_exercise add column trace_context bytea;
alter table participant_events_unassign add column trace_context bytea;
