alter table participant_command_completions add ( temp clob );
update participant_command_completions set temp=status_message;
alter table participant_command_completions drop column status_message;
alter table participant_command_completions rename column temp to status_message;
