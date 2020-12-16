-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table participant_events add column submitters array;
update participant_events set submitters = array[submitter];
alter table participant_events drop column submitter;

-- Replace the participant_command_completions table created in V11__Create_command_completions_table.sql
-- where the index on participant_command_completions(completion_offset, application_id, submitting_party) was not named
-- thus motivating the replacement of the entire table here.
alter table participant_command_completions rename to participant_command_completions_to_be_dropped;
create table participant_command_completions (
  completion_offset binary not null,
  record_time timestamp not null,

  application_id varchar not null,
  submitters array,
  command_id varchar not null,

  transaction_id varchar, -- null if the command was rejected and checkpoints
  status_code integer, -- null for successful command and checkpoints
  status_message varchar -- null for successful command and checkpoints
);

insert into participant_command_completions (completion_offset, record_time, application_id, submitters, command_id, transaction_id, status_code, status_message)
select completion_offset, record_time, application_id, array[submitting_party], command_id, transaction_id, status_code, status_message
from participant_command_completions_to_be_dropped;

create index participant_command_completions_idx on participant_command_completions(completion_offset, application_id, submitters);

drop table participant_command_completions_to_be_dropped;
