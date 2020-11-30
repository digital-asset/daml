-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table participant_events add column submitters array;
update participant_events set submitters = array[submitter];
alter table participant_events drop column submitter;

alter table participant_command_completions add column submitters array;
update participant_command_completions set submitters = array[submitting_party];
create index participant_command_completions_idx on participant_command_completions(completion_offset, application_id, submitters);
drop index PUBLIC.INDEX_2;
alter table participant_command_completions drop column submitting_party;
