-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

alter table participant_events
    alter column submitter type varchar array using array[submitter];
alter table participant_events
    rename column submitter to submitters;

alter table participant_command_completions
    alter column submitting_party type varchar array using array[submitting_party];
alter table participant_command_completions
    rename column submitting_party to submitters;
