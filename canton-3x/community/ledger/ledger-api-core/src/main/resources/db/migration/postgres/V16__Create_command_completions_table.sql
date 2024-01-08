-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE participant_command_completions
(
    completion_offset bigint not null,
    record_time timestamp not null,

    application_id varchar, -- null for checkpoints
    submitting_party varchar, -- null for checkpoints
    command_id varchar, -- null for checkpoints

    transaction_id varchar, -- null if the command was rejected and checkpoints
    status_code integer, -- null for successful command and checkpoints
    status_message varchar -- null for successful command and checkpoints
);

CREATE INDEX ON participant_command_completions(completion_offset, application_id, submitting_party);
