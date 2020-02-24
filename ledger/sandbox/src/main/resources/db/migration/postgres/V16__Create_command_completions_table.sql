-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE participant_command_completions
(
    completion_offset bigint not null,
    record_time timestamp not null, -- TODO re-evaluate usefulness after new ledger time lands
    application_id varchar not null,
    submitting_party varchar not null,
    command_id varchar not null,

    transaction_id varchar, -- null if the command was rejected
    status_code integer, -- null for successful command and checkpoints
    status_message varchar -- null for successful command and checkpoints
);

CREATE INDEX ON participant_command_completions(completion_offset, application_id, submitting_party);