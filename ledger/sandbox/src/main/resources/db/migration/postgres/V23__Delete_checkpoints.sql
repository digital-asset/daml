-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V23: Delete checkpoints
--
-- Checkpoints/heartbeats are no longer stored and previously stored checkpoints
-- can be removed.
---------------------------------------------------------------------------------------------------


-- ledger_entries
delete from ledger_entries where typ = 'checkpoint';

alter table ledger_entries drop constraint check_entry;
alter table ledger_entries add constraint check_entry check (
    (typ = 'transaction' and transaction_id is not null and effective_at is not null and transaction is not null and (
      (submitter is null and application_id is null and command_id is null) or
      (submitter is not null and application_id is not null and command_id is not null)
    )) or
    (typ = 'rejection' and command_id is not null and application_id is not null and submitter is not null and
     rejection_type is not null and rejection_description is not null)
);


-- participant_command_completions
delete from participant_command_completions
where application_id is null and submitting_party is null and command_id is null;

alter table participant_command_completions
alter column application_id set not null;

alter table participant_command_completions
alter column submitting_party set not null;

alter table participant_command_completions
alter column command_id set not null;