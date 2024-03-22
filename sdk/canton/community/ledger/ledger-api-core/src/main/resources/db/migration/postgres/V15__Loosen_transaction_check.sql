-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Loosen the existing constraint on transactions in ledger_entries
-- to allow persistence without the optional submitter info (submitter, applicationId, commandId).
-- This will occur if the submitter is not hosted by this participant node.
-- The constraint requires either all of the submitter info or none of it to be set.
ALTER TABLE ledger_entries DROP CONSTRAINT check_entry;
ALTER TABLE ledger_entries ADD CONSTRAINT check_entry CHECK (
    (typ = 'transaction' and transaction_id is not null and effective_at is not null and transaction is not null and (
      (submitter is null and application_id is null and command_id is null) or
      (submitter is not null and application_id is not null and command_id is not null)
    )) or
    (typ = 'rejection' and command_id is not null and application_id is not null and submitter is not null and
     rejection_type is not null and rejection_description is not null) or
    (typ = 'checkpoint')
);
