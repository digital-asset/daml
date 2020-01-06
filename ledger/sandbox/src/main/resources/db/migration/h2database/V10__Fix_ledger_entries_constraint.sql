-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- The original constraint used the expression `!= null` to prevent null fields.
-- This fixes the check to use `is not null` (`null != null` returns true in h2).
-- NOTE: This migration will fail if there is existing data in the table not conforming to the corrected constraint.
ALTER TABLE ledger_entries DROP CONSTRAINT check_entry;
ALTER TABLE ledger_entries ADD CONSTRAINT check_entry CHECK (
  (typ = 'transaction' and transaction_id is not null and command_id is not null and application_id is not null and
    submitter is not null and effective_at is not null and transaction is not null) or
  (typ = 'rejection' and command_id is not null and application_id is not null and submitter is not null and
    rejection_type is not null and rejection_description is not null) or
  (typ = 'checkpoint')
);
