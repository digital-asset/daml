-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V13: Stable Offsets
--
-- Stable offsets are stored as varchar and can be sorted lexicographically.
---------------------------------------------------------------------------------------------------
ALTER TABLE parameters ALTER COLUMN ledger_end DROP NOT NULL;

set referential_integrity false;

ALTER TABLE configuration_entries ALTER COLUMN ledger_offset TYPE VARCHAR;
ALTER TABLE contract_divulgences ALTER COLUMN ledger_offset TYPE VARCHAR;
ALTER TABLE contracts ALTER COLUMN create_offset TYPE VARCHAR;
ALTER TABLE contracts ALTER COLUMN archive_offset TYPE VARCHAR;
ALTER TABLE ledger_entries ALTER COLUMN ledger_offset TYPE VARCHAR;
ALTER TABLE packages ALTER COLUMN ledger_offset TYPE VARCHAR;
ALTER TABLE package_entries ALTER COLUMN ledger_offset TYPE VARCHAR;
ALTER TABLE parameters ALTER COLUMN ledger_end TYPE VARCHAR;
ALTER TABLE participant_command_completions ALTER COLUMN completion_offset TYPE VARCHAR;
ALTER TABLE parties ALTER COLUMN ledger_offset TYPE VARCHAR;
ALTER TABLE party_entries ALTER COLUMN ledger_offset TYPE VARCHAR;

set referential_integrity true;