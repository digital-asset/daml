-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V14: Stable Offsets
--
-- Stable offsets are stored as binary and can be sorted lexicographically.
---------------------------------------------------------------------------------------------------
ALTER TABLE parameters ALTER COLUMN ledger_end DROP NOT NULL;

set referential_integrity false;

ALTER TABLE configuration_entries ALTER COLUMN ledger_offset TYPE BINARY;
ALTER TABLE contract_divulgences ALTER COLUMN ledger_offset TYPE BINARY;
ALTER TABLE contracts ALTER COLUMN create_offset TYPE BINARY;
ALTER TABLE contracts ALTER COLUMN archive_offset TYPE BINARY;
ALTER TABLE ledger_entries ALTER COLUMN ledger_offset TYPE BINARY;
ALTER TABLE packages ALTER COLUMN ledger_offset TYPE BINARY;
ALTER TABLE package_entries ALTER COLUMN ledger_offset TYPE BINARY;
ALTER TABLE parameters ALTER COLUMN ledger_end TYPE BINARY;
ALTER TABLE participant_command_completions ALTER COLUMN completion_offset TYPE BINARY;
ALTER TABLE participant_events ALTER COLUMN event_offset TYPE BINARY;
ALTER TABLE parties ALTER COLUMN ledger_offset TYPE BINARY;
ALTER TABLE party_entries ALTER COLUMN ledger_offset TYPE BINARY;

set referential_integrity true;
