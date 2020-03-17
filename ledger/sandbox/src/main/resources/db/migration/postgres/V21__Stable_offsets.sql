-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V21: Stable Offsets
--
-- Stable offsets are stored as bytea and can be sorted lexicographically.
---------------------------------------------------------------------------------------------------

ALTER TABLE parameters ALTER COLUMN ledger_end DROP NOT NULL;

ALTER TABLE contract_divulgences DROP CONSTRAINT contract_divulgences_ledger_offset_fkey1;
ALTER TABLE contracts DROP CONSTRAINT contracts_create_offset_fkey;
ALTER TABLE contracts DROP CONSTRAINT contracts_archive_offset_fkey;

ALTER TABLE configuration_entries ALTER COLUMN ledger_offset TYPE bytea using null;
ALTER TABLE contract_divulgences ALTER COLUMN ledger_offset TYPE bytea using null;
ALTER TABLE contracts ALTER COLUMN create_offset TYPE bytea using null;
ALTER TABLE contracts ALTER COLUMN archive_offset TYPE bytea using null;
ALTER TABLE ledger_entries ALTER COLUMN ledger_offset TYPE bytea using null;
ALTER TABLE packages ALTER COLUMN ledger_offset TYPE bytea using null;
ALTER TABLE package_entries ALTER COLUMN ledger_offset TYPE bytea using null;
ALTER TABLE parameters ALTER COLUMN ledger_end TYPE bytea using null;
ALTER TABLE participant_command_completions ALTER COLUMN completion_offset TYPE bytea using null;
ALTER TABLE participant_events ALTER COLUMN event_offset TYPE bytea using null;
ALTER TABLE parties ALTER COLUMN ledger_offset TYPE bytea using null;
ALTER TABLE party_entries ALTER COLUMN ledger_offset TYPE bytea using null;

ALTER TABLE contract_divulgences add foreign key (ledger_offset) references ledger_entries (ledger_offset);
ALTER TABLE contracts add foreign key (create_offset) references ledger_entries (ledger_offset);
ALTER TABLE contracts add foreign key (archive_offset) references ledger_entries (ledger_offset);
