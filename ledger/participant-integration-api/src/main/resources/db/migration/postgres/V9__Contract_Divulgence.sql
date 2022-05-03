-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V9: Contract divulgence
--
-- This schema version builds on V2 and V8 by modifying the contract_divulgences.contract_id foreign key
-- to point to contract_data.id (rather than contracts.id previously). Because there is no way to alter a foreign key
-- constraint or to drop and add an unnamed constraint, the script rebuilds the contract_divulgences table.

ALTER TABLE contract_divulgences RENAME TO contract_divulgences_to_be_dropped;
ALTER TABLE contract_divulgences_to_be_dropped RENAME CONSTRAINT contract_divulgences_idx TO contract_divulgences_idx_to_be_dropped;

CREATE TABLE contract_divulgences (
  contract_id   varchar  not null,
  -- The party to which the given contract was divulged
  party         varchar  not null,
  -- The offset at which the contract was divulged to the given party
  ledger_offset bigint   not null,
  -- The transaction ID at which the contract was divulged to the given party
  transaction_id varchar not null,

  foreign key (contract_id) references contract_data (id), -- refer to contract_data instead, the reason for this script
  foreign key (ledger_offset) references ledger_entries (ledger_offset),
  foreign key (transaction_id) references ledger_entries (transaction_id),

  CONSTRAINT contract_divulgences_idx UNIQUE(contract_id, party)
);

INSERT INTO contract_divulgences (contract_id, party, ledger_offset, transaction_id)
SELECT contract_id, party, ledger_offset, transaction_id
FROM contract_divulgences_to_be_dropped;

DROP TABLE contract_divulgences_to_be_dropped;
