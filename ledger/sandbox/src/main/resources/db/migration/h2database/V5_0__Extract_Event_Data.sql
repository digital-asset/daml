-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V5.0: Extract event data
--
-- This schema version adds the tables contract_signatories and contract_observers, and the column contracts_create_event_id
-- to store event related data so that it can be easily retrieved later.

ALTER TABLE contracts ADD create_event_id varchar;


CREATE TABLE contract_signatories (
  contract_id varchar not null,
  signatory   varchar not null,
  foreign key (contract_id) references contracts (id)
);
CREATE UNIQUE INDEX contract_signatories_idx
  ON contract_signatories (contract_id, signatory);


CREATE TABLE contract_observers (
  contract_id varchar not null,
  observer   varchar    not null,
  foreign key (contract_id) references contracts (id)
);
CREATE UNIQUE INDEX contract_observer_idx
  ON contract_observers (contract_id, observer);
