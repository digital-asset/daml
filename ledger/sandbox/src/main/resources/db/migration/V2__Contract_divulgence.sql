-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V2: Contract divulgence
--
-- This schema version adds a table for tracking contract divulgence.
-- This is required for making sure contracts can only be fetched by parties that see the contract.
---------------------------------------------------------------------------------------------------



CREATE TABLE contract_divulgences (
  contract_id varchar references contracts (id) not null,
  party       varchar                           not null
);
CREATE UNIQUE INDEX contract_divulgences_idx
  ON contract_divulgences (contract_id, party);
