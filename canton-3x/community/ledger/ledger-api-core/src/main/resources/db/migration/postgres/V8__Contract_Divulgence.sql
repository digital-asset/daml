-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V8: Contract divulgence
--
-- This schema version splits the contracts table into:
--   contracts_data, only holding contract data
--   contracts, only holding contract metadata
--
-- This is done because for divulged contracts, we only know the contract data,
-- but no other metadata.
---------------------------------------------------------------------------------------------------

-- Move the `contract` column (the serialized contract data) from contracts to contract_data.
CREATE TABLE contract_data (
  id             varchar primary key not null,
  -- the serialized contract value, using the definition in
  -- `daml-lf/transaction/src/main/protobuf/com/digitalasset/daml/lf/value.proto`
  -- and the encoder in `ContractSerializer.scala`.
  contract       bytea               not null
);
INSERT INTO contract_data (id, contract) SELECT id, contract FROM contracts;
ALTER TABLE contracts DROP COLUMN contract;
