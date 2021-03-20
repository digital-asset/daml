-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V26: Contracts new schema
--
-- Used for interpretation and validation by the DAML engine
---------------------------------------------------------------------------------------------------

-- contains all active and divulged contracts
create table participant_contracts
(
    contract_id                  NVARCHAR2(1000) primary key not null,

    -- template_id and create_argument need to be nullable to leave this fields empty as part of this migration
    -- a second (java) migration will fill them up
    -- a third (sql) migration will make them non-nullable, as it's supposed to always be populated
    template_id                  NVARCHAR2(1000)             not null,
    create_argument              BLOB                        not null,

    -- the following fields are null for divulged contracts
    create_stakeholders          VARCHAR_ARRAY,
    --TODO BH: binary (blob) field cannot be part of the index, creating a hex representation of the blob for the index
    create_key_hash_hex          VARCHAR2(4000),
    create_key_hash              BLOB,
    create_ledger_effective_time TIMESTAMP,
    create_argument_compression  SMALLINT
);

-- support looking up a contract by key
-- TODO BH: consider whether ORA_HASH() could help
create unique index participant_contracts_idx on participant_contracts (create_key_hash_hex);


-- visibility of contracts to parties
create table participant_contract_witnesses
(
    contract_id      NVARCHAR2(1000) not null,
    contract_witness NVARCHAR2(1000) not null,

    primary key (contract_id, contract_witness),
    foreign key (contract_id) references participant_contracts (contract_id)
);

