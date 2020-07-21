-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V18: Contracts new schema
--
-- Used for interpretation and validation by the DAML engine
---------------------------------------------------------------------------------------------------

-- contains all active and divulged contracts
create table participant_contracts
(
    contract_id varchar primary key not null,
    template_id varchar not null,
    create_argument bytea not null,

    -- the following fields are null for divulged contracts
    create_stakeholders array,
    create_key_hash bytea,
    create_ledger_effective_time timestamp
);

-- support looking up a contract by key
create unique index on participant_contracts(create_key_hash);

-- visibility of contracts to parties
create table participant_contract_witnesses
(
    contract_id varchar not null,
    contract_witness varchar not null,

    primary key (contract_id, contract_witness),
    foreign key (contract_id) references participant_contracts(contract_id)
);
