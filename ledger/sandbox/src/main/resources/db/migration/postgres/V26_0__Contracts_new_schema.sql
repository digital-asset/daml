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
    contract_id varchar primary key not null,
    template_id varchar not null,
    -- create_argument needs to be nullable to leave this field empty as part of this migration
    -- a second (java) migration will fill it up
    -- a third (sql) migration will make this non-nullable, as it's supposed to always be populated
    create_argument bytea,

    -- the following fields are null for divulged contracts
    create_stakeholders varchar array,
    create_key_hash bytea,
    create_ledger_effective_time timestamp
);

-- support looking up a contract by key
create unique index on participant_contracts(create_key_hash);

insert into participant_contracts
select
    contracts.id as contract_id,
    contracts.package_id || ':' || contracts.name as template_id,
    null as create_argument, -- filled up in a subsequent migration
    array_agg(contract_observers.observer) || array_agg(contract_signatories.signatory) as create_stakeholders,
    decode(contract_keys.value_hash, 'hex') as create_key_hash,
    ledger_entries.effective_at as create_ledger_effective_time
from contracts
join contract_data on contract_data.id = contracts.id
left join contract_keys on contract_keys.contract_id = contracts.id
left join ledger_entries on ledger_entries.transaction_id = contracts.transaction_id
left join contract_observers on contract_observers.contract_id = contracts.id
left join contract_signatories on contract_signatories.contract_id = contracts.id
group by (
    contracts.id,
    template_id,
    create_key_hash,
    create_ledger_effective_time
);

-- visibility of contracts to parties
create table participant_contract_witnesses
(
    contract_id varchar not null,
    contract_witness varchar not null,

    primary key (contract_id, contract_witness),
    foreign key (contract_id) references participant_contracts(contract_id)
);

insert into participant_contract_witnesses select contract_id, party as contract_witness from contract_divulgences;