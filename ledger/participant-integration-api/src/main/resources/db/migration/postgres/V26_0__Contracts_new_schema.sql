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

    -- template_id and create_argument need to be nullable to leave this fields empty as part of this migration
    -- a second (java) migration will fill them up
    -- a third (sql) migration will make them non-nullable, as it's supposed to always be populated
    template_id varchar,
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
    contract_data.id as contract_id,
    null as template_id, -- filled up in a subsequent migration
    null as create_argument, -- filled up in a subsequent migration
    array_agg(contract_observers.observer) filter (where contract_observers.observer is not null) || array_agg(contract_signatories.signatory) filter (where contract_signatories.signatory is not null) as create_stakeholders,
    decode(contract_keys.value_hash, 'hex') as create_key_hash,
    ledger_entries.effective_at as create_ledger_effective_time
from contract_data
left join contracts on contracts.id = contract_data.id
left join contract_keys on contract_keys.contract_id = contract_data.id
left join ledger_entries on ledger_entries.transaction_id = contracts.transaction_id
left join contract_observers on contract_observers.contract_id = contract_data.id
left join contract_signatories on contract_signatories.contract_id = contract_data.id
group by (
    contract_data.id,
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

insert into participant_contract_witnesses
(
select contract_id, witness as contract_witness
from contract_witnesses
join contracts on contracts.id = contract_witnesses.contract_id and contracts.archive_offset is null
union
select contract_id, party as contract_witness
from contract_divulgences
);
