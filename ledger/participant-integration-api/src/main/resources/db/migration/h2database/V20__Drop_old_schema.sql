-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V20: Drop old schema
---------------------------------------------------------------------------------------------------

drop table contracts cascade;
drop table contract_data cascade;
drop table contract_divulgences cascade;
drop table contract_keys cascade;
drop table contract_key_maintainers cascade;
drop table contract_observers cascade;
drop table contract_signatories cascade;
drop table contract_witnesses cascade;
drop table disclosures cascade;
drop table ledger_entries cascade;
