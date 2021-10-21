-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V30: Drop old schema
--
-- Also removes checkpoints as part of the data migration, if there are any still lingering around.
--
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

delete from participant_command_completions where application_id is null and submitting_party is null;
