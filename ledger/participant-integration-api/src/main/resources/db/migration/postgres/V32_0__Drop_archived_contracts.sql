-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V32.0: Drop archived contracts (see https://github.com/digital-asset/daml/issues/6017)
--
-- V26.0 mistakenly added data for archived contracts to the participant_contracts table
-- Since that table should _only_ be used for execution and validation it should only
-- contain the most up-to-date state for active contracts.
--
---------------------------------------------------------------------------------------------------

-- To remove the archived contracts we rely on consuming events from the participant_events
-- table and remove from both participant_contract_witnesses and participant_contracts all
-- those rows that are related to contract_ids that have been subject to a consuming event

delete from participant_contract_witnesses where contract_id in (select contract_id from participant_events where exercise_consuming);
delete from participant_contracts where contract_id in (select contract_id from participant_events where exercise_consuming);
