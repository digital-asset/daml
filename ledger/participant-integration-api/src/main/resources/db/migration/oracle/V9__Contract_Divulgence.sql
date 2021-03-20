-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V9: Contract divulgence
--
-- This schema version builds on V2 and V8 by modifying the contract_divulgences.contract_id foreign key
-- to point to contract_data.id (rather than contracts.id previously). Because there is no way to alter a foreign key
-- constraint or to drop and add an unnamed constraint, the script rebuilds the contract_divulgences table.

-- dropped by V30__

