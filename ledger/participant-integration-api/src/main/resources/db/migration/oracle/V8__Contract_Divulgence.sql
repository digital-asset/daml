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

-- dropped by V30___

