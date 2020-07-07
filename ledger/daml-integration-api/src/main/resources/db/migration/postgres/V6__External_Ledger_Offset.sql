-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V6: External Ledger Offset
--
-- This schema version adds a column to the parameters table that stores the external ledger offset
-- that corresponds to the index ledger end.
---------------------------------------------------------------------------------------------------

ALTER TABLE parameters
ADD external_ledger_end varchar;
