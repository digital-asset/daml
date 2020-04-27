-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V22: Stable Offsets
--
-- The removal of maximum record time (MRT) also removed the "TimedOut" rejection reason.
-- For historical data, it is replaced by the similar "InvalidLedgerTime" rejection reason.
---------------------------------------------------------------------------------------------------

UPDATE ledger_entries
SET rejection_type = 'InvalidLedgerTime'
WHERE rejection_type = 'TimedOut'
