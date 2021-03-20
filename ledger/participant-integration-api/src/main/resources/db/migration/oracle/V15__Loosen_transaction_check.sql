-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- Loosen the existing constraint on transactions in ledger_entries
-- to allow persistence without the optional submitter info (submitter, applicationId, commandId).
-- This will occur if the submitter is not hosted by this participant node.
-- The constraint requires either all of the submitter info or none of it to be set.

-- dropped by V30__

