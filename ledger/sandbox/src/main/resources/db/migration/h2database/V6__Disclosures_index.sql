-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V6: Disclosures index
--
-- This schema version adds an index to the disclosures table

CREATE INDEX idx_disclosures_transaction_id
    ON disclosures (transaction_id);
