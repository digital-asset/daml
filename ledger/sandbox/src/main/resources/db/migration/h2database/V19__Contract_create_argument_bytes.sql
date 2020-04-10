-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V19: Contract create argument bytes
--
-- participant_contracts(create_argument) should have been a byte array from the very beginning
---------------------------------------------------------------------------------------------------

set referential_integrity false;

ALTER TABLE participant_contracts ALTER COLUMN create_argument TYPE BINARY;

set referential_integrity true;
