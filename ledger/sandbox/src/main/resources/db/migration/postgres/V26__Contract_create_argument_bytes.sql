-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V26: Contract create argument bytes
--
-- participant_contracts(create_argument) should have been a byte array from the very beginning
---------------------------------------------------------------------------------------------------

ALTER TABLE participant_contracts ALTER COLUMN create_argument TYPE bytea USING create_argument::bytea;
