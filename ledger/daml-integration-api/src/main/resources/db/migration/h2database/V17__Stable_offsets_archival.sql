-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V14: Stable offsets archival
--
-- create_consumed_at is stored as byte array
---------------------------------------------------------------------------------------------------

set referential_integrity false;

ALTER TABLE participant_events ALTER COLUMN create_consumed_at TYPE BINARY;

set referential_integrity true;
