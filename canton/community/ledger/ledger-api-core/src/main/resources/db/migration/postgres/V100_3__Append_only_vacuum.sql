-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


---------------------------------------------------------------------------------------------------
-- V100.3: Vacuum
--
-- This is a maintenance task run after the big migration to the append-only schema.
-- It is run in a separate migration because Flyway does not allow mixing transactional and
-- non-transactional statements in a single migration script.
---------------------------------------------------------------------------------------------------

VACUUM ANALYZE;
