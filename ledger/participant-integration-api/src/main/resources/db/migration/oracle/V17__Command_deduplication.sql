-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V16: New command deduplication
--
-- Command deduplication has moved from ledger to participant
---------------------------------------------------------------------------------------------------

CREATE TABLE participant_command_submissions
(
    -- The deduplication key
    deduplication_key NVARCHAR2(1000) primary key not null,
    -- The time the command will stop being deduplicated
    deduplicate_until timestamp                   not null
);