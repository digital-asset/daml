--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V110: Add columns to store command deduplication information in completions
---------------------------------------------------------------------------------------------------

ALTER TABLE participant_command_completions
    -- The submission ID will be provided by the participant or driver if the application didn't provide one.
    -- Nullable to support historical data.
    ADD COLUMN submission_id text,
    -- The three alternatives below are mutually exclusive, i.e. the deduplication
    -- interval could have specified by the application as one of:
    -- 1. an initial offset
    -- 2. an initial timestamp
    -- 3. a duration (split into two columns, seconds and nanos, mapping protobuf's 1:1)
    ADD COLUMN deduplication_offset text,
    ADD COLUMN deduplication_duration_seconds bigint,
    ADD COLUMN deduplication_duration_nanos integer,
    ADD COLUMN deduplication_start timestamp;
