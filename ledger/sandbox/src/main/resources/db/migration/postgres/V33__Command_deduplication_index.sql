-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V33: Index for participant_command_submissions
--
-- The index covers both columns
---------------------------------------------------------------------------------------------------


CREATE INDEX participant_command_submissions_idx ON participant_command_submissions (deduplication_key, deduplicate_until);
