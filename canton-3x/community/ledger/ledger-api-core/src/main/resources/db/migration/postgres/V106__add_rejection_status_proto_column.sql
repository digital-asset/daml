--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- Adds a column to store extra details for the rejection status.
--
-- The `rejection_status_details` column contains a Protocol-Buffers-serialized message of type
-- `daml.platform.index.StatusDetails`, containing the code, message, and further details
-- (decided by the ledger driver), and may be `NULL` even if the other two columns are set.

ALTER TABLE participant_command_completions
    RENAME COLUMN status_code TO rejection_status_code;
ALTER TABLE participant_command_completions
    RENAME COLUMN status_message TO rejection_status_message;
ALTER TABLE participant_command_completions
    ADD COLUMN rejection_status_details BYTEA;
