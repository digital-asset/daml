--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- Store the completion status as a serialized Protocol Buffers message of type `google.rpc.Status`.
--
-- The `rejection_status` contains a Protocol-Buffers-serialized message of type
-- `google.rpc.Status`, containing the code, message, and further details (decided by the ledger
-- driver).
--
-- We only rename the `rejection_status_code` and `rejection_status_message` columns, and so they
-- may contain historical data. Readers of this table will therefore need to query for all three
-- columns and either use the `google.rpc.Status` message in `rejection_status` if it exists, or
-- construct one from the `rejection_status_code` and `rejection_status_message` if it does not.

ALTER TABLE participant_command_completions
    RENAME COLUMN status_code TO rejection_status_code;
ALTER TABLE participant_command_completions
    RENAME COLUMN status_message TO rejection_status_message;
ALTER TABLE participant_command_completions
    ADD COLUMN rejection_status BYTEA;
