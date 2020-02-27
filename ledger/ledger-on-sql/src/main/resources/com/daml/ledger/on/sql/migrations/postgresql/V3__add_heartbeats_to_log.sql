-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

ALTER TABLE ${table.prefix}log
    ALTER COLUMN entry_id DROP NOT NULL,
    ALTER COLUMN envelope DROP NOT NULL,
    ADD COLUMN heartbeat_timestamp INTEGER,
    ADD CONSTRAINT record_or_timestamp CHECK (
            (entry_id IS NOT NULL AND envelope IS NOT NULL AND heartbeat_timestamp IS NULL)
            OR (entry_id IS NULL AND envelope IS NULL AND heartbeat_timestamp IS NOT NULL)
        );
