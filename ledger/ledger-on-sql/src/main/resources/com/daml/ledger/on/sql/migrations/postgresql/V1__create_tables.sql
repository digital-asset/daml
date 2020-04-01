-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ${table.prefix}meta
(
    -- By explicitly using a value here, we ensure we only ever have one row in this table.
    -- An attempt to write a second row will result in a key conflict.
    table_key INTEGER DEFAULT 0 NOT NULL PRIMARY KEY,
    ledger_id TEXT              NOT NULL
);

CREATE TABLE ${table.prefix}log
(
    sequence_no         SERIAL PRIMARY KEY,
    entry_id            BYTEA,
    envelope            BYTEA,
    heartbeat_timestamp BIGINT,
    CONSTRAINT record_or_timestamp CHECK (
            (entry_id IS NOT NULL AND envelope IS NOT NULL AND heartbeat_timestamp IS NULL)
            OR (entry_id IS NULL AND envelope IS NULL AND heartbeat_timestamp IS NOT NULL)
        )
);

CREATE TABLE ${table.prefix}state
(
    key   BYTEA PRIMARY KEY NOT NULL,
    value BYTEA             NOT NULL
);
