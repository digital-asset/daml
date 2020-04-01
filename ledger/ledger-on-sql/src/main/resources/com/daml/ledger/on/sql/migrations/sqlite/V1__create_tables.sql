-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ${table.prefix}meta
(
    -- By explicitly using a value here, we ensure we only ever have one row in this table.
    -- An attempt to write a second row will result in a key conflict.
    table_key INTEGER DEFAULT 0 NOT NULL PRIMARY KEY,
    ledger_id TEXT NOT NULL
);

CREATE TABLE ${table.prefix}log
(
    sequence_no         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    entry_id            VARBINARY(16384) NOT NULL,
    envelope            BLOB NOT NULL
);

CREATE TABLE ${table.prefix}state
(
    key   VARBINARY(16384) PRIMARY KEY NOT NULL,
    value BLOB                         NOT NULL
);
