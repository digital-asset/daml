-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ${table.prefix}meta
(
    -- By explicitly using a value here, we ensure we only ever have one row in this table.
    -- An attempt to write a second row will result in a key conflict.
    table_key INTEGER DEFAULT 0 NOT NULL PRIMARY KEY,
    ledger_id TEXT NOT NULL
);
