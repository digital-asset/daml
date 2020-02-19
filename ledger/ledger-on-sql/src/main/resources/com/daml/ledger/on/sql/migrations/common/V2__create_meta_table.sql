-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ${table.prefix}meta
(
    table_key INTEGER DEFAULT 0 NOT NULL PRIMARY KEY, -- used to ensure we don't write twice
    ledger_id TEXT NOT NULL
);
