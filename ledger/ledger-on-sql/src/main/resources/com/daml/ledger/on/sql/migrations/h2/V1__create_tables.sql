-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ledger_log
(
    sequence_no IDENTITY PRIMARY KEY NOT NULL,
    entry_id    VARBINARY(16384)     NOT NULL,
    envelope    BLOB                 NOT NULL
);

CREATE TABLE ledger_state
(
    key   VARBINARY(16384) PRIMARY KEY NOT NULL,
    value BLOB                         NOT NULL
);
