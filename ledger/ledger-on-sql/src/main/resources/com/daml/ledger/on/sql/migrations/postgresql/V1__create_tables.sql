-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ledger_log
(
    sequence_no SERIAL PRIMARY KEY,
    entry_id    BYTEA NOT NULL,
    envelope    BYTEA NOT NULL
);

CREATE TABLE ledger_state
(
    key   BYTEA PRIMARY KEY NOT NULL,
    value BYTEA             NOT NULL
);
