-- Copyright (c) 2020 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

CREATE TABLE ${table.prefix}new_log
(
    sequence_no         INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL,
    entry_id            VARBINARY(16384),
    envelope            BLOB,
    heartbeat_timestamp BIGINT
);

INSERT INTO ${table.prefix}new_log (sequence_no, entry_id, envelope)
SELECT *
FROM ${table.prefix}log;

DROP TABLE ${table.prefix}log;

ALTER TABLE ${table.prefix}new_log
    RENAME TO ${table.prefix}log;
