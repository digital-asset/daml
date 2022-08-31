--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- NOTE: We keep participant user and party record tables independent from indexer-based tables, such that
--       we maintain a property that they can be moved to a separate database without any extra schema changes.

-- User tables
CREATE TABLE participant_user_annotations (
    internal_id         INTEGER             NOT NULL REFERENCES participant_users (internal_id) ON DELETE CASCADE,
    name                VARCHAR(512)        NOT NULL,
    val                 TEXT,
    updated_at          BIGINT              NOT NULL,
    UNIQUE (internal_id, name)
);
ALTER TABLE participant_users ADD COLUMN is_deactivated     BOOLEAN   NOT NULL DEFAULT FALSE;
ALTER TABLE participant_users ADD COLUMN resource_version   BIGINT    NOT NULL DEFAULT 0;

-- Party record tables
CREATE TABLE participant_party_records (
    internal_id         SERIAL              PRIMARY KEY,
    party               VARCHAR(512)        NOT NULL UNIQUE COLLATE "C",
    resource_version    BIGINT              NOT NULL,
    created_at          BIGINT              NOT NULL
);
CREATE TABLE participant_party_record_annotations (
    internal_id         INTEGER             NOT NULL REFERENCES participant_party_records (internal_id) ON DELETE CASCADE,
    name                VARCHAR(512)        NOT NULL,
    val                 TEXT,
    updated_at          BIGINT              NOT NULL,
    UNIQUE (internal_id, name)
);
