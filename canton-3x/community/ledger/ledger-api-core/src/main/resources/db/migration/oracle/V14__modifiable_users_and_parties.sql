--  Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

-- NOTE: We keep participant user and party record tables independent from indexer-based tables, such that
--       we maintain a property that they can be moved to a separate database without any extra schema changes.

-- User tables
CREATE TABLE participant_user_annotations (
    internal_id         NUMBER                NOT NULL REFERENCES participant_users (internal_id) ON DELETE CASCADE,
    name                VARCHAR2(512 CHAR)    NOT NULL,
    val                 CLOB,
    updated_at          NUMBER                NOT NULL,
    UNIQUE (internal_id, name)
);
ALTER TABLE participant_users ADD is_deactivated     NUMBER    DEFAULT 0 NOT NULL;
ALTER TABLE participant_users ADD resource_version   NUMBER    DEFAULT 0 NOT NULL;

-- Party record tables
CREATE TABLE participant_party_records (
    internal_id         NUMBER                GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    party               VARCHAR2(512 CHAR)    NOT NULL UNIQUE,
    resource_version    NUMBER                NOT NULL,
    created_at          NUMBER                NOT NULL
);
CREATE TABLE participant_party_record_annotations (
    internal_id         NUMBER                NOT NULL REFERENCES participant_party_records (internal_id) ON DELETE CASCADE,
    name                VARCHAR2(512 CHAR)    NOT NULL,
    val                 CLOB,
    updated_at          NUMBER                NOT NULL,
    UNIQUE (internal_id, name)
);
