--  Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
--  SPDX-License-Identifier: Apache-2.0

CREATE TABLE participant_users (
    internal_id         NUMBER          GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    user_id             VARCHAR2(256)   NOT NULL UNIQUE,
    primary_party       VARCHAR2(512),
    created_at          NUMBER          NOT NULL
);

CREATE TABLE participant_user_rights (
    user_internal_id    NUMBER          NOT NULL REFERENCES participant_users (internal_id) ON DELETE CASCADE,
    user_right          NUMBER          NOT NULL,
    for_party           VARCHAR2(512),
    granted_at          NUMBER          NOT NULL,
    UNIQUE (user_internal_id, user_right, for_party)
);

INSERT INTO participant_users(user_id, primary_party, created_at)
VALUES ('participant_admin', NULL, 0);

INSERT INTO participant_user_rights(user_internal_id, user_right, for_party, granted_at)
SELECT internal_id,
       1,
       NULL,
       0
FROM participant_users
WHERE user_id = 'participant_admin';
