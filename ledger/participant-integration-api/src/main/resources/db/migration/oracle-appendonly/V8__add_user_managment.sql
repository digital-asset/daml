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
SELECT 'participant_admin',
        NULL,
        days * 24 * 3600 * 1000 * 1000 +
        hours * 3600 * 1000 * 1000 +
        minutes * 60 * 1000 * 1000 +
        seconds * 1000 * 1000
FROM
    (
    SELECT
        EXTRACT(day FROM diff) days,
        EXTRACT(hour FROM diff) hours,
        EXTRACT(minute FROM diff) minutes,
        EXTRACT(second FROM diff) seconds
    FROM
        (
        SELECT sys_extract_utc(current_timestamp) - to_timestamp('1-1-1970 00:00:00','MM-DD-YYYY HH24:Mi:SS') AS diff FROM dual
        )
    )
;

INSERT INTO participant_user_rights(user_internal_id, user_right, for_party, granted_at)
SELECT internal_id,
       1,
       NULL,
       created_at
FROM participant_users
WHERE user_id = 'participant_admin'
;
