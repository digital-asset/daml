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

CREATE OR REPLACE TRIGGER participant_users_created_at_trigger
    BEFORE INSERT ON participant_users
    FOR EACH ROW
BEGIN
    SELECT
        days * 24 * 3600 * 1000 * 1000 +
        hours * 3600 * 1000 * 1000 +
        minutes * 60 * 1000 * 1000 +
        seconds * 1000 * 1000
    INTO :new.created_at
    FROM
        (
        SELECT
            EXTRACT(day FROM diff) days,
            EXTRACT(hour FROM diff) hours,
            EXTRACT(minute FROM diff) minutes,
            EXTRACT(second FROM diff) seconds
        FROM
            (
            SELECT sys_extract_utc(current_timestamp) - to_timestamp('1-1-1970 00:00:00','MM-DD-YYYY HH24:Mi:SS') as diff FROM dual
            )
        );
END;
/

CREATE OR REPLACE TRIGGER participant_user_rights_granted_at_trigger
    BEFORE INSERT ON participant_user_rights
    FOR EACH ROW
BEGIN
    SELECT
        days * 24 * 3600 * 1000 * 1000 +
        hours * 3600 * 1000 * 1000 +
        minutes * 60 * 1000 * 1000 +
        seconds * 1000 * 1000
    INTO :new.granted_at
    FROM
        (
        SELECT
            EXTRACT(day FROM diff) days,
            EXTRACT(hour FROM diff) hours,
            EXTRACT(minute FROM diff) minutes,
            EXTRACT(second FROM diff) seconds
        FROM
            (
            SELECT sys_extract_utc(current_timestamp) - to_timestamp('1-1-1970 00:00:00','MM-DD-YYYY HH24:Mi:SS') as diff FROM dual
            )
        );
END;
/


INSERT INTO participant_users(user_id, primary_party) VALUES ('participant_admin', NULL);
INSERT INTO participant_user_rights(user_internal_id, user_right, for_party)
    SELECT internal_id, 1, NULL
    FROM participant_users
    WHERE user_id = 'participant_admin';

