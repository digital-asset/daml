-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V12: package_upload_entries
--
-- This schema version adds a table for tracking DAML-LF package submissions
-- It includes id to track the package submission and status
---------------------------------------------------------------------------------------------------

CREATE TABLE package_upload_entries
(
    ledger_offset    bigint primary key not null,
    recorded_at      timestamp          not null, --with timezone
    -- SubmissionId for package to be uploaded
    submission_id    varchar            not null,
    -- participant id that initiated the package upload
    participant_id   varchar            not null,
    -- The type of entry, one of 'accept' or 'reject'
    typ              varchar            not null,
    -- If the type is 'rejection', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar,

    constraint check_entry_type
        check (
                (typ = 'accept' and rejection_reason is null) or
                (typ = 'reject' and rejection_reason is not null)
            )
);

-- Index for retrieving the package upload entry by submission id per participant
CREATE UNIQUE INDEX idx_package_upload_entries
    ON package_upload_entries (submission_id, participant_id)

