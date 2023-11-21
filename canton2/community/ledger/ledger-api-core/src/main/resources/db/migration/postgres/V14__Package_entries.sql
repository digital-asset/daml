-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V14: package_entries
--
-- This schema version adds a table for tracking DAML-LF package submissions
-- It includes id to track the package submission and status
---------------------------------------------------------------------------------------------------

CREATE TABLE package_entries
(
    ledger_offset    bigint primary key not null,
    recorded_at      timestamp          not null, --with timezone
    -- SubmissionId for package to be uploaded
    submission_id    varchar,
    -- The type of entry, one of 'accept' or 'reject'
    typ              varchar            not null,
    -- If the type is 'reject', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar,

    constraint check_package_entry_type
        check (
                (typ = 'accept' and rejection_reason is null) or
                (typ = 'reject' and rejection_reason is not null)
            )
);

-- Index for retrieving the package entry by submission id
CREATE UNIQUE INDEX idx_package_entries
    ON package_entries (submission_id)
