-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V12: package_entries for package upload
--
-- This schema version adds a table for tracking DAML-LF package submissions
-- It includes id to track the package submission and status
---------------------------------------------------------------------------------------------------


ALTER TABLE packages
    -- SubmissionId for package to be uploaded
    ADD submission_id varchar not null,

    -- participant id that initiated the package upload
    participant_id varchar not null,

    -- If the type is 'rejection', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar;

