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
    -- SubmissionId for package to be uploaded
    submission_id       varchar primary key     not null,
    -- The unique identifier of the package (the hash of its content)
    package_id          varchar                 not null,
    -- Packages are uploaded as DAR files (i.e., in groups)
    -- This field can be used to find out which packages were uploaded together
    upload_id           varchar                 not null,
    -- A human readable description of the package source
    source_description  varchar,
    -- participant id that initiated the package upload
    participant_id      varchar                 not null,
    -- The size of the archive payload (i.e., the serialized DAML-LF package), in bytes
    size                bigint                   not null,
    -- The time when the package was added
    known_since         timestamp                not null,
    -- The ledger end at the time when the package was added
    ledger_offset       bigint                   not null,
    -- The DAML-LF archive, serialized using the protobuf message `daml_lf.Archive`.
    --  See also `daml-lf/archive/da/daml_lf.proto`.
    package             bytea                    not null,
    -- If the type is 'rejection', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason    varchar
)
