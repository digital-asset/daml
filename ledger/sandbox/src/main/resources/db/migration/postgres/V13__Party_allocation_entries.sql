-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V13: party_allocation_entries
--
-- This schema version adds a table for tracking party allocation submissions
---------------------------------------------------------------------------------------------------

CREATE TABLE party_allocation_entries
(
    -- SubmissionId for the party allocation
    submission_id    varchar not null,
    -- participant id that initiated the allocation request
    participant_id   varchar not null,
    -- party
    party   varchar not null,
    -- displayName
    display_name   varchar not null,
    -- The ledger end at the time when the party allocation was added
    ledger_offset      bigint                not null,
    -- If the type is 'rejection', then the rejection reason is set.
    -- Rejection reason is a human-readable description why the change was rejected.
    rejection_reason varchar
)