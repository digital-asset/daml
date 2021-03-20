-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V4: List of parties
--
-- This schema version adds a table for tracking known parties.
-- In the sandbox, parties are added implicitly when they are first mentioned in a transaction,
-- or explicitly through an API call.
---------------------------------------------------------------------------------------------------


CREATE TABLE parties
(
    -- The unique identifier of the party
    party         NVARCHAR2(1000) primary key not null,
    -- A human readable name of the party, might not be unique
    display_name  NVARCHAR2(1000),
    -- True iff the party was added explicitly through an API call
    explicit      NUMBER(1, 0)                not null,
    -- For implicitly added parties: the offset of the transaction that introduced the party
    -- For explicitly added parties: the ledger end at the time when the party was added
    ledger_offset BLOB,
    is_local      NUMBER(1, 0)                not null
);

