-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

---------------------------------------------------------------------------------------------------
-- V4: List of parties
--
-- This schema version adds a table for tracking known parties.
-- In the sandbox, parties are added implicitly when they are first mentioned in a transaction,
-- or explicitly through an API call.
---------------------------------------------------------------------------------------------------



CREATE TABLE parties (
  -- The unique identifier of the party
  party varchar primary key not null,
  -- A human readable name of the party, might not be unique
  display_name varchar,
  -- True iff the party was added explicitly through an API call
  explicit bool not null,
  -- For implicitly added parties: the offset of the transaction that introduced the party
  -- For explicitly added parties: the ledger end at the time when the party was added
  ledger_offset bigint
);

