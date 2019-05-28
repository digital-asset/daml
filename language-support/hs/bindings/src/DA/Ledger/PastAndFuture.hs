-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.PastAndFuture(PastAndFuture(..)) where

-- This type is used as the return of the `Ledger.getTransaction`
-- function, and captures the pairing of those ledger transactions
-- which have already occurred in the past, and those transactions
-- which will occur in the future.

import DA.Ledger.Stream as Stream

data PastAndFuture a = PastAndFuture { past :: [a], future :: Stream a }
