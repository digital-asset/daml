-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.PastAndFuture(PastAndFuture(..)) where

import DA.Ledger.Stream as Stream

data PastAndFuture a = PF { past :: [a], future :: Stream a }
