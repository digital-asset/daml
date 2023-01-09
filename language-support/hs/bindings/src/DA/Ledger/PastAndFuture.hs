-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.PastAndFuture(PastAndFuture(..),mapListPF) where

-- This type is used as the return of the `Ledger.getTransaction`
-- function, and captures the pairing of those ledger transactions
-- which have already occurred in the past, and those transactions
-- which will occur in the future.

import Control.Monad (join)
import DA.Ledger.Stream as Stream

data PastAndFuture a = PastAndFuture { past :: [a], future :: Stream a }

mapListPF :: (a -> IO [b]) -> PastAndFuture a -> IO (PastAndFuture b)
mapListPF f PastAndFuture{past=past0,future=future0} = do
    past <- fmap join $ mapM f past0
    future <- mapListStream f future0
    return $ PastAndFuture {past, future}
