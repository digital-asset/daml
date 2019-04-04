-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module System.IO.Timing
  ( withTiming
  ) where

import Data.Time.Clock (getCurrentTime, diffUTCTime)

-- | Time the given IO action and return the result
-- along with the number of seconds the action took.
withTiming :: IO a -> IO (a, Double)
withTiming act = do
  tstart <- getCurrentTime
  result <- tstart `seq` act
  tend <- result `seq` getCurrentTime
  let diff = diffUTCTime tend tstart
  return
    ( result
    , fromRational (toRational diff) * 1000.0
    )
