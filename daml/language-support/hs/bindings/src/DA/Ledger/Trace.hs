-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Ledger.Trace(trace) where

import Control.Concurrent(myThreadId)
import Control.Monad (when)
import Data.Time.Clock (getCurrentTime,utctDayTime)
import System.IO (hFlush,stdout)

enabled :: Bool
enabled = False

trace :: String -> IO ()
trace s = when enabled $ do
    tid <- myThreadId
    t <- fmap utctDayTime getCurrentTime
    let t' = (fromIntegral ((truncate (t * 100) :: Int) `mod` 6000) :: Double) / 100.0
    putStrLn $ "[" <> show tid <> "--" <> show t' <> "]:" <> s
    hFlush stdout
