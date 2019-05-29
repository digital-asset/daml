-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Trace(trace) where

import Control.Concurrent
import Control.Monad(when)
import Data.Time.Clock
import System.IO (hFlush, stdout)

enabled :: Bool
enabled = False

trace :: String -> IO ()
trace s = when enabled $ do
    tid <- myThreadId
    t <- fmap utctDayTime getCurrentTime
    putStrLn $ "[" <> show tid <> "--" <> show t <> "]:" <> s
    hFlush stdout
