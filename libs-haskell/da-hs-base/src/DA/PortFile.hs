-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Keep in sync with compatibility/bazel_tools/daml_ledger/Sandbox.hs
module DA.PortFile (readPortFile, maxRetries, retryDelayMillis) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import qualified Data.Text.IO as T
import Safe (readMay)
import System.Exit
import System.IO
import System.IO.Error

readPortFile :: Int -> String -> IO Int
readPortFile 0 file = do
  T.hPutStrLn stderr ("Port file was not written to '" ++ file ++ "' in time.")
  exitFailure
readPortFile n file = do
  fileContent <- catchJust (guard . shouldCatch) (readFile file) (const $ pure "")
  case readMay fileContent of
    Nothing -> do
      threadDelay (1000 * retryDelayMillis)
      readPortFile (n-1) file
    Just p -> pure p

-- On Windows we sometimes get permission errors. It looks like
-- this might come from a race where sandbox is writing the file at the same
-- time we try to open it so catching the exception is the right thing to do.
shouldCatch :: IOException -> Bool
shouldCatch e = isDoesNotExistError e || isPermissionError e

retryDelayMillis :: Int
retryDelayMillis = 50

maxRetries :: Int
maxRetries = 120 * (1000 `div` retryDelayMillis)
