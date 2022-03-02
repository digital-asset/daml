-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Keep in sync with compatibility/bazel_tools/daml_ledger/Sandbox.hs
module DA.PortFile (readPortFileWith, readPortFile, maxRetries, retryDelayMillis) where

import Control.Concurrent
import Control.Exception
import Control.Monad
import qualified Data.Text.IO as T
import Data.Text (pack)
import Safe (readMay)
import System.Exit
import System.IO
import System.IO.Error
import System.Process

readOnce :: (String -> Maybe t) -> FilePath -> IO (Maybe t)
readOnce parseFn file = catchJust
    (guard . shouldCatch)
    (parseFn <$> readFile file)
    (const $ pure Nothing)

readPortFileWith :: (String -> Maybe t) -> ProcessHandle -> Int -> FilePath -> IO t
readPortFileWith _ _ 0 file = do
  T.hPutStrLn stderr ("Port file was not written to '" <> pack file <> "' in time.")
  exitFailure
readPortFileWith parseFn ph n file = do
  result <- readOnce parseFn file
  case result of
    Just p -> pure p
    Nothing -> do
      status <- getProcessExitCode ph
      case status of
        Nothing -> do -- Process still active. Try again.
          threadDelay (1000 * retryDelayMillis)
          readPortFileWith parseFn ph (n-1) file
        Just exitCode -> do -- Process exited already. Try reading one last time, then give up.
          threadDelay (1000 * retryDelayMillis)
          result <- readOnce parseFn file
          case result of
            Just p -> pure p
            Nothing -> do
              T.hPutStrLn stderr ("Port file was not written to '" <> pack file <> "' before process exit with " <> pack (show exitCode))
              exitFailure

readPortFile :: ProcessHandle -> Int -> FilePath -> IO Int
readPortFile = readPortFileWith readMay

-- On Windows we sometimes get permission errors. It looks like
-- this might come from a race where sandbox is writing the file at the same
-- time we try to open it so catching the exception is the right thing to do.
shouldCatch :: IOException -> Bool
shouldCatch e = isDoesNotExistError e || isPermissionError e

retryDelayMillis :: Int
retryDelayMillis = 50

maxRetries :: Int
maxRetries = 120 * (1000 `div` retryDelayMillis)
