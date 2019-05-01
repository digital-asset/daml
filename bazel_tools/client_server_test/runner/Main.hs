-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
module Main(main) where

import qualified Data.Text as T
import qualified Data.Text.IO as T
import System.Environment
import qualified Data.Conduit as C
import Data.Conduit.Process (withCheckedProcessCleanup)
import qualified Data.Conduit.Text as C.T
import Conduit (runConduit, (.|), liftIO)
import Control.Concurrent.Async
import System.Process (proc, callProcess, CreateProcess)
import System.IO (stdout, stderr, Handle, hFlush, readFile)
import Control.Concurrent (threadDelay)
import System.IO.Temp (emptySystemTempFile)
import System.Directory (removeFile)
import System.Exit
import Safe
import Data.List.Split (splitOn)

retryDelayMillis :: Int
retryDelayMillis = 100

-- Wait up to 60s for the port file to be written to. A long timeout is used to
-- avoid flaky results under high system load.
maxRetries :: Int
maxRetries = 60 * (1000 `div` retryDelayMillis)

readPortFile :: Int -> String -> IO Int
readPortFile 0 file = do
  T.hPutStrLn stderr "Port file was not written to in time."
  removeFile file
  exitFailure

readPortFile n file =
  readMay <$> readFile file >>= \case
    Nothing -> do
      threadDelay (1000 * retryDelayMillis)
      readPortFile (n-1) file
    Just p -> pure p

runCheckedProc :: T.Text -> CreateProcess -> IO () -> IO ()
runCheckedProc desc proc resume =
  withCheckedProcessCleanup proc $ \(_ :: Handle) stdoutSrc stderrSrc ->
  withAsync (runConduit (stderrSrc .| splitOutput .| C.awaitForever printStderr)) $ \_ ->
  withAsync (runConduit (stdoutSrc .| splitOutput .| C.awaitForever printStdout)) $
  const resume

  where
    printStdout line =
      liftIO $ do
        T.hPutStrLn stdout $ desc <> ": " <> line
        hFlush stdout
    printStderr line =
      liftIO $ T.hPutStrLn stderr $ desc <> ": " <> line
    splitOutput = C.T.decode C.T.utf8 .| C.T.lines

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs] <- getArgs
  tempFile <- emptySystemTempFile "client-server-test-runner-port"
  let splitArgs = filter (/= "") . splitOn " "

  let serverProc =
        proc serverExe (["--port-file", tempFile] <> splitArgs serverArgs)
  runCheckedProc "SERVER" serverProc $ do
    port <- readPortFile maxRetries tempFile
    removeFile tempFile
    callProcess clientExe
      (["--target-port", show port] <> splitArgs clientArgs)
    exitSuccess
