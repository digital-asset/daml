-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import Control.Concurrent (threadDelay)
import qualified Data.Text.IO as T
import System.Environment
import System.Process
import System.IO
import System.IO.Extra (withTempFile)
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
readPortFile 0 _file = do
  T.hPutStrLn stderr "Port file was not written to in time."
  exitFailure

readPortFile n file =
  readMay <$> readFile file >>= \case
    Nothing -> do
      threadDelay (1000 * retryDelayMillis)
      readPortFile (n-1) file
    Just p -> pure p

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs] <- getArgs
  withTempFile $ \tempFile -> do
    let splitArgs = filter (/= "") . splitOn " "
    let serverProc = proc serverExe $ ["--port-file", tempFile] <> splitArgs serverArgs
    withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
      port <- readPortFile maxRetries tempFile
      callProcess clientExe (["--target-port", show port] <> splitArgs clientArgs)
