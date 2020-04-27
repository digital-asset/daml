-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import DA.PortFile
import System.Environment
import System.Process
import System.IO.Extra (withTempFile)
import Data.List.Split (splitOn)


main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, _runnerArgs] <- getArgs
  withTempFile $ \tempFile -> do
    let splitArgs = filter (/= "") . splitOn " "
    let serverProc = proc serverExe $ ["--port-file", tempFile] <> splitArgs serverArgs
    withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
      port <- readPortFile maxRetries tempFile
      callProcess clientExe (["--target-port", show port] <> splitArgs clientArgs)
