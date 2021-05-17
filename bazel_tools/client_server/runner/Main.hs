-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main(main) where

import DA.PortFile
import System.Environment
import System.FilePath ((</>))
import System.Process
import System.IO.Extra (withTempDir)
import Data.List.Split (splitOn)


main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, _runnerArgs] <- getArgs
  withTempDir $ \tempDir -> do
    let splitArgs = filter (/= "") . splitOn " "
    let portFile = tempDir </> "portfile"
    let serverProc = proc serverExe $ ["--port-file", portFile] <> splitArgs serverArgs
    withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
      port <- readPortFile maxRetries portFile
      callProcess clientExe (["--target-port", show port] <> splitArgs clientArgs)
