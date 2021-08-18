-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Control.Monad (unless)
import Data.List.Extra (replace, splitOn, stripInfix)
import Data.Maybe (isJust)
import System.Environment (getArgs)
import System.FilePath ((</>))
import System.Process (callProcess, proc, withCreateProcess)
import System.Exit (exitFailure)
import System.IO (hPutStrLn, stderr)
import System.IO.Extra (withTempDir)

import DA.PortFile

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, _] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  let splitServerArgs = splitArgs serverArgs
  let splitClientArgs = splitArgs clientArgs
  unless (any (isJust . stripInfix "%PORT_FILE%") splitServerArgs) $ do
    hPutStrLn stderr "No server parameters accept a port file."
    exitFailure
  withTempDir $ \tempDir -> do
    let portFile = tempDir </> "portfile"
    let interpolatedServerArgs = map (replace "%PORT_FILE%" portFile) splitServerArgs
    let serverProc = proc serverExe interpolatedServerArgs
    withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
      port <- readPortFile maxRetries portFile
      let interpolatedClientArgs = map (replace "%PORT%" (show port)) splitClientArgs
      callProcess clientExe interpolatedClientArgs
