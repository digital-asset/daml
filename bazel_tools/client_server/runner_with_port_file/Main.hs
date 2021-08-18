-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module Main (main) where

import Data.List.Extra (replace, splitOn, stripInfix)
import Data.Maybe (isJust)
import System.Environment (getArgs)
import System.FilePath ((</>))
import System.Process (callProcess, proc, withCreateProcess)
import System.IO.Temp (withSystemTempDirectory)

import DA.PortFile

main :: IO ()
main = do
  [clientExe, clientArgs, serverExe, serverArgs, _] <- getArgs
  let splitArgs = filter (/= "") . splitOn " "
  let splitServerArgs = splitArgs serverArgs
  withSystemTempDirectory "runner" $ \tempDir -> do
    (portFile, interpolatedServerArgs) <-
      if any (isJust . stripInfix "%PORT_FILE%") splitServerArgs
        then do
          let portFile = tempDir </> "portfile"
          let interpolatedArgs = map (replace "%PORT_FILE%" portFile) splitServerArgs
          return (Just portFile, interpolatedArgs)
        else
          return (Nothing, splitServerArgs)
    let serverProc = proc serverExe interpolatedServerArgs
    withCreateProcess serverProc $ \_stdin _stdout _stderr _ph -> do
      maybePort <- mapM (readPortFile maxRetries) portFile
      let splitClientArgs = splitArgs clientArgs
      let interpolatedClientArgs =
            case maybePort of
              Nothing -> splitClientArgs
              Just port -> map (replace "%PORT%" (show port)) splitClientArgs
      callProcess clientExe interpolatedClientArgs
