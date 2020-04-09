-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify ( main ) where

import Options.Applicative

import DA.Daml.LF.Verify.Generate
import DA.Daml.LF.Verify.Read
import DA.Daml.LF.Verify.Context

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  pkgs <- readPackages optInputDars
  let delta = runDelta $ genPackages pkgs
  putStrLn "Constraints generated."
  case delta of
    Left _ -> return ()
    Right _ -> return ()

