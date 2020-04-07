-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify
  ( module LF
  , main
  ) where

import Options.Applicative

import DA.Daml.LF.Verify.Generate as LF
import DA.Daml.LF.Verify.Read as LF
import DA.Daml.LF.Verify.Context as LF

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  pkgs <- readPackages optInputDars
  let _delta = runDelta $ genPackages pkgs
  putStrLn "Constraints generated."

