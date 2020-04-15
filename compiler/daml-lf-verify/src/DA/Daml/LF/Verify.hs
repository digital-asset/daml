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
  putStrLn "Start value phase" >> case runDelta (genPackages ValuePhase pkgs) emptyDelta of
    Left err-> putStrLn "Value phase finished with error: " >> print err
    Right delta1 -> putStrLn "Start solving" >>
                    let delta2 = solveValueUpdatesDelta delta1
                    in putStrLn "Start template phase" >> case runDelta (genPackages TemplatePhase pkgs) delta2 of
      Left err -> putStrLn "Template phase finished with error: " >> print err
      Right _delta3 -> putStrLn "Success!"

