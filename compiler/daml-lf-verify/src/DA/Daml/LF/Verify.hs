-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify ( main ) where

import Options.Applicative

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Generate
import DA.Daml.LF.Verify.Read
import DA.Daml.LF.Verify.Context

-- TODO: temporarily hardcoded
choiceName :: ChoiceName
choiceName = ChoiceName "Iou_Split"

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  pkgs <- readPackages optInputDars
  putStrLn "Start value phase" >> case runEnv (genPackages ValuePhase pkgs) emptyEnv of
    Left err-> putStrLn "Value phase finished with error: " >> print err
    Right env1 -> putStrLn "Start solving" >>
      let env2 = solveValueUpdatesEnv env1
      in putStrLn "Start template phase" >> case runEnv (genPackages TemplatePhase pkgs) env2 of
        Left err -> putStrLn "Template phase finished with error: " >> print err
        Right env3 -> putStrLn "Success!" >> print (lookupChoInHMap (_envchs env3) choiceName)

