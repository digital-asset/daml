-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify ( main ) where

import Options.Applicative

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Generate
import DA.Daml.LF.Verify.Solve
import DA.Daml.LF.Verify.Read
import DA.Daml.LF.Verify.Context
import DA.Bazel.Runfiles

-- TODO: temporarily hardcoded
templName :: TypeConName
templName = TypeConName ["Iou"]

choiceName :: ChoiceName
choiceName = ChoiceName "Iou_Merge"

fieldName :: FieldName
fieldName = FieldName "amount"

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  solver <- locateRunfiles "z3_nix/bin/z3"
  pkgs <- readPackages optInputDars
  putStrLn "Start value phase" >> case runEnv (genPackages ValuePhase pkgs) emptyEnv of
    Left err-> putStrLn "Value phase finished with error: " >> print err
    Right env1 -> do
      putStrLn "Start value solving"
      let env2 = solveValueUpdatesEnv env1
      putStrLn "Start template phase" >> case runEnv (genPackages TemplatePhase pkgs) env2 of
        Left err -> putStrLn "Template phase finished with error: " >> print err
        Right env3 -> do
          putStrLn "Success!"
          putStrLn "Start constraint solving phase"
          let cset = constructConstr env3 templName choiceName fieldName
          putStr "Create: " >> print (_cCres cset)
          putStr "Archive: " >> print (_cArcs cset)
          solveConstr solver cset
