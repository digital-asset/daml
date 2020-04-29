-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

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
-- TODO: Allow for choice and field in different templates.
templName :: TypeConName
templName = TypeConName ["Iou"]

choiceName :: ChoiceName
choiceName = ChoiceName "Iou_Merge"

fieldName :: FieldName
fieldName = FieldName "content"

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  solver <- locateRunfiles "z3_nix/bin/z3"
  pkgs <- readPackages optInputDars
  putStrLn "Start value gathering"
  case runEnv (genPackages pkgs) (emptyEnv :: Env 'ValueGathering) of
    Left err-> putStrLn "Value phase finished with error: " >> print err
    Right env1 -> do
      putStrLn "Start value solving"
      let env2 = solveValueReferences env1
      putStrLn "Start choice gathering"
      case runEnv (genPackages pkgs) env2 of
        Left err -> putStrLn "Choice phase finished with error: " >> print err
        Right env3 -> do
          putStrLn "Start choice solving"
          let env4 = solveChoiceReferences env3
          putStrLn "Start constraint solving phase"
          let cset = constructConstr env4 templName choiceName fieldName
          putStr "Create: " >> print (_cCres cset)
          putStr "Archive: " >> print (_cArcs cset)
          solveConstr solver cset
