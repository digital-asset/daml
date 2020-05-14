-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify
  ( main
  , verify
  ) where

import Control.Monad (when)
import Data.Text
import Options.Applicative

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Generate
import DA.Daml.LF.Verify.Solve
import DA.Daml.LF.Verify.Read
import DA.Daml.LF.Verify.Context
import DA.Bazel.Runfiles

getSolver :: IO FilePath
getSolver = locateRunfiles "z3_nix/bin/z3"

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  let choiceTmpl = TypeConName [pack optChoiceTmpl]
      choiceName = ChoiceName (pack optChoiceName)
      fieldTmpl = TypeConName [pack optFieldTmpl]
      fieldName = FieldName (pack optFieldName)
  result <- verify optInputDar True choiceTmpl choiceName fieldTmpl fieldName
  print result

-- | Execute the full verification pipeline.
verify :: FilePath
  -- ^ The DAR file to load.
  -> Bool
  -- ^ Enable print outs.
  -> TypeConName
  -- ^ The template in which the given choice is defined.
  -> ChoiceName
  -- ^ The choice to be verified.
  -> TypeConName
  -- ^ The template in which the given field is defined.
  -> FieldName
  -- ^ The field to be verified.
  -> IO Result
verify dar verbose choiceTmpl choiceName fieldTmpl fieldName = do
  pkgs <- readPackages [dar]
  solver <- getSolver
  when verbose $ putStrLn "Start value gathering"
  case runEnv (genPackages pkgs) (emptyEnv :: Env 'ValueGathering) of
    Left err-> do
      putStrLn "Value phase finished with error: "
      print err
      return Unknown
    Right env1 -> do
      when verbose $ putStrLn "Start value solving"
      let env2 = solveValueReferences env1
      when verbose $ putStrLn "Start choice gathering"
      case runEnv (genPackages pkgs) env2 of
        Left err -> do
          putStrLn "Choice phase finished with error: "
          print err
          return Unknown
        Right env3 -> do
          when verbose $ putStrLn "Start choice solving"
          let env4 = solveChoiceReferences env3
          when verbose $ putStrLn "Start constraint solving phase"
          let cset = constructConstr env4 choiceTmpl choiceName fieldTmpl fieldName
          when verbose $ putStr "Create: " >> print (_cCres cset)
          when verbose $ putStr "Archive: " >> print (_cArcs cset)
          solveConstr solver verbose cset
