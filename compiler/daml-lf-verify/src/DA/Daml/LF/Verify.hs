-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify
  ( main
  , verify
  ) where

import Data.Text
import Data.Text.Prettyprint.Doc
import Data.Text.Prettyprint.Doc.Render.String
import Options.Applicative
import System.Exit
import System.IO

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
  result <- verify optInputDar putStrLn choiceTmpl choiceName fieldTmpl fieldName
  print result

outputError :: Error
  -- ^ The error message.
  -> String
  -- ^ An additional message providing context.
  -> IO Result
outputError err msg = do
  hPutStrLn stderr msg
  hPutStrLn stderr (show err)
  exitFailure

-- | Execute the full verification pipeline.
verify :: FilePath
  -- ^ The DAR file to load.
  -> (String -> IO ())
  -- ^ Function for debugging printouts.
  -> TypeConName
  -- ^ The template in which the given choice is defined.
  -> ChoiceName
  -- ^ The choice to be verified.
  -> TypeConName
  -- ^ The template in which the given field is defined.
  -> FieldName
  -- ^ The field to be verified.
  -> IO Result
verify dar debug choiceTmpl choiceName fieldTmpl fieldName = do
  -- Read the packages to analyse, and initialise the provided solver.
  pkgs <- readPackages [dar]
  solver <- getSolver
  -- Start reading data type and value definitions. References to other
  -- values are just stored as references at this point.
  debug "Start value gathering"
  case runEnv (genPackages pkgs) (emptyEnv :: Env 'ValueGathering) of
    Left err-> outputError err "Value phase finished with error: "
    Right env1 -> do
      -- All value definitions have been handled. Start computing closures of
      -- the stored value references. After this phase, all value references
      -- should be inlined.
      debug "Start value solving"
      let env2 = solveValueReferences env1
      -- Start reading template definitions. References to choices are just
      -- stored as references at this point.
      debug "Start choice gathering"
      case runEnv (genPackages pkgs) env2 of
        Left err -> outputError err "Choice phase finished with error: "
        Right env3 -> do
          -- All choice definitions have been handled. Start computing closures
          -- of the stored choice references. After this phase, all choice
          -- references should be inlined.
          debug "Start choice solving"
          let env4 = solveChoiceReferences env3
          -- Construct the actual constraints to be solved by the SMT solver.
          debug "Start constraint solving phase"
          let cset = constructConstr env4 choiceTmpl choiceName fieldTmpl fieldName
          debug $ renderString $ layoutCompact ("Create: " <+> pretty (_cCres cset))
          debug $ renderString $ layoutCompact ("Archive: " <+> pretty (_cArcs cset))
          -- Pass the constraints to the SMT solver.
          solveConstr solver debug cset
