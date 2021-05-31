-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify
  ( main
  , verify
  ) where

import Data.Maybe
import qualified Data.NameMap as NM
import Options.Applicative
import System.Exit
import System.IO

import DA.Daml.LF.Ast.Base
import DA.Daml.LF.Verify.Generate
import DA.Daml.LF.Verify.Solve
import DA.Daml.LF.Verify.Read
import DA.Daml.LF.Verify.Context
import DA.Daml.LF.Verify.ReferenceSolve
import DA.Bazel.Runfiles

getSolver :: IO FilePath
getSolver = locateRunfiles "z3_nix/bin/z3"

main :: IO ()
main = do
  Options{..} <- execParser optionsParserInfo
  let (choiceMod, choiceTmpl, choiceName) = optChoice
      (fieldMod, fieldTmpl, fieldName) = optField
  _ <- verify optInputDar putStrLn choiceMod choiceTmpl choiceName fieldMod fieldTmpl fieldName
  putStrLn "\n==========\n"
  putStrLn "Done."

outputError :: Error
  -- ^ The error message.
  -> String
  -- ^ An additional message providing context.
  -> IO a
outputError err msg = do
  hPutStrLn stderr msg
  hPrint stderr err
  exitFailure

-- | Execute the full verification pipeline.
verify :: FilePath
  -- ^ The DAR file to load.
  -> (String -> IO ())
  -- ^ Function for debugging printouts.
  -> ModuleName
  -- ^ The module in which the given choice is defined.
  -> TypeConName
  -- ^ The template in which the given choice is defined.
  -> ChoiceName
  -- ^ The choice to be verified.
  -> ModuleName
  -- ^ The module in which the given field is defined.
  -> TypeConName
  -- ^ The template in which the given field is defined.
  -> FieldName
  -- ^ The field to be verified.
  -> IO [Result]
verify dar debug choiceModName choiceTmplName choiceName fieldModName fieldTmplName fieldName = do
  -- Read the packages to analyse, and initialise the provided solver.
  pkgs <- readPackages [dar]
  solver <- getSolver
  -- Find the given template names in the packages.
  choiceTmpl <- findTemplate pkgs choiceModName choiceTmplName
  fieldTmpl <- findTemplate pkgs fieldModName fieldTmplName
  -- Start reading data type and value definitions. References to other
  -- values are just stored as references at this point.
  case runEnv (genPackages pkgs) (emptyEnv :: Env 'ValueGathering) of
    Left err-> outputError err "Value phase finished with error: "
    Right env1 -> do
      -- All value definitions have been handled. Start computing closures of
      -- the stored value references. After this phase, all value references
      -- should be inlined.
      let env2 = solveValueReferences env1
      -- Start reading template definitions. References to choices are just
      -- stored as references at this point.
      case runEnv (genPackages pkgs) env2 of
        Left err -> outputError err "Choice phase finished with error: "
        Right env3 -> do
          -- All choice definitions have been handled. Start computing closures
          -- of the stored choice references. After this phase, all choice
          -- references should be inlined.
          let env4 = solveChoiceReferences env3
          -- Construct the actual constraints to be solved by the SMT solver.
          let csets = constructConstr env4 choiceTmpl choiceName fieldTmpl fieldName
          mapM (debugAndSolve solver) csets
  where
    -- | Output some debugging information and solve the given constraints.
    debugAndSolve :: FilePath -> ConstraintSet -> IO Result
    debugAndSolve solver cset = do
      -- Pass the constraints to the SMT solver.
      (debug_info, result) <- solveConstr solver cset
      debug $ debug_info choiceName fieldName
      return result

    -- | Lookup the first package that defines the given template. This avoids
    -- having to pass in the package reference manually when using the tool.
    findTemplate :: [(PackageId, (Package, Maybe PackageName))]
      -- ^ The package from the DAR file.
      -> ModuleName
      -- ^ The module name.
      -> TypeConName
      -- ^ The template name.
      -> IO (Qualified TypeConName)
    findTemplate pkgs mod tem = maybe
      (outputError (UnknownTmpl tem) "Parsing phase finished with error: ")
      (\pacid -> return $ Qualified (PRImport pacid) mod tem)
      (listToMaybe $ mapMaybe (templateInPackage mod tem) pkgs)

    -- | Return the package id of the module containing the given template, if
    -- it exists.
    templateInPackage :: ModuleName
      -- ^ The module to look for.
      -> TypeConName
      -- ^ The template to look for.
      -> (PackageId, (Package, Maybe PackageName))
      -- ^ The package to look in.
      -> Maybe PackageId
    templateInPackage modName temName (id, (pac,_)) =
      case NM.lookup modName $ packageModules pac of
        Nothing -> Nothing
        Just mod -> case NM.lookup temName $ moduleTemplates mod of
          Nothing -> Nothing
          Just _ -> Just id

