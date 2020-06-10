-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DataKinds #-}

-- | Static verification of DAML packages.
module DA.Daml.LF.Verify
  ( main
  , verify
  ) where

import Data.Maybe
import qualified Data.NameMap as NM
import qualified Data.Text as T
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
  let choiceTmpl = TypeConName [T.pack optChoiceTmpl]
      choiceName = ChoiceName (T.pack optChoiceName)
      fieldTmpl = TypeConName [T.pack optFieldTmpl]
      fieldName = FieldName (T.pack optFieldName)
  _ <- verify optInputDar putStrLn choiceTmpl choiceName fieldTmpl fieldName
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
  -> TypeConName
  -- ^ The template in which the given choice is defined.
  -> ChoiceName
  -- ^ The choice to be verified.
  -> TypeConName
  -- ^ The template in which the given field is defined.
  -> FieldName
  -- ^ The field to be verified.
  -> IO [Result]
verify dar debug choiceTmplName choiceName fieldTmplName fieldName = do
  -- Read the packages to analyse, and initialise the provided solver.
  pkgs <- readPackages [dar]
  solver <- getSolver
  -- Find the given template names in the packages.
  choiceTmpl <- findTemplate pkgs choiceTmplName
  fieldTmpl <- findTemplate pkgs fieldTmplName
  -- Start reading data type and value definitions. References to other
  -- values are just stored as references at this point.
  case runEnv (genPackages pkgs) (emptyEnv :: Env 'ValueGathering) of
    Left err-> outputError err "Value phase finished with error: "
    Right env1 -> do
      -- All value definitions have been handled. Start computing closures of
      -- the stored value references. After this phase, all value references
      -- should be inlined.
      let env2 = solveValueReferences env1
      -- Inline the newly solved references in the boolean constraints.
      case runEnv inlineReferences env2 of
        Left err -> outputError err "Value reference solving phase finished with error: "
        Right env3 -> do
          -- Start reading template definitions. References to choices are just
          -- stored as references at this point.
          debug "Start choice gathering"
          case runEnv (genPackages pkgs) env3 of
            Left err -> outputError err "Choice phase finished with error: "
            Right env4 -> do
              -- All choice definitions have been handled. Start computing closures
              -- of the stored choice references. After this phase, all choice
              -- references should be inlined.
              debug "Start choice solving"
              let env5 = solveChoiceReferences env4
              -- Construct the actual constraints to be solved by the SMT solver.
              let csets = constructConstr env5 choiceTmpl choiceName fieldTmpl fieldName
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
      -> TypeConName
      -- ^ The template name.
      -> IO (Qualified TypeConName)
    findTemplate pkgs tem = maybe
      (outputError (UnknownTmpl tem) "Parsing phase finished with error: ")
      (\(pacid, mod) -> return $ Qualified (PRImport pacid) mod tem)
      (listToMaybe $ mapMaybe (templateInPackage tem) pkgs)

    -- | Return the package id and the name of the module containing the given
    -- template, if it exists.
    templateInPackage :: TypeConName
      -- ^ The template to look for.
      -> (PackageId, (Package, Maybe PackageName))
      -- ^ The package to look in.
      -> Maybe (PackageId, ModuleName)
    templateInPackage tem (id, (pac,_)) =
      case templateInModules tem $ NM.toList $ packageModules pac of
        Nothing -> Nothing
        Just mod -> Just (id, mod)

    -- | Return the name of the module containing the given template, if it
    -- exists.
    templateInModules :: TypeConName
      -- ^ The template to look for.
      -> [Module]
      -- ^ The modules to look in.
      -> Maybe ModuleName
    templateInModules tem mods =
      listToMaybe $
        mapMaybe ( \Module{..} ->
          let tmpls = NM.toList moduleTemplates
          in if not (any (\Template{..} -> tplTypeCon == tem) tmpls)
            then Nothing
            else Just moduleName )
        mods
