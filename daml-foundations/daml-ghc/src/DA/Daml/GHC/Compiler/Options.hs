-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.GHC.Compiler.Options
    ( Options(..)
    , defaultOptionsIO
    , mkOptions
    , getBaseDir
    , toCompileOpts
    ) where


import Development.IDE.UtilGHC (runGhcFast, setupDamlGHC)
import qualified Development.IDE.Functions.Compile as Compile

import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.GHC.Compiler.Preprocessor

import           Control.Monad.Reader
import qualified Data.List.Extra as List
import Data.Maybe (fromMaybe)
import qualified "ghc-lib" GHC
import "ghc-lib-parser" Module (moduleNameSlashes)
import qualified System.Directory as Dir
import           System.FilePath
import DA.Pretty (renderPretty)

-- | Compiler run configuration for DAML-GHC.
data Options = Options
  { optImportPath :: [FilePath]
    -- ^ import path for both user modules and standard library
  , optPackageDbs :: [FilePath]
    -- ^ package databases that will be loaded
  , optMbPackageName :: Maybe String
    -- ^ compile in the context of the given package name and create interface files
  , optWriteInterface :: Bool
  -- ^ Whether we should write interface files during typechecking.
  -- Until the stdlib moves to a separate package, this should only be used
  -- with the bootstrap compiler since the stdlib files are mounted read-only.
  , optHideAllPkgs :: Bool
    -- ^ hide all imported packages
  , optPackageImports :: [(String, [(String, String)])]
    -- ^ list of explicit package imports and modules with aliases
  , optShakeProfiling :: Maybe FilePath
    -- ^ enable shake profiling
  , optThreads :: Int
    -- ^ number of threads to use
  , optDamlLfVersion :: LF.Version
    -- ^ The target DAML LF version
  , optDebug :: Bool
    -- ^ Whether to enable debugging output
  } deriving Show

-- | Convert to the DAML-independent CompileOpts type.
-- TODO (MK) Cleanup as part of the Options vs CompileOpts cleanup
toCompileOpts :: Options -> Compile.CompileOpts
toCompileOpts Options{..} =
    Compile.CompileOpts
      { optPreprocessor = damlPreprocessor
      , optRunGhcSession = \mbMod packageState m -> runGhcFast $ do
            let importPaths = maybe [] moduleImportPaths mbMod <> optImportPath
            setupDamlGHC importPaths optMbPackageName packageState
            m
      , optWriteIface = optWriteInterface
      , optMbPackageName = optMbPackageName
      , optPackageDbs = optPackageDbs
      , optHideAllPkgs = optHideAllPkgs
      , optPackageImports = optPackageImports
      , optThreads = optThreads
      , optShakeProfiling = optShakeProfiling
      }

moduleImportPaths :: GHC.ParsedModule -> [FilePath]
moduleImportPaths pm =
    maybe [] (\modRoot -> [modRoot]) mbModuleRoot
  where
    ms   = GHC.pm_mod_summary pm
    file = GHC.ms_hspp_file ms
    mod'  = GHC.ms_mod ms
    rootPathDir  = takeDirectory file
    rootModDir   = takeDirectory . moduleNameSlashes . GHC.moduleName $ mod'
    mbModuleRoot
        | rootModDir == "." = Just rootPathDir
        | otherwise = dropTrailingPathSeparator <$> List.stripSuffix rootModDir rootPathDir


-- | Check that import paths and package db directories exist
-- and add the default package db if it exists
mkOptions :: Options -> IO Options
mkOptions opts@Options{..} = do
    baseDir <- getBaseDir
    mapM_ checkDirExists $ optImportPath <> optPackageDbs
    let defaultPkgDb = baseDir </> "package-database"
    hasDefaultPkgDb <- Dir.doesDirectoryExist defaultPkgDb
    pure opts
            { optPackageDbs =
                  map (</> versionSuffix) $
                  [defaultPkgDb | hasDefaultPkgDb] ++ optPackageDbs
            }
  where checkDirExists f =
          Dir.doesDirectoryExist f >>= \ok ->
          unless ok $ error $
            "Required configuration/package database directory does not exist: " <> f
        versionSuffix = case optDamlLfVersion of
            LF.VDev _ -> "dev"
            _ -> renderPretty optDamlLfVersion

-- | Default configuration for the compiler with package database set according to daml-lf version
-- and located runfiles. If the version argument is Nothing it is set to the default daml-lf
-- version.
defaultOptionsIO :: Maybe LF.Version -> IO Options
defaultOptionsIO mbVersion = do
    baseDir <- getBaseDir
    mkOptions Options
        { optImportPath = [baseDir </> "daml-stdlib-src"]
        , optPackageDbs = []
        , optMbPackageName = Nothing
        , optWriteInterface = False
        , optHideAllPkgs = False
        , optPackageImports = []
        , optShakeProfiling = Nothing
        , optThreads = 1
        , optDamlLfVersion = fromMaybe LF.versionDefault mbVersion
        , optDebug = False
        }

getBaseDir :: IO FilePath
getBaseDir = locateRunfiles (mainWorkspace </> "daml-foundations/daml-ghc")
