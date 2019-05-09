-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.GHC.Compiler.Options
    ( Options(..)
    , defaultOptionsIO
    , defaultOptions
    , mkOptions
    , getBaseDir
    , toCompileOpts
    , projectPackageDatabase
    , basePackages
    ) where


import Development.IDE.UtilGHC (runGhcFast)
import DA.Daml.GHC.Compiler.Config (setupDamlGHC)
import qualified Development.IDE.Types.Options as Compile

import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.GHC.Compiler.Preprocessor

import           Control.Monad.Reader
import qualified Data.List.Extra as List
import Data.Foldable (toList)
import Data.Maybe
import Data.Tuple.Extra
import "ghc-lib-parser" DynFlags
import qualified "ghc-lib" GHC
import "ghc-lib-parser" Module (moduleNameSlashes)
import "ghc-lib-parser" PackageConfig
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
  , optIfaceDir :: FilePath
    -- ^ Directory to write interface files to. Default is current working directory.
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
  , optGhcCustomOpts :: [String]
    -- ^ custom options, parsed by GHC option parser, overriding DynFlags
  } deriving Show

-- | Convert to the DAML-independent CompileOpts type.
-- TODO (MK) Cleanup as part of the Options vs CompileOpts cleanup
toCompileOpts :: Options -> Compile.IdeOptions
toCompileOpts Options{..} =
    Compile.IdeOptions
      { optPreprocessor = damlPreprocessor
      , optRunGhcSession = \mbMod packageState m -> runGhcFast $ do
            let importPaths = maybe [] moduleImportPaths mbMod <> optImportPath
            setupDamlGHC importPaths optMbPackageName packageState optGhcCustomOpts
            m
      , optPkgLocationOpts = Compile.IdePkgLocationOptions
          { optLocateHieFile = locateInPkgDb "hie"
          , optLocateSrcFile = locateInPkgDb "daml"
          }
      , optWriteIface = optWriteInterface
      , optIfaceDir = optIfaceDir
      , optExtensions = ["daml"]
      , optMbPackageName = optMbPackageName
      , optPackageDbs = optPackageDbs
      , optHideAllPkgs = optHideAllPkgs
      , optPackageImports = map (second toRenaming) optPackageImports
      , optThreads = optThreads
      , optShakeProfiling = optShakeProfiling
      }
  where
    toRenaming aliases = ModRenaming False [(GHC.mkModuleName mod, GHC.mkModuleName alias) | (mod, alias) <- aliases]
    locateInPkgDb :: String -> PackageConfig -> GHC.Module -> IO (Maybe FilePath)
    locateInPkgDb ext pkgConfig mod
      | (importDir : _) <- importDirs pkgConfig = do
            -- We only produce package configs with exactly one importDir.
            let path = importDir </> moduleNameSlashes (GHC.moduleName mod) <.> ext
            exists <- Dir.doesFileExist path
            pure $ if exists
                then Just path
                else Nothing
      | otherwise = pure Nothing

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

-- | The project package database path relative to the project root.
projectPackageDatabase :: FilePath
projectPackageDatabase = ".package-database"

-- | Packages that we ship with the compiler.
basePackages :: [String]
basePackages = ["daml-prim", "daml-stdlib"]

-- | Check that import paths and package db directories exist
-- and add the default package db if it exists
mkOptions :: Options -> IO Options
mkOptions opts@Options {..} = do
    mapM_ checkDirExists $ optImportPath <> optPackageDbs
    mbDefaultPkgDb <- locateRunfilesMb (mainWorkspace </> "daml-foundations" </> "daml-ghc" </> "package-database")
    let mbDefaultPkgDbDir = fmap (</> "package-db_dir") mbDefaultPkgDb
    pkgDbs <- filterM Dir.doesDirectoryExist (toList mbDefaultPkgDbDir ++ [projectPackageDatabase])
    pure opts {optPackageDbs = map (</> versionSuffix) $ pkgDbs ++ optPackageDbs}
  where checkDirExists f =
          Dir.doesDirectoryExist f >>= \ok ->
          unless ok $ error $
            "Required configuration/package database directory does not exist: " <> f
        versionSuffix = renderPretty optDamlLfVersion

-- | Default configuration for the compiler with package database set according to daml-lf version
-- and located runfiles. If the version argument is Nothing it is set to the default daml-lf
-- version.
defaultOptionsIO :: Maybe LF.Version -> IO Options
defaultOptionsIO mbVersion = mkOptions $ defaultOptions mbVersion

defaultOptions :: Maybe LF.Version -> Options
defaultOptions mbVersion =
    Options
        { optImportPath = []
        , optPackageDbs = []
        , optMbPackageName = Nothing
        , optWriteInterface = False
        , optIfaceDir = "."
        , optHideAllPkgs = False
        , optPackageImports = []
        , optShakeProfiling = Nothing
        , optThreads = 1
        , optDamlLfVersion = fromMaybe LF.versionDefault mbVersion
        , optDebug = False
        , optGhcCustomOpts = []
        }

getBaseDir :: IO FilePath
getBaseDir = locateRunfiles (mainWorkspace </> "daml-foundations/daml-ghc")
