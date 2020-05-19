-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

module DA.Daml.Options.Types
    ( Options(..)
    , EnableScenarioService(..)
    , SkipScenarioValidation(..)
    , DlintUsage(..)
    , Haddock(..)
    , IncrementalBuild(..)
    , InferDependantPackages(..)
    , PackageFlag(..)
    , ModRenaming(..)
    , PackageArg(..)
    , defaultOptions
    , getBaseDir
    , damlArtifactDir
    , projectPackageDatabase
    , ifaceDir
    , distDir
    , genDir
    , basePackages
    , getPackageDbs
    , pkgNameVersion
    , fullPkgName
    , optUnitId
    ) where

import Control.Monad.Reader
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Pretty
import Data.Maybe
import qualified Data.Text as T
import Development.IDE.GHC.Util (prettyPrint)
import Development.IDE.Types.Location
import DynFlags (ModRenaming(..), PackageFlag(..), PackageArg(..))
import Module (UnitId, stringToUnitId)
import qualified System.Directory as Dir
import System.FilePath

-- | Orphan instances for debugging
instance Show PackageFlag where
    show = prettyPrint

-- | Compiler run configuration for DAML-GHC.
data Options = Options
  { optImportPath :: [FilePath]
    -- ^ import path for both user modules and standard library
  , optPackageDbs :: [FilePath]
    -- ^ User-specified package databases that will be loaded.
    -- This should not contain the LF version suffix. We will append this at the usesite.
  , optStablePackages :: Maybe FilePath
    -- ^ The directory in which stable DALF packages are located.
  , optMbPackageName :: Maybe LF.PackageName
    -- ^ Name of the package (version not included, so this is not the unit id)
  , optMbPackageVersion :: Maybe LF.PackageVersion
    -- ^ Version of the package
  , optIfaceDir :: Maybe FilePath
    -- ^ directory to write interface files to. If set to `Nothing` we default to <current working dir>.daml/interfaces.
  , optPackageImports :: [PackageFlag]
    -- ^ list of explicit package imports and modules with aliases. The boolean flag controls
    -- whether modules without given alias are visible.
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
  , optScenarioService :: EnableScenarioService
    -- ^ Controls whether the scenario service is started.
  , optSkipScenarioValidation :: SkipScenarioValidation
    -- ^ Controls whether the scenario service server run package validations.
    -- This is mostly used to run additional checks on CI while keeping the IDE fast.
  , optDlintUsage :: DlintUsage
  -- ^ Information about dlint usage.
  , optIsGenerated :: Bool
    -- ^ Whether we're compiling generated code. Then we allow internal imports.
  , optDflagCheck :: Bool
    -- ^ Whether to check dflags. In some cases we want to turn this check of. For example when
    -- migrating or running the daml doc test.
  , optCoreLinting :: Bool
    -- ^ Whether to enable linting of the generated GHC Core. (Used in testing.)
  , optHaddock :: Haddock
    -- ^ Whether to enable lexer option `Opt_Haddock` (default is `Haddock False`).
  , optCppPath :: Maybe FilePath
    -- ^ Enable CPP, by giving filepath to the executable.
  , optIncrementalBuild :: IncrementalBuild
  -- ^ Whether to do an incremental on-disk build as opposed to keeping everything in memory.
  , optInferDependantPackages :: InferDependantPackages
  -- ^ Whether to infer --package flags from deps/data-deps contained in daml.yaml
  , optEnableOfInterestRule :: Bool
  -- ^ Whether we should enable the of interest rule that automatically compiles all
  -- modules to DALFs or not. This is required in the IDE but we can disable it
  -- in other cases, e.g., daml-docs.
  } deriving Show

newtype IncrementalBuild = IncrementalBuild { getIncrementalBuild :: Bool }
  deriving Show

newtype InferDependantPackages = InferDependantPackages { getInferDependantPackages :: Bool }
  deriving Show

newtype Haddock = Haddock Bool
  deriving Show

data DlintUsage
  = DlintEnabled { dlintUseDataDir :: FilePath, dlintAllowOverrides :: Bool }
  | DlintDisabled
  deriving Show

newtype SkipScenarioValidation = SkipScenarioValidation { getSkipScenarioValidation :: Bool }
  deriving Show

newtype EnableScenarioService = EnableScenarioService { getEnableScenarioService :: Bool }
    deriving Show

damlArtifactDir :: FilePath
damlArtifactDir = ".daml"

-- | The project package database path relative to the project root.
projectPackageDatabase :: FilePath
projectPackageDatabase = damlArtifactDir </> "package-database"

ifaceDir :: FilePath
ifaceDir = damlArtifactDir </> "interfaces"

genDir :: FilePath
genDir = damlArtifactDir </> "generated"

distDir :: FilePath
distDir = damlArtifactDir </> "dist"

-- | Packages that we ship with the compiler.
basePackages :: [String]
basePackages = ["daml-prim", "daml-stdlib"]

-- | Find the builtin package dbs if the exist.
locateBuiltinPackageDbs :: Maybe NormalizedFilePath -> IO [FilePath]
locateBuiltinPackageDbs mbProjRoot = do
    -- package db for daml-stdlib and daml-prim
    internalPackageDb <- fmap (</> "pkg-db_dir") $ locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "pkg-db")
    -- If these directories do not exist, we just discard them.
    filterM Dir.doesDirectoryExist (internalPackageDb : [fromNormalizedFilePath projRoot </> projectPackageDatabase | Just projRoot <- [mbProjRoot]])

-- Given the target LF version and the package dbs specified by the user, return the versioned package dbs
-- including builtin package dbs.
getPackageDbs :: LF.Version -> Maybe NormalizedFilePath -> [FilePath] -> IO [FilePath]
getPackageDbs version mbProjRoot userPkgDbs = do
    builtinPkgDbs <- locateBuiltinPackageDbs mbProjRoot
    pure $ map (</> renderPretty version) (builtinPkgDbs ++ userPkgDbs)

defaultOptions :: Maybe LF.Version -> Options
defaultOptions mbVersion =
    Options
        { optImportPath = []
        , optPackageDbs = []
        , optStablePackages = Nothing
        , optMbPackageName = Nothing
        , optMbPackageVersion = Nothing
        , optIfaceDir = Nothing
        , optPackageImports = []
        , optShakeProfiling = Nothing
        , optThreads = 1
        , optDamlLfVersion = fromMaybe LF.versionDefault mbVersion
        , optDebug = False
        , optGhcCustomOpts = []
        , optScenarioService = EnableScenarioService True
        , optSkipScenarioValidation = SkipScenarioValidation False
        , optDlintUsage = DlintDisabled
        , optIsGenerated = False
        , optDflagCheck = True
        , optCoreLinting = False
        , optHaddock = Haddock False
        , optCppPath = Nothing
        , optIncrementalBuild = IncrementalBuild False
        , optInferDependantPackages = InferDependantPackages True
        , optEnableOfInterestRule = True
        }

getBaseDir :: IO FilePath
getBaseDir = locateRunfiles (mainWorkspace </> "compiler/damlc")

pkgNameVersion :: LF.PackageName -> Maybe LF.PackageVersion -> UnitId
pkgNameVersion (LF.PackageName n) mbV =
    stringToUnitId $ T.unpack $ case mbV of
        Nothing -> n
        Just (LF.PackageVersion v) -> n <> "-" <> v

fullPkgName :: LF.PackageName -> Maybe LF.PackageVersion -> LF.PackageId -> String
fullPkgName (LF.PackageName n) mbV (LF.PackageId h) =
    T.unpack $ case mbV of
        Nothing -> n <> "-" <> h
        Just (LF.PackageVersion v) -> n <> "-" <> v <> "-" <> h

optUnitId :: Options -> Maybe UnitId
optUnitId Options{..} = fmap (\name -> pkgNameVersion name optMbPackageVersion) optMbPackageName
