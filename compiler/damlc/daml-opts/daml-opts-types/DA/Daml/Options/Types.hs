-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Options.Types
    ( Options(..)
    , EnableScenarioService(..)
    , ScenarioValidation(..)
    , DlintUsage(..)
    , Haddock(..)
    , defaultOptionsIO
    , defaultOptions
    , mkOptions
    , getBaseDir
    , damlArtifactDir
    , projectPackageDatabase
    , ifaceDir
    , distDir
    , genDir
    , basePackages
    ) where

import Control.Monad.Reader
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Pretty (renderPretty)
import Data.Maybe
import qualified System.Directory as Dir
import System.FilePath

-- | Compiler run configuration for DAML-GHC.
data Options = Options
  { optImportPath :: [FilePath]
    -- ^ import path for both user modules and standard library
  , optPackageDbs :: [FilePath]
    -- ^ package databases that will be loaded
  , optMbPackageName :: Maybe String
    -- ^ compile in the context of the given package name and create interface files
  , optWriteInterface :: Bool
    -- ^ whether to write interface files or not.
  , optIfaceDir :: Maybe FilePath
    -- ^ alternative directory to write interface files to. Default is <current working dir>.daml/interfaces.
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
  , optScenarioService :: EnableScenarioService
    -- ^ Controls whether the scenario service is started.
  , optScenarioValidation :: ScenarioValidation
    -- ^ Controls whether the scenario service server runs all checks
    -- or only a subset of them. This is mostly used to run additional
    -- checks on CI while keeping the IDE fast.
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
  } deriving Show

newtype Haddock = Haddock Bool
  deriving Show

data DlintUsage
  = DlintEnabled { dlintUseDataDir :: FilePath, dlintAllowOverrides :: Bool }
  | DlintDisabled
  deriving Show

data ScenarioValidation
    = ScenarioValidationLight
    | ScenarioValidationFull
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

-- | Check that import paths and package db directories exist and add
-- the default package db if it exists
mkOptions :: Options -> IO Options
mkOptions opts@Options {..} = do
    mapM_ checkDirExists $ optImportPath <> optPackageDbs
    defaultPkgDb <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "pkg-db")
    let defaultPkgDbDir = defaultPkgDb </> "pkg-db_dir"
    pkgDbs <- filterM Dir.doesDirectoryExist [defaultPkgDbDir, projectPackageDatabase]
    case optDlintUsage of
      DlintEnabled dir _ -> checkDirExists dir
      DlintDisabled -> return ()
    pure opts {optPackageDbs = map (</> versionSuffix) $ pkgDbs ++ optPackageDbs}
  where checkDirExists f =
          Dir.doesDirectoryExist f >>= \ok ->
          unless ok $ fail $ "Required directory does not exist: " <> f
        versionSuffix = renderPretty optDamlLfVersion

-- | Default configuration for the compiler with package database set
-- according to daml-lf version and located runfiles. If the version
-- argument is Nothing it is set to the default daml-lf
-- version. Linting is enabled but not '.dlint.yaml' overrides.
defaultOptionsIO :: Maybe LF.Version -> IO Options
defaultOptionsIO mbVersion = do
  dlintDataDir <-locateRunfiles $ mainWorkspace </> "compiler/damlc/daml-ide-core"
  mkOptions $ (defaultOptions mbVersion){optDlintUsage=DlintEnabled dlintDataDir False}

defaultOptions :: Maybe LF.Version -> Options
defaultOptions mbVersion =
    Options
        { optImportPath = []
        , optPackageDbs = []
        , optMbPackageName = Nothing
        , optWriteInterface = False
        , optIfaceDir = Nothing
        , optHideAllPkgs = False
        , optPackageImports = []
        , optShakeProfiling = Nothing
        , optThreads = 1
        , optDamlLfVersion = fromMaybe LF.versionDefault mbVersion
        , optDebug = False
        , optGhcCustomOpts = []
        , optScenarioService = EnableScenarioService True
        , optScenarioValidation = ScenarioValidationFull
        , optDlintUsage = DlintDisabled
        , optIsGenerated = False
        , optDflagCheck = True
        , optCoreLinting = False
        , optHaddock = Haddock False
        }

getBaseDir :: IO FilePath
getBaseDir = locateRunfiles (mainWorkspace </> "compiler/damlc")
