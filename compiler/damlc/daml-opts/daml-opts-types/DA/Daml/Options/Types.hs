-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.Options.Types
    ( Options(..)
    , EnableScenarioService(..)
    , SkipScenarioValidation(..)
    , DlintUsage(..)
    , Haddock(..)
    , IncrementalBuild(..)
    , PackageImport(..)
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
import Data.Bifunctor
import Data.Maybe
import GHC.Show
import qualified Module as GHC
import qualified System.Directory as Dir
import System.Environment
import System.FilePath

data PackageImport = PackageImport
  { pkgImportUnitId :: GHC.UnitId
  , pkgImportExposeImplicit :: Bool
  -- ^ Expose modules that do not have explicit explicit renamings.
  , pkgImportModRenamings :: [(GHC.ModuleName, GHC.ModuleName)]
  -- ^ Expose module m under name n
  }

-- We handwrite the orphan instance to avoid introducing an orphan for GHC.ModuleName
instance Show PackageImport where
    showsPrec prec PackageImport{..} = showParen (prec > appPrec) $
        showString "PackageImport {" .
        showString "pkgImportUnitId = " .
        shows pkgImportUnitId .
        showCommaSpace .
        showString "pkgImportExposeImplicit = " .
        shows pkgImportExposeImplicit .
        showCommaSpace .
        showString "pkgImportModRenamings" .
        shows (map (bimap GHC.moduleNameString GHC.moduleNameString) pkgImportModRenamings) .
        showString "}"
     where appPrec = 10

-- | Compiler run configuration for DAML-GHC.
data Options = Options
  { optImportPath :: [FilePath]
    -- ^ import path for both user modules and standard library
  , optPackageDbs :: [FilePath]
    -- ^ package databases that will be loaded
  , optStablePackages :: Maybe FilePath
    -- ^ The directory in which stable DALF packages are located.
  , optMbPackageName :: Maybe String
    -- ^ compile in the context of the given package name and create interface files
  , optWriteInterface :: Bool
    -- ^ whether to write interface files or not during `damlc compile`. This is _only_
    -- relevant for `compile` which at the moment is only used for building daml-stdlib
    -- and daml-prim.
  , optIfaceDir :: Maybe FilePath
    -- ^ alternative directory to write interface files to. Default is <current working dir>.daml/interfaces.
  , optHideAllPkgs :: Bool
    -- ^ hide all imported packages
  , optPackageImports :: [PackageImport]
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
  , optGhcVersionFile :: Maybe FilePath
    -- ^ Path to "ghcversion.h". Needed for running CPP. We ship this
    -- as part of our runfiles. This is set by 'mkOptions'.
  , optIncrementalBuild :: IncrementalBuild
  -- ^ Whether to do an incremental on-disk build as opposed to keeping everything in memory.
  } deriving Show

newtype IncrementalBuild = IncrementalBuild { getIncrementalBuild :: Bool }
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
    -- On Windows, looking up mainWorkspace/compiler/damlc and then appeanding stable-packages doesn’t work.
    -- On the other hand, looking up the full path directly breaks our resources logic for dist tarballs.
    -- Therefore we first try stable-packages and then fall back to resources if that does not exist
    stablePackages <- do
        execPath <- getExecutablePath
        let jarResources = takeDirectory execPath </> "resources"
        hasJarResources <- Dir.doesDirectoryExist jarResources
        if hasJarResources
           then pure (jarResources </> "stable-packages")
           else locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "stable-packages")
    stablePackagesExist <- Dir.doesDirectoryExist stablePackages
    let mbStablePackages = do
            guard stablePackagesExist
            pure stablePackages
    ghcVersionFile <- locateRunfiles (mainWorkspace </> "compiler" </> "damlc" </> "ghcversion.h")

    pure opts {
        optPackageDbs = map (</> versionSuffix) $ pkgDbs ++ optPackageDbs,
        optStablePackages = mbStablePackages,
        optGhcVersionFile = Just ghcVersionFile
    }
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
        , optStablePackages = Nothing
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
        , optSkipScenarioValidation = SkipScenarioValidation False
        , optDlintUsage = DlintDisabled
        , optIsGenerated = False
        , optDflagCheck = True
        , optCoreLinting = False
        , optHaddock = Haddock False
        , optCppPath = Nothing
        , optGhcVersionFile = Nothing
        , optIncrementalBuild = IncrementalBuild False
        }

getBaseDir :: IO FilePath
getBaseDir = locateRunfiles (mainWorkspace </> "compiler/damlc")
