-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE DataKinds #-}

module DA.Daml.Options.Types
    ( Options(..)
    , EnableScriptService(..)
    , EnableInterfaces(..)
    , StudioAutorunAllScripts(..)
    , SkipScriptValidation(..)
    , DlintRulesFile(..)
    , DlintHintFiles(.., NoDlintHintFiles)
    , DlintOptions(..)
    , DlintUsage(..)
    , Haddock(..)
    , IncrementalBuild(..)
    , IgnorePackageMetadata(..)
    , PackageFlag(..)
    , ModRenaming(..)
    , PackageArg(..)
    , IgnoreDataDepVisibility(..)
    , ForceUtilityPackage(..)
    , ExplicitSerializable(..)
    , defaultOptions
    , damlArtifactDir
    , packageDatabasePath
    , packageDependenciesDatabasePath
    , ifaceDir
    , distDir
    , genDir
    , basePackages
    , getPackageDbs
    , pkgNameVersion
    , fullPkgName
    , optUnitId
    , getLogger
    , UpgradeInfo (..)
    , defaultUiTypecheckUpgrades
    , defaultUiWarnBadInterfaceInstances
    , defaultUiWarnBadExceptions
    , defaultUpgradeInfo
    , allWarningFlagParsers
    , warningFlagParserInlineDamlCustom
    , inlineDamlCustomWarningToGhcFlag
    , InlineDamlCustomWarnings (..)
    ) where

import Control.Monad.Reader
import DA.Bazel.Runfiles
import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Project.Types (PackagePath)
import DA.Daml.Resolution.Config (ResolutionData)
import DA.Pretty
import qualified DA.Service.Logger as Logger
import qualified DA.Service.Logger.Impl.IO as Logger.IO
import Data.Maybe
import qualified Data.Text as T
import Development.IDE.GHC.Util (prettyPrint)
import Development.IDE.Types.Location
import DynFlags (ModRenaming(..), PackageFlag(..), PackageArg(..))
import Module (UnitId, stringToUnitId)
import qualified System.Directory as Dir
import System.FilePath
import qualified DA.Daml.LF.TypeChecker.Error.WarningFlags as WarningFlags
import Data.HList
import qualified DA.Daml.LF.TypeChecker.Error as TypeCheckerError
import qualified DA.Daml.LFConversion.Errors as LFConversion
import DA.Daml.LF.TypeChecker.Upgrade (UpgradeInfo(..))

-- | Orphan instances for debugging
instance Show PackageFlag where
    show = prettyPrint

-- | Compiler run configuration for Daml-GHC.
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
  , optMbPackageConfigPath :: Maybe PackagePath
    -- ^ Path to the daml.yaml
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
    -- ^ The target Daml-LF version
  , optLogLevel :: Logger.Priority
    -- ^ Min log level that we display
  , optDetailLevel :: PrettyLevel
    -- ^ Level of detail in pretty printed output
  , optGhcCustomOpts :: [String]
    -- ^ custom options, parsed by GHC option parser, overriding DynFlags
  , optScriptService :: EnableScriptService
    -- ^ Controls whether the script service is started.
  , optEnableInterfaces :: EnableInterfaces
    -- ^ Whether interfaces should be allowed as a language feature. Off by default.
  , optTestFilter :: T.Text -> Bool
    -- ^ Only execute tests with a name for which the given predicate holds.
  , optSkipScriptValidation :: SkipScriptValidation
    -- ^ Controls whether the script service server run package validations.
    -- This is mostly used to run additional checks on CI while keeping the IDE fast.
  , optDlintUsage :: DlintUsage
    -- ^ dlint configuration.
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
  , optIgnorePackageMetadata :: IgnorePackageMetadata
  -- ^ Whether to ignore the package metadata generated from the daml.yaml
  -- This is set to True when building data-dependency packages where we
  -- have precise package flags and don’t want to use the daml.yaml from the
  -- main package.
  , optEnableOfInterestRule :: Bool
  -- ^ Whether we should enable the of interest rule that automatically compiles all
  -- modules to DALFs or not. This is required in the IDE but we can disable it
  -- in other cases, e.g., daml-docs.
  , optAccessTokenPath :: Maybe FilePath
  -- ^ Path to a file containing an access JWT token. This is used for building to query/fetch
  -- packages from remote ledgers.
  , optHideUnitId :: Bool
  -- ^ When running in IDE, some rules need access to the package name and version, but we don't want to use own
  -- unit-id, as script service assume it will be "main"
  , optUpgradeInfo :: UpgradeInfo
  , optTypecheckerWarningFlags :: WarningFlags.WarningFlags TypeCheckerError.ErrorOrWarning
  , optLfConversionWarningFlags :: WarningFlags.WarningFlags LFConversion.ErrorOrWarning
  , optInlineDamlCustomWarningFlags :: WarningFlags.WarningFlags InlineDamlCustomWarnings
  , optIgnoreDataDepVisibility :: IgnoreDataDepVisibility
  , optForceUtilityPackage :: ForceUtilityPackage
  , optResolutionData :: Maybe ResolutionData
  , optExplicitSerializable :: ExplicitSerializable
  }

data InlineDamlCustomWarnings
  = DisableDeprecatedExceptions
  | CryptoTextIsAlpha
  deriving (Enum, Bounded, Ord, Eq, Show)

warningFlagParserInlineDamlCustom :: WarningFlags.WarningFlagParser InlineDamlCustomWarnings
warningFlagParserInlineDamlCustom = WarningFlags.mkWarningFlagParser
  (\case
    DisableDeprecatedExceptions -> WarningFlags.AsWarning
    CryptoTextIsAlpha -> WarningFlags.AsWarning)
  [ WarningFlags.WarningFlagSpec "deprecated-exceptions" False $ \case
      DisableDeprecatedExceptions -> True
      _ -> False
  , WarningFlags.WarningFlagSpec "crypto-text-is-alpha" False $ \case
      CryptoTextIsAlpha -> True
      _ -> False
  ]

inlineDamlCustomWarningToGhcFlag :: WarningFlags.WarningFlags InlineDamlCustomWarnings -> [String]
inlineDamlCustomWarningToGhcFlag flags = map go [minBound..maxBound]
  where
    toName :: InlineDamlCustomWarnings -> String
    toName DisableDeprecatedExceptions = "x-exceptions"
    toName CryptoTextIsAlpha = "x-crypto"

    go inlineWarning =
      case WarningFlags.getWarningStatus flags inlineWarning of
        WarningFlags.AsError -> "-Werror=" <> toName inlineWarning
        WarningFlags.AsWarning -> "-W" <> toName inlineWarning
        WarningFlags.Hidden -> "-Wno-" <> toName inlineWarning

allWarningFlagParsers :: WarningFlags.WarningFlagParsers '[InlineDamlCustomWarnings, TypeCheckerError.ErrorOrWarning, LFConversion.ErrorOrWarning]
allWarningFlagParsers =
  WarningFlags.combineParsers
    (ProdT warningFlagParserInlineDamlCustom
      (ProdT TypeCheckerError.warningFlagParser
        (ProdT LFConversion.warningFlagParser ProdZ)))

newtype IncrementalBuild = IncrementalBuild { getIncrementalBuild :: Bool }
  deriving Show

newtype IgnorePackageMetadata = IgnorePackageMetadata { getIgnorePackageMetadata :: Bool }
  deriving Show

newtype IgnoreDataDepVisibility = IgnoreDataDepVisibility { getIgnoreDataDepVisibility :: Bool }
  deriving Show

newtype Haddock = Haddock Bool
  deriving Show

-- | The dlint rules file is a dlint yaml file that's used as the base for
-- the rules used during linting. Really there is no difference between the
-- rules file and the other hint files, but it is useful to specify them
-- separately since this one can act as the base, allowing the other hint files
-- to selectively ignore individual rules.
data DlintRulesFile
  = DefaultDlintRulesFile
    -- ^ "WORKSPACE/compiler/damlc/daml-ide-core/dlint.yaml"
  | ExplicitDlintRulesFile FilePath
    -- ^ User-provided rules file
  deriving Show

data DlintHintFiles
  = ImplicitDlintHintFile
    -- ^ First existing file of
    --    *       ".dlint.yaml"
    --    *    "../.dlint.yaml"
    --    * "../../.dlint.yaml"
    --    * ...
    --    * "~/.dlint.yaml"
  | ExplicitDlintHintFiles [FilePath]
  deriving Show

pattern NoDlintHintFiles :: DlintHintFiles
pattern NoDlintHintFiles = ExplicitDlintHintFiles []

data DlintOptions = DlintOptions
  { dlintRulesFile :: DlintRulesFile
  , dlintHintFiles :: DlintHintFiles
  }
  deriving Show

data DlintUsage
  = DlintEnabled DlintOptions
  | DlintDisabled
  deriving Show

newtype SkipScriptValidation = SkipScriptValidation { getSkipScriptValidation :: Bool }
  deriving Show

newtype EnableScriptService = EnableScriptService { getEnableScriptService :: Bool }
    deriving Show

newtype StudioAutorunAllScripts = StudioAutorunAllScripts { getStudioAutorunAllScripts :: Bool }
    deriving Show

newtype EnableInterfaces = EnableInterfaces { getEnableInterfaces :: Bool }
    deriving Show

newtype ForceUtilityPackage = ForceUtilityPackage { getForceUtilityPackage :: Bool }
    deriving Show

newtype ExplicitSerializable = ExplicitSerializable { getExplicitSerializable :: Bool }
    deriving Show

damlArtifactDir :: FilePath
damlArtifactDir = ".daml"

-- | The package package database path relative to the package root.
packageDatabasePath :: FilePath
packageDatabasePath = damlArtifactDir </> "package-database"

packageDependenciesDatabasePath :: FilePath
packageDependenciesDatabasePath = damlArtifactDir </> "dependencies"

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
    internalPackageDb <- locateResource Resource
      -- //compiler/damlc/pkg-db
      { resourcesPath = "pkg-db_dir"
        -- In a packaged application, the directory "pkg-db_dir" is preserved
        -- underneath the resources directory because it is the target's
        -- only output (even if it's a directory).
        -- See @bazel_tools/packaging/packaging.bzl@.
      , runfilesPathPrefix = mainWorkspace </> "compiler" </> "damlc" </> "pkg-db"
      }
    -- If these directories do not exist, we just discard them.
    filterM Dir.doesDirectoryExist (internalPackageDb : [fromNormalizedFilePath packageRoot </> packageDatabasePath | Just packageRoot <- [mbProjRoot]])

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
        , optMbPackageConfigPath = Nothing
        , optIfaceDir = Nothing
        , optPackageImports = []
        , optShakeProfiling = Nothing
        , optThreads = 1
        , optDamlLfVersion = fromMaybe LF.defaultLfVersion mbVersion
        , optLogLevel = Logger.Info
        , optDetailLevel = DA.Pretty.prettyNormal
        , optGhcCustomOpts = []
        , optScriptService = EnableScriptService True
        , optEnableInterfaces = EnableInterfaces True
        , optTestFilter = const True
        , optSkipScriptValidation = SkipScriptValidation False
        , optDlintUsage = DlintDisabled
        , optIsGenerated = False
        , optDflagCheck = True
        , optCoreLinting = False
        , optHaddock = Haddock False
        , optCppPath = Nothing
        , optIncrementalBuild = IncrementalBuild False
        , optIgnorePackageMetadata = IgnorePackageMetadata False
        , optEnableOfInterestRule = False
        , optAccessTokenPath = Nothing
        , optHideUnitId = False
        , optUpgradeInfo = defaultUpgradeInfo
        , optTypecheckerWarningFlags = WarningFlags.mkWarningFlags TypeCheckerError.warningFlagParser []
        , optLfConversionWarningFlags = WarningFlags.mkWarningFlags LFConversion.warningFlagParser []
        , optInlineDamlCustomWarningFlags = WarningFlags.mkWarningFlags warningFlagParserInlineDamlCustom []
        , optIgnoreDataDepVisibility = IgnoreDataDepVisibility False
        , optForceUtilityPackage = ForceUtilityPackage False
        , optResolutionData = Nothing
        , optExplicitSerializable = ExplicitSerializable False
        }

defaultUpgradeInfo :: UpgradeInfo
defaultUpgradeInfo = UpgradeInfo
    { uiUpgradedPackagePath = Nothing
    , uiTypecheckUpgrades = defaultUiTypecheckUpgrades
    }

defaultUiTypecheckUpgrades, defaultUiWarnBadInterfaceInstances, defaultUiWarnBadExceptions :: Bool
defaultUiTypecheckUpgrades = True
defaultUiWarnBadInterfaceInstances = False
defaultUiWarnBadExceptions = False

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
optUnitId Options{..} = guard (not optHideUnitId) >> fmap (\name -> pkgNameVersion name optMbPackageVersion) optMbPackageName

getLogger :: Options -> T.Text -> IO (Logger.Handle IO)
getLogger Options {optLogLevel} name = Logger.IO.newStderrLogger optLogLevel name
