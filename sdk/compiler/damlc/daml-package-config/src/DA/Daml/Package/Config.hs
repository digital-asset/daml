-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

-- | Types and functions for dealing with package config in daml.yaml
module DA.Daml.Package.Config
    ( MultiPackageConfigFields (..)
    , PackageConfigFields (..)
    , parseProjectConfig
    , overrideSdkVersion
    , withPackageConfig
    , findMultiPackageConfig
    , withMultiPackageConfig
    , checkPkgConfig
    ) where

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types

import Control.Exception.Safe (throwIO, displayException)
import Control.Monad (when)
import Control.Monad.Extra (loopM)
import Control.Monad.Trans.Class (lift)
import Control.Monad.Trans.State.Lazy
import qualified Data.Aeson as A
import qualified Data.Aeson.Key as A
import qualified Data.Aeson.KeyMap as A
import qualified Data.Aeson.Encoding as A
import Data.List.Extra (nubOrd)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import qualified Data.Set as Set
import qualified Data.Text as T
import qualified Data.Yaml as Y
import qualified Module as Ghc
import System.Directory (canonicalizePath, doesFileExist, withCurrentDirectory)
import System.FilePath (takeDirectory, (</>))
import System.IO (hPutStrLn, stderr)
import Text.Regex.TDFA
import qualified Data.SemVer as V

-- | daml.yaml config fields specific to packaging.
data PackageConfigFields = PackageConfigFields
    { pName :: LF.PackageName
    , pSrc :: String
    , pExposedModules :: Maybe [Ghc.ModuleName]
    , pVersion :: Maybe LF.PackageVersion
    -- ^ This is optional since for `damlc compile` and `damlc package`
    -- we might not have a version. In `damlc build` this is always set to `Just`.
    , pDependencies :: [String]
    , pDataDependencies :: [String]
    , pModulePrefixes :: Map Ghc.UnitId Ghc.ModuleName
    -- ^ Map from unit ids to a prefix for all modules in that package.
    -- If this is specified, all modules from the package will be remapped
    -- under the given prefix.
    , pSdkVersion :: UnresolvedReleaseVersion
    , pUpgradedPackagePath :: Maybe String
    , pTypecheckUpgrades :: Bool
    }

-- | Parse the daml.yaml for package specific config fields.
parseProjectConfig :: ProjectConfig -> Either ConfigError PackageConfigFields
parseProjectConfig project = do
    pName <- queryProjectConfigRequired ["name"] project
    pSrc <- queryProjectConfigRequired ["source"] project
    pExposedModules <-
        fmap (map Ghc.mkModuleName) <$>
        queryProjectConfig ["exposed-modules"] project
    pVersion <- Just <$> queryProjectConfigRequired ["version"] project
    pDependencies <- queryProjectConfigRequired ["dependencies"] project
    pDataDependencies <- fromMaybe [] <$> queryProjectConfig ["data-dependencies"] project
    pModulePrefixes <- fromMaybe Map.empty <$> queryProjectConfig ["module-prefixes"] project
    pSdkVersion <- queryProjectConfigRequired ["sdk-version"] project
    pUpgradedPackagePath <- queryProjectConfig ["upgrades"] project
    pTypecheckUpgrades <- fromMaybe False <$> queryProjectConfig ["typecheck-upgrades"] project
    Right PackageConfigFields {..}

checkPkgConfig :: PackageConfigFields -> [T.Text]
checkPkgConfig PackageConfigFields {pName, pVersion} =
  [ T.unlines $
  ["Invalid package name: " <> T.pack (show pName) <> ". Package names should have the format " <> packageNameRegex <> "."]
  ++ errDescription
  | not $ LF.unPackageName pName =~ packageNameRegex
  ] ++
  [ T.unlines $
  ["Invalid package version: " <> T.pack (show pVersion) <> ". Package versions should have the format " <> versionRegex <> "."]
  ++ errDescription
  | Just version <- [pVersion]
  , not $ LF.unPackageVersion version =~ versionRegex
  ]
  where
    errDescription =
      [ "You may be able to compile packages with different formats, but you will not be able to"
      , "use them as dependencies in other projects. Unsupported package names or versions may"
      , "start causing compilation errors without warning."
      ]
    versionRegex = "^(0|[1-9][0-9]*)(\\.(0|[1-9][0-9]*))*$" :: T.Text
    packageNameRegex = "^[A-Za-z][A-Za-z0-9]*(\\-[A-Za-z][A-Za-z0-9]*)*$" :: T.Text

data MultiPackageConfigFields = MultiPackageConfigFields
    { mpPackagePaths :: [FilePath]
    , mpDars :: [FilePath]
    }

-- | Intermediate of MultiPackageConfigFields that carries links to other config files, before being flattened into a single MultiPackageConfigFields
data MultiPackageConfigFieldsIntermediate = MultiPackageConfigFieldsIntermediate
    { mpiConfigFields :: MultiPackageConfigFields
    , mpiOtherConfigFiles :: [FilePath]
    }

-- | Parse the multi-package.yaml file for auto rebuilds/IDE intelligence in multi-package projects
parseMultiPackageConfig :: MultiPackageConfig -> Either ConfigError MultiPackageConfigFieldsIntermediate
parseMultiPackageConfig multiPackage = do
    mpPackagePaths <- fromMaybe [] <$> queryMultiPackageConfig ["packages"] multiPackage
    mpDars <- fromMaybe [] <$> queryMultiPackageConfig ["dars"] multiPackage
    let mpiConfigFields = MultiPackageConfigFields {..}
    mpiOtherConfigFiles <- fromMaybe [] <$> queryMultiPackageConfig ["projects"] multiPackage
    Right MultiPackageConfigFieldsIntermediate {..}

overrideSdkVersion :: PackageConfigFields -> IO PackageConfigFields
overrideSdkVersion pkgConfig = do
    sdkVersionM <- getSdkVersionMaybe
    case sdkVersionM of
        Nothing ->
            pure pkgConfig
        Just (Left sdkVersionError) -> do
            hPutStrLn stderr $ unwords
                [ "Warning: Using SDK version "
                , V.toString (unwrapUnresolvedReleaseVersion (pSdkVersion pkgConfig))
                , " from config instead of "
                , sdkVersionEnvVar
                , " enviroment variable because it doesn't contain a valid version.\n"
                , displayException sdkVersionError
                ]
            pure pkgConfig
        Just (Right sdkVersion) -> do
            when (pSdkVersion pkgConfig /= sdkVersion) $
                hPutStrLn stderr $ unwords
                    [ "Warning: Using SDK version"
                    , V.toString (unwrapUnresolvedReleaseVersion sdkVersion)
                    , "from"
                    , sdkVersionEnvVar
                    , "enviroment variable instead of SDK version"
                    , V.toString (unwrapUnresolvedReleaseVersion (pSdkVersion pkgConfig))
                    , "from"
                    , projectConfigName
                    , "config file."
                    ]
            pure pkgConfig { pSdkVersion = sdkVersion }

-- If any of these fields are present in a daml.yaml, it is considered a "full" daml.yaml
-- rather than a verion/options only file, and as such, cannot be ignored by processes like multi-build.
-- Note that we do not handle this by restricting unknown fields, as our daml.yaml parsing has always been
-- lax.
-- Fields "sdk-version", and "build-options" are intentionally missing.
fullDamlYamlFields :: Set.Set String
fullDamlYamlFields = Set.fromList
  [ "name"
  , "source"
  , "exposed-modules"
  , "version"
  , "dependencies"
  , "data-dependencies"
  , "module-prefixes"
  , "upgrades"
  , "typecheck-upgrades"
  ]

withPackageConfig :: ProjectPath -> (PackageConfigFields -> IO a) -> IO a
withPackageConfig projectPath f = do
    project <- readProjectConfig projectPath
    -- If the config only has the sdk-version, it is "valid" but not usable for package config. It should be handled explicitly
    case unwrapProjectConfig project of
      A.Object (fmap A.toString . A.keys -> strKeys) | all (`Set.notMember` fullDamlYamlFields) strKeys ->
        throwIO $ ConfigFileInvalid "project" $ Y.InvalidYaml $ Just $ Y.YamlException $
          projectConfigName ++ " is a packageless daml.yaml, cannot be used for package config."
      _ -> pure ()

    pkgConfig <- either throwIO pure (parseProjectConfig project)
    pkgConfig' <- overrideSdkVersion pkgConfig
    f pkgConfig'

-- Traverses up the directory tree from current project path and returns the project path of the "nearest" project.yaml
-- Stops at root, but also won't pick any files it doesn't have permission to search
findMultiPackageConfig :: ProjectPath -> IO (Maybe ProjectPath)
findMultiPackageConfig projectPath = do
  filePath <- canonicalizePath $ unwrapProjectPath projectPath
  flip loopM filePath $ \path -> do
    hasMultiPackage <- doesFileExist $ path </> multiPackageConfigName
    if hasMultiPackage
      then pure $ Right $ Just $ ProjectPath path
      else
        let newPath = takeDirectory path
        in pure $ if path == newPath then Right Nothing else Left newPath

canonicalizeMultiPackageConfigIntermediate :: ProjectPath -> MultiPackageConfigFieldsIntermediate -> IO MultiPackageConfigFieldsIntermediate
canonicalizeMultiPackageConfigIntermediate projectPath (MultiPackageConfigFieldsIntermediate (MultiPackageConfigFields packagePaths darPaths) multiPackagePaths) =
  withCurrentDirectory (unwrapProjectPath projectPath) $ do
    MultiPackageConfigFieldsIntermediate
      <$> (MultiPackageConfigFields <$> traverse canonicalizePath packagePaths <*> traverse canonicalizePath darPaths)
      <*> traverse canonicalizePath multiPackagePaths

-- Given some computation to give a result and dependencies, we explore the entire cyclic graph to give the combined
-- result from every node without revisiting the same node multiple times
exploreAndFlatten :: forall a b. Eq a => a -> (a -> IO ([a], b)) -> IO [b]
exploreAndFlatten start eval = evalStateT (go start) []
  where
    go :: a -> StateT [a] IO [b]
    go v = do
      explored <- gets $ elem v
      if explored
        then pure []
        else do
          modify (v :)
          (as, b) <- lift $ eval v
          bs <- concat <$> traverse go as
          pure $ b : bs

fullParseMultiPackageConfig :: ProjectPath -> IO MultiPackageConfigFields
fullParseMultiPackageConfig startPath = do
  mpcs <- exploreAndFlatten startPath $ \projectPath -> do
    multiPackage <- readMultiPackageConfig projectPath
    multiPackageConfigI <- either throwIO pure (parseMultiPackageConfig multiPackage)
    canonMultiPackageConfigI <- canonicalizeMultiPackageConfigIntermediate projectPath multiPackageConfigI
    pure (ProjectPath <$> mpiOtherConfigFiles canonMultiPackageConfigI, mpiConfigFields canonMultiPackageConfigI)

  pure $ MultiPackageConfigFields (nubOrd $ concatMap mpPackagePaths mpcs) (nubOrd $ concatMap mpDars mpcs)

-- Gives the filepath where the multipackage was found if its not the same as project path.
withMultiPackageConfig :: ProjectPath -> (MultiPackageConfigFields -> IO a) -> IO a
withMultiPackageConfig projectPath f = fullParseMultiPackageConfig projectPath >>= f

-- | Orphans because I’m too lazy to newtype everything.
instance A.FromJSON Ghc.ModuleName where
    parseJSON = A.withText "ModuleName" $ \t -> pure $ Ghc.mkModuleName (T.unpack t)

instance A.ToJSON Ghc.ModuleName where
    toJSON m = A.toJSON (Ghc.moduleNameString m)

instance A.FromJSON Ghc.UnitId where
    parseJSON = A.withText "UnitId" $ \t -> pure $ Ghc.stringToUnitId (T.unpack t)

instance A.FromJSONKey Ghc.UnitId where
    fromJSONKey = A.FromJSONKeyText $ \t -> Ghc.stringToUnitId (T.unpack t)

instance A.ToJSON Ghc.UnitId where
    toJSON unitId = A.toJSON (Ghc.unitIdString unitId)

instance A.ToJSONKey Ghc.UnitId where
    toJSONKey =
        A.ToJSONKeyText (A.fromString . Ghc.unitIdString) (A.text . T.pack . Ghc.unitIdString)
