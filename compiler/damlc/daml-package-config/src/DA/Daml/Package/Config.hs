-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# OPTIONS_GHC -Wno-orphans #-}

-- | Types and functions for dealing with package config in daml.yaml
module DA.Daml.Package.Config
    ( MultiPackageConfigFields (..)
    , PackageConfigFields (..)
    , PackageSdkVersion (..)
    , parseProjectConfig
    , overrideSdkVersion
    , withPackageConfig
    , withMultiPackageConfig
    , checkPkgConfig
    ) where

import qualified DA.Daml.LF.Ast as LF
import DA.Daml.Project.Config
import DA.Daml.Project.Consts
import DA.Daml.Project.Types

import Control.Exception.Safe (throwIO)
import Control.Monad (when)
import Control.Monad.Extra (loopM)
import qualified Data.Aeson as A
import qualified Data.Aeson.Key as A
import qualified Data.Aeson.Encoding as A
import Data.List (elemIndex, nub)
import Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import Data.Maybe (fromMaybe)
import qualified Data.Text as T
import qualified Data.Yaml as Y
import qualified Module as Ghc
import System.Directory (canonicalizePath, listDirectory, withCurrentDirectory)
import System.FilePath (takeDirectory)
import System.IO (hPutStrLn, stderr)
import Text.Regex.TDFA

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
    , pSdkVersion :: PackageSdkVersion
    , pUpgradedPackagePath :: Maybe String
    }

-- | SDK version for package.
newtype PackageSdkVersion = PackageSdkVersion
    { unPackageSdkVersion :: String
    } deriving (Eq, Y.FromJSON)

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
    }

-- | Intermediate of MultiPackageConfigFields that carries links to other config files, before being flattened into a single MultiPackageConfigFields
data MultiPackageConfigFieldsIntermediate = MultiPackageConfigFieldsIntermediate
    { mpiConfigFields :: MultiPackageConfigFields
    , mpiOtherConfigFiles :: [FilePath]
    }

-- | Parse the multi-package.yaml file for auto rebuilds/IDE intelligence in multi-package projects
parseMultiPackageConfig :: MultiPackageConfig -> Either ConfigError MultiPackageConfigFieldsIntermediate
parseMultiPackageConfig multiPackage = do
    mpiConfigFields <- MultiPackageConfigFields . fromMaybe [] <$> queryMultiPackageConfig ["packages"] multiPackage
    mpiOtherConfigFiles <- fromMaybe [] <$> queryMultiPackageConfig ["projects"] multiPackage
    Right MultiPackageConfigFieldsIntermediate {..}

overrideSdkVersion :: PackageConfigFields -> IO PackageConfigFields
overrideSdkVersion pkgConfig = do
    sdkVersionM <- getSdkVersionMaybe
    case sdkVersionM of
        Nothing ->
            pure pkgConfig
        Just sdkVersion -> do
            when (pSdkVersion pkgConfig /= PackageSdkVersion sdkVersion) $
                hPutStrLn stderr $ unwords
                    [ "Warning: Using SDK version"
                    , sdkVersion
                    , "from"
                    , sdkVersionEnvVar
                    , "enviroment variable instead of SDK version"
                    , unPackageSdkVersion (pSdkVersion pkgConfig)
                    , "from"
                    , projectConfigName
                    , "config file."
                    ]
            pure pkgConfig { pSdkVersion = PackageSdkVersion sdkVersion }

withPackageConfig :: ProjectPath -> (PackageConfigFields -> IO a) -> IO a
withPackageConfig projectPath f = do
    project <- readProjectConfig projectPath
    pkgConfig <- either throwIO pure (parseProjectConfig project)
    pkgConfig' <- overrideSdkVersion pkgConfig
    f pkgConfig'

-- Traverses up the directory tree from current project path and returns the project path of the "nearest" project.yaml
-- Stops at root, errors on failure to find.
findMultiPackageConfigProjectPath :: FilePath -> IO ProjectPath
findMultiPackageConfigProjectPath filePath = flip loopM filePath $ \path -> do
  hasMultiPackage <- elem multiPackageConfigName <$> listDirectory path
  if hasMultiPackage
    then pure $ Right $ ProjectPath path
    else
      let newPath = takeDirectory path
       in if path == newPath
            then throwIO (ConfigFileInvalid "multi-package" (Y.InvalidYaml (Just (Y.YamlException $ "Yaml file not found: " <> multiPackageConfigName))))
            else pure $ Left newPath

canonicalizeMultiPackageConfigIntermediate :: ProjectPath -> MultiPackageConfigFieldsIntermediate -> IO MultiPackageConfigFieldsIntermediate
canonicalizeMultiPackageConfigIntermediate projectPath (MultiPackageConfigFieldsIntermediate (MultiPackageConfigFields packagePaths) multiPackagePaths) =
  withCurrentDirectory (unwrapProjectPath projectPath) $ do
    MultiPackageConfigFieldsIntermediate
      <$> (MultiPackageConfigFields <$> traverse canonicalizePath packagePaths)
      <*> traverse canonicalizePath multiPackagePaths

-- Fix for single argument that checks for loops over that single argument in the recursion
cyclelessIOFix :: forall a b. Eq a => ([a] -> String) -> ((a -> IO b) -> a -> IO b) -> a -> IO b
cyclelessIOFix loopShow f = loop []
  where
    loop :: [a] -> a -> IO b
    loop seen cur = do
      case cur `elemIndex` seen of
        Nothing -> f (loop (cur : seen)) cur
        Just i -> error $ "Cycle detected: " <> loopShow (reverse $ cur : take (i + 1) seen)

fullParseMultiPackageConfig :: ProjectPath -> IO MultiPackageConfigFields
fullParseMultiPackageConfig = cyclelessIOFix loopShow $ \loop projectPath -> do
    multiPackage <- readMultiPackageConfig projectPath
    multiPackageConfigI <- either throwIO pure (parseMultiPackageConfig multiPackage)
    canonMultiPackageConfigI <- canonicalizeMultiPackageConfigIntermediate projectPath multiPackageConfigI
    otherMultiPackageConfigs <- traverse loop (ProjectPath <$> mpiOtherConfigFiles canonMultiPackageConfigI)

    pure $ MultiPackageConfigFields $ nub $ concatMap mpPackagePaths $ mpiConfigFields canonMultiPackageConfigI : otherMultiPackageConfigs
  where
    loopShow :: [ProjectPath] -> String
    loopShow = ("\n" <>) . unlines . fmap ((" - " <>) . unwrapProjectPath)

withMultiPackageConfig :: ProjectPath -> (MultiPackageConfigFields -> IO a) -> IO a
withMultiPackageConfig projectPath f = do
    canonProjectFilePath <- canonicalizePath $ unwrapProjectPath projectPath
    multiPackageProjectPath <- findMultiPackageConfigProjectPath canonProjectFilePath
    multiPackageConfig <- fullParseMultiPackageConfig multiPackageProjectPath
    f multiPackageConfig

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
