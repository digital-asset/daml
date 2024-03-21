-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Project.Config
    ( DamlConfig
    , ProjectConfig
    , SdkConfig
    , readSdkConfig
    , readProjectConfig
    , readDamlConfig
    , readMultiPackageConfig
    , releaseVersionFromProjectConfig
    , sdkVersionFromSdkConfig
    , listSdkCommands
    , queryDamlConfig
    , queryProjectConfig
    , querySdkConfig
    , queryMultiPackageConfig
    , queryDamlConfigRequired
    , queryProjectConfigRequired
    , querySdkConfigRequired
    , queryMultiPackageConfigRequired
    ) where

import DA.Daml.Project.Consts
import DA.Daml.Project.Types
import DA.Daml.Project.Util
import qualified Data.Aeson.Key as A
import qualified Data.Text as T
import Data.Text (Text)
import qualified Data.Yaml as Y
import Data.Yaml ((.:?))
import Data.Either.Extra
import Data.Foldable
import System.FilePath
import Control.Exception.Safe

-- | Read daml config file.
-- Throws a ConfigError if reading or parsing fails.
readDamlConfig :: DamlPath -> IO DamlConfig
readDamlConfig (DamlPath path) = readConfig "daml" (path </> damlConfigName)

-- | Read project config file.
-- Throws a ConfigError if reading or parsing fails.
readProjectConfig :: ProjectPath -> IO ProjectConfig
readProjectConfig (ProjectPath path) = readConfig "project" (path </> projectConfigName)

-- | Read sdk config file.
-- Throws a ConfigError if reading or parsing fails.
readSdkConfig :: SdkPath -> IO SdkConfig
readSdkConfig (SdkPath path) = readConfig "SDK" (path </> sdkConfigName)

-- | Read multi package config file.
-- Throws a ConfigError if reading or parsing fails.
readMultiPackageConfig :: ProjectPath -> IO MultiPackageConfig
readMultiPackageConfig (ProjectPath path) = readConfig "multi-package" (path </> multiPackageConfigName)

-- | (internal) Helper function for defining 'readXConfig' functions.
-- Throws a ConfigError if reading or parsing fails.
readConfig :: Y.FromJSON b => Text -> FilePath -> IO b
readConfig name path = do
    configE <- Y.decodeFileEither path
    fromRightM (throwIO . ConfigFileInvalid name) configE

-- | Determine pinned sdk version from project config, if it exists.
releaseVersionFromProjectConfig :: ProjectConfig -> Either ConfigError (Maybe UnresolvedReleaseVersion)
releaseVersionFromProjectConfig = queryProjectConfig ["sdk-version"]

-- | Determine sdk version from sdk config, if it exists.
sdkVersionFromSdkConfig :: SdkConfig -> Either ConfigError SdkVersion
sdkVersionFromSdkConfig = querySdkConfigRequired ["version"]

-- | Read sdk config to get list of sdk commands.
listSdkCommands :: SdkPath -> EnrichedCompletion -> SdkConfig -> Either ConfigError [SdkCommandInfo]
listSdkCommands sdkPath enriched sdkConf = map (\f -> f sdkPath enriched) <$> querySdkConfigRequired ["commands"] sdkConf

-- | Query the daml config by passing a path to the desired property.
-- See 'queryConfig' for more details.
queryDamlConfig :: Y.FromJSON t => [Text] -> DamlConfig -> Either ConfigError (Maybe t)
queryDamlConfig path = queryConfig "daml" "DamlConfig" path . unwrapDamlConfig

-- | Query the project config by passing a path to the desired property.
-- See 'queryConfig' for more details.
queryProjectConfig :: Y.FromJSON t => [Text] -> ProjectConfig -> Either ConfigError (Maybe t)
queryProjectConfig path = queryConfig "project" "ProjectConfig" path . unwrapProjectConfig

-- | Query the sdk config by passing a list of members to the desired property.
-- See 'queryConfig' for more details.
querySdkConfig :: Y.FromJSON t => [Text] -> SdkConfig -> Either ConfigError (Maybe t)
querySdkConfig path = queryConfig "SDK" "SdkConfig" path . unwrapSdkConfig

-- | Query the multi-package config by passing a list of members to the desired property.
-- See 'queryConfig' for more details.
queryMultiPackageConfig :: Y.FromJSON t => [Text] -> MultiPackageConfig -> Either ConfigError (Maybe t)
queryMultiPackageConfig path = queryConfig "multi-package" "MultiPackageConfig" path . unwrapMultiPackageConfig

-- | Like 'queryDamlConfig' but returns an error if the property is missing.
queryDamlConfigRequired :: Y.FromJSON t => [Text] -> DamlConfig -> Either ConfigError t
queryDamlConfigRequired path = queryConfigRequired "daml" "DamlConfig" path . unwrapDamlConfig

-- | Like 'queryProjectConfig' but returns an error if the property is missing.
queryProjectConfigRequired :: Y.FromJSON t => [Text] -> ProjectConfig -> Either ConfigError t
queryProjectConfigRequired path = queryConfigRequired "project" "ProjectConfig" path . unwrapProjectConfig

-- | Like 'querySdkConfig' but returns an error if the property is missing.
querySdkConfigRequired :: Y.FromJSON t => [Text] -> SdkConfig -> Either ConfigError t
querySdkConfigRequired path = queryConfigRequired "SDK" "SdkConfig" path . unwrapSdkConfig

-- | Like 'queryMultiPackageConfig' but returns an error if the property is missing.
queryMultiPackageConfigRequired :: Y.FromJSON t => [Text] -> MultiPackageConfig -> Either ConfigError t
queryMultiPackageConfigRequired path = queryConfigRequired "multi-package" "MultiPackageConfig" path . unwrapMultiPackageConfig

-- | (internal) Helper function for querying config data. The 'path' argument
-- represents the location of the desired property within the config file.
-- For example, if you had a YAML file like so:
--
--     a:
--        b:
--           c:
--              <desired property>
--
-- Then you would pass ["a", "b", "c"] as path to get the desired property.
--
-- This distinguishes between a missing property and a poorly formed property:
--    * If the property is missing, this returns (Right Nothing).
--    * If the property is poorly formed, this returns (Left ...).
queryConfig :: Y.FromJSON t => Text -> Text -> [Text] -> Y.Value -> Either ConfigError (Maybe t)
queryConfig name root path cfg
    = mapLeft (ConfigFieldInvalid name path)
    . flip Y.parseEither cfg
    $ \v0 -> do
        let initial = (root, Just v0)
            step (p, Nothing) _ = pure (p, Nothing)
            step (p, Just v) n = do
                v' <- Y.withObject (T.unpack p) (.:? A.fromText n) v
                pure (p <> "." <> n, v')

        (_,v1) <- foldlM step initial path
        mapM Y.parseJSON v1

-- | (internal) Like 'queryConfig' but returns an error if property is missing.
queryConfigRequired :: Y.FromJSON t => Text -> Text -> [Text] -> Y.Value -> Either ConfigError t
queryConfigRequired name root path cfg = do
    resultM <- queryConfig name root path cfg
    fromMaybeM (Left $ ConfigFieldMissing name path) resultM
