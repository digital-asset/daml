-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Config
    ( DamlConfig
    , ProjectConfig
    , SdkConfig
    , readSdkConfig
    , readProjectConfig
    , readDamlConfig
    , sdkVersionFromProjectConfig
    , sdkVersionFromSdkConfig
    , listSdkCommands
    , queryDamlConfig
    , queryProjectConfig
    , querySdkConfig
    , queryDamlConfigRequired
    , queryProjectConfigRequired
    , querySdkConfigRequired
    ) where

import DAML.Assistant.Types
import DAML.Assistant.Consts
import DAML.Assistant.Util
import qualified Data.Text as T
import qualified Data.Yaml as Y
import Data.Yaml ((.:?))
import Data.Either.Extra
import Data.Foldable
import System.FilePath

-- | Read daml config file.
-- Throws an assistant error if reading or parsing fails.
readDamlConfig :: DamlPath -> IO DamlConfig
readDamlConfig (DamlPath path) = readConfig "daml" (path </> damlConfigName)

-- | Read project config file.
-- Throws an assistant error if reading or parsing fails.
readProjectConfig :: ProjectPath -> IO ProjectConfig
readProjectConfig (ProjectPath path) = readConfig "project" (path </> projectConfigName)

-- | Read sdk config file.
-- Throws an assistant error if reading or parsing fails.
readSdkConfig :: SdkPath -> IO SdkConfig
readSdkConfig (SdkPath path) = readConfig "SDK" (path </> sdkConfigName)

-- | (internal) Helper function for defining 'readXConfig' functions.
readConfig :: Y.FromJSON b => Text -> FilePath -> IO b
readConfig name path = do
    configE <- Y.decodeFileEither path
    requiredE ("Failed to read " <> name <> " config file " <> pack path) configE

-- | Determine pinned sdk version from project config, if it exists.
sdkVersionFromProjectConfig :: ProjectConfig -> Either AssistantError (Maybe SdkVersion)
sdkVersionFromProjectConfig = queryProjectConfig ["project", "sdk-version"]

-- | Determine sdk version from project config, if it exists.
sdkVersionFromSdkConfig :: SdkConfig -> Either AssistantError SdkVersion
sdkVersionFromSdkConfig = querySdkConfigRequired ["version"]

-- | Read sdk config to get list of sdk commands.
listSdkCommands :: SdkConfig -> Either AssistantError [SdkCommandInfo]
listSdkCommands = querySdkConfigRequired ["commands"]

-- | Query the daml config by passing a path to the desired property.
-- See 'queryConfig' for more details.
queryDamlConfig :: Y.FromJSON t => [Text] -> DamlConfig -> Either AssistantError (Maybe t)
queryDamlConfig path = queryConfig "daml" "DamlConfig" path . unwrapDamlConfig

-- | Query the project config by passing a path to the desired property.
-- See 'queryConfig' for more details.
queryProjectConfig :: Y.FromJSON t => [Text] -> ProjectConfig -> Either AssistantError (Maybe t)
queryProjectConfig path = queryConfig "project" "ProjectConfig" path . unwrapProjectConfig

-- | Query the sdk config by passing a list of members to the desired property.
-- See 'queryConfig' for more details.
querySdkConfig :: Y.FromJSON t => [Text] -> SdkConfig -> Either AssistantError (Maybe t)
querySdkConfig path = queryConfig "SDK" "SdkConfig" path . unwrapSdkConfig

-- | Like 'queryDamlConfig' but returns an error if the property is missing.
queryDamlConfigRequired :: Y.FromJSON t => [Text] -> DamlConfig -> Either AssistantError t
queryDamlConfigRequired path = queryConfigRequired "daml" "DamlConfig" path . unwrapDamlConfig

-- | Like 'queryProjectConfig' but returns an error if the property is missing.
queryProjectConfigRequired :: Y.FromJSON t => [Text] -> ProjectConfig -> Either AssistantError t
queryProjectConfigRequired path = queryConfigRequired "project" "ProjectConfig" path . unwrapProjectConfig

-- | Like 'querySdkConfig' but returns an error if the property is missing.
querySdkConfigRequired :: Y.FromJSON t => [Text] -> SdkConfig -> Either AssistantError t
querySdkConfigRequired path = queryConfigRequired "SDK" "SdkConfig" path . unwrapSdkConfig

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
queryConfig :: Y.FromJSON t => Text -> Text -> [Text] -> Y.Value -> Either AssistantError (Maybe t)
queryConfig name root path cfg
    = mapLeft (assistantErrorBecause ("Invalid " <> name <> " config file.") . pack)
    . flip Y.parseEither cfg
    $ \v0 -> do
        let initial = (root, Just v0)
            step (p, Nothing) _ = pure (p, Nothing)
            step (p, Just v) n = do
                v' <- Y.withObject (unpack p) (.:? n) v
                pure (p <> "." <> n, v')

        (_,v1) <- foldlM step initial path
        mapM Y.parseJSON v1

-- | (internal) Like 'queryConfig' but returns an error if property is missing.
queryConfigRequired :: Y.FromJSON t => Text -> Text -> [Text] -> Y.Value -> Either AssistantError t
queryConfigRequired name root path cfg = do
    resultM <- queryConfig name root path cfg
    fromMaybeM (Left $ assistantErrorBecause ("Invalid " <> name <> " config file.")
        ("Missing required property: " <> T.intercalate "." (root:path))) resultM
