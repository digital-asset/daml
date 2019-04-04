-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.SdkVersion
    ( getInstalledSdkVersions
    , getDefaultSdkVersion
    , getActiveSdkVersion
    , getActiveSdkVersion'
    , requireSdkVersion
    , requireSdkVersion'
    ) where

import           Data.List            (isPrefixOf)
import           DA.Sdk.Cli.Conf
import qualified DA.Sdk.Cli.Monad.Locations as L
import           DA.Sdk.Cli.Monad
import           DA.Sdk.Prelude
import           DA.Sdk.Version       (SemVersion (..), parseSemVersion)
import qualified DA.Sdk.Cli.Monad.FileSystem as FS

getInstalledSdkVersions :: (FS.MonadFS m, L.MonadLocations m) => m (Either FS.LsFailed [SemVersion])
getInstalledSdkVersions = do
    sdkPackagePath <- L.getSdkPackagePath
    errOrFiles <- FS.ls sdkPackagePath
    let result = map (parse . pathToText . filename) .
                    filter (not . isPrefixOf "." . pathToString . filename) <$>
                        errOrFiles
    return result
  where
    parse x | Just ver <- parseSemVersion x = ver
            | otherwise = error $ "Failed to parse semantic version of SDK: " <> show x

-- | Get the default SDK version
getDefaultSdkVersion :: CliM (Maybe SemVersion)
getDefaultSdkVersion = do
    conf <- asks envConf
    return (confDefaultSDKVersion conf)

-- | Get the current effective SDK version
-- Returns the project SDK version if in a project, otherwise
-- the default SDK version.
getActiveSdkVersion :: CliM (Maybe SemVersion)
getActiveSdkVersion = do
    mbProj <- asks envProject
    case mbProj of
      Nothing   -> getDefaultSdkVersion
      Just proj -> return (Just $ projectSDKVersion proj)

getActiveSdkVersion' :: Monad m => Maybe Project -> Maybe SemVersion -> m (Maybe SemVersion)
getActiveSdkVersion' mbProj defaultSdkVsn =
    case mbProj of
      Nothing   -> return defaultSdkVsn
      Just proj -> return (Just $ projectSDKVersion proj)

requireSdkVersion :: Maybe SemVersion -> CliM SemVersion
requireSdkVersion Nothing = do
    error "No SDK version available. You need to run 'da upgrade'"
requireSdkVersion (Just ver) = return ver

requireSdkVersion' :: Monad m => Maybe SemVersion -> m SemVersion
requireSdkVersion' Nothing = do
    error "No SDK version available. You need to run 'da upgrade'"
requireSdkVersion' (Just ver) = return ver
