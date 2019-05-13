-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Version
    ( getInstalledSdkVersions
    , getLatestSdkVersionCached
    , getSdkVersionFromSdkPath
    , getSdkVersionFromProjectPath
    , getAssistantSdkVersion
    , getDefaultSdkVersion
    ) where

import DAML.Assistant.Types
import DAML.Assistant.Util
import DAML.Assistant.Cache
import DAML.Assistant.Install
import DAML.Project.Config
import DAML.Project.Consts hiding (getDamlPath, getProjectPath)
import System.Directory
import System.FilePath
import System.Environment.Blank
import Control.Monad.Extra
import Data.Maybe
import Data.Either.Extra
import Safe

-- | Get the latest released SDK version. Designed to return Nothing if
-- anything fails (e.g. machine is offline). The result is cached in
-- $DAML_HOME/cache/latest-sdk-version.txt and only polled once a day.
getLatestSdkVersionCached :: DamlPath -> IO (Maybe SdkVersion)
getLatestSdkVersionCached damlPath =
    cacheLatestSdkVersion damlPath $ do
        tryAssistantM getLatestVersion

-- | Determine SDK version of running daml assistant. Fails with an
-- AssistantError exception if the version cannot be determined.
getAssistantSdkVersion :: IO SdkVersion
getAssistantSdkVersion = do
    exePath <- requiredIO "Failed to determine executable path of assistant."
        getExecutablePath
    sdkPath <- required "Failed to determine SDK path of assistant." =<<
        findM hasSdkConfig (ascendants exePath)
    getSdkVersionFromSdkPath (SdkPath sdkPath)
    where
        hasSdkConfig :: FilePath -> IO Bool
        hasSdkConfig p = doesFileExist (p </> sdkConfigName)

-- | Determine SDK version from an SDK directory. Fails with an
-- AssistantError exception if the version cannot be determined.
getSdkVersionFromSdkPath :: SdkPath -> IO SdkVersion
getSdkVersionFromSdkPath sdkPath = do
    config <- requiredIO "Failed to read SDK config." $
        readSdkConfig sdkPath
    requiredE "Failed to parse SDK version from SDK config." $
        sdkVersionFromSdkConfig config

-- | Determine SDK version from project root. Fails with an
-- AssistantError exception if the version cannot be determined.
getSdkVersionFromProjectPath :: ProjectPath -> IO SdkVersion
getSdkVersionFromProjectPath projectPath = do
    config <- requiredIO "Failed to read project config." $
        readProjectConfig projectPath
    versionM <- requiredE "Failed to parse SDK version from project config." $
        sdkVersionFromProjectConfig config
    required "SDK version missing from project config." versionM

-- | Get the list of installed SDK versions. Returned list is
-- in no particular order. Fails with an AssistantError exception
-- if this list cannot be obtained.
getInstalledSdkVersions :: DamlPath -> IO [SdkVersion]
getInstalledSdkVersions (DamlPath path) = do
    let sdkdir = path </> "sdk"
    subdirs <- requiredIO "Failed to list installed SDKs." $ do
        dirlist <- listDirectory sdkdir
        filterM (\p -> doesDirectoryExist (sdkdir </> p)) dirlist
    pure (mapMaybe (eitherToMaybe . parseVersion . pack) subdirs)


-- | Get the default SDK version for commands run outside of a
-- project. This is defined as the latest installed version
-- without a release tag (e.g. this will prefer version 0.12.17
-- over version 0.12.18-nightly even though the latter came later).
--
-- Raises an AssistantError exception if the version cannot be
-- obtained, either because we cannot determine the installed
-- versions or it is empty.
getDefaultSdkVersion :: DamlPath -> IO SdkVersion
getDefaultSdkVersion damlPath = do
    installedVersions <- getInstalledSdkVersions damlPath
    required "There are no installed SDK versions." $
        maximumMay (filter isStableVersion installedVersions)



