-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Version
    ( getInstalledSdkVersions
    , getSdkVersionFromSdkPath
    , getSdkVersionFromProjectPath
    , getAssistantSdkVersion
    , getDefaultSdkVersion
    , getAvailableSdkVersions
    , getAvailableSdkVersionsCached
    , refreshAvailableSdkVersions
    , getLatestSdkVersionCached
    , getAvailableSdkSnapshotVersions
    , getLatestSdkSnapshotVersion
    , getLatestReleaseVersion
    ) where

import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util
import DA.Daml.Assistant.Cache
import DA.Daml.Project.Config
import DA.Daml.Project.Consts hiding (getDamlPath, getProjectPath)
import System.Directory
import System.FilePath
import System.Environment.Blank
import Control.Exception.Safe
import Control.Monad.Extra
import Data.Maybe
import Data.List
import Data.Either.Extra
import Data.Aeson (eitherDecodeStrict')
import Data.Text.Encoding
import Safe
import Network.HTTP.Simple
import Network.HTTP.Client
    ( Request(responseTimeout)
    , responseBody
    , responseStatus
    , responseTimeoutMicro
    )
import qualified Network.HTTP.Client as Http
import Network.HTTP.Types (ok200)
import Network.HTTP.Client.TLS (newTlsManager)

import qualified Data.HashMap.Strict as M

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
    config <- requiredAny "Failed to read SDK config." $
        readSdkConfig sdkPath
    requiredE "Failed to parse SDK version from SDK config." $
        sdkVersionFromSdkConfig config

-- | Determine SDK version from project root. Fails with an
-- AssistantError exception if the version cannot be determined.
getSdkVersionFromProjectPath :: ProjectPath -> IO SdkVersion
getSdkVersionFromProjectPath projectPath =
    requiredIO ("Failed to read SDK version from " <> pack projectConfigName) $ do
        configE <- tryConfig $ readProjectConfig projectPath
        case sdkVersionFromProjectConfig =<< configE of
            Right (Just v) ->
                pure v
            Left (ConfigFileInvalid _ raw) ->
                throwIO $ assistantErrorDetails
                    (projectConfigName <> " is an invalid YAML file")
                    [("path", unwrapProjectPath projectPath </> projectConfigName)
                    ,("internal", displayException raw)]
            Right Nothing ->
                throwIO $ assistantErrorDetails
                    ("sdk-version field is missing from " <> projectConfigName)
                    [("path", unwrapProjectPath projectPath </> projectConfigName)]
            Left (ConfigFieldMissing _ _) ->
                throwIO $ assistantErrorDetails
                    ("sdk-version field is missing from " <> projectConfigName)
                    [("path", unwrapProjectPath projectPath </> projectConfigName)]
            Left (ConfigFieldInvalid _ _ raw) ->
                throwIO $ assistantErrorDetails
                    ("sdk-version field is invalid in " <> projectConfigName)
                    [("path", unwrapProjectPath projectPath </> projectConfigName)
                    ,("internal", raw)]

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
        maximumMay installedVersions

-- | Get the list of available versions afresh. This will fetch
-- https://docs.daml.com/versions.json and parse the obtained list
-- of versions.
getAvailableSdkVersions :: IO [SdkVersion]
getAvailableSdkVersions = wrapErr "Fetching list of available SDK versions" $ do
    response <- requiredAny "HTTP connection to docs.daml.com failed" $ do
        request <- parseRequest "GET http://docs.daml.com/versions.json"
        httpBS request { responseTimeout = responseTimeoutMicro 2000000 }

    when (getResponseStatusCode response /= 200) $ do
        throwIO $ assistantErrorBecause
            "Fetching list of available SDK versions from docs.daml.com failed"
            (pack . show $ getResponseStatus response)

    versionsMap :: M.HashMap Text Text <-
        fromRightM
            (throwIO . assistantErrorBecause "Versions list from docs.daml.com does not contain valid JSON" . pack)
            (eitherDecodeStrict' (getResponseBody response))

    pure . sort $ mapMaybe (eitherToMaybe . parseVersion) (M.keys versionsMap)

-- | Get the list of available snapshot versions. This will fetch
-- https://docs.daml.com/snapshots.json and parse the obtained list
-- of versions.
getAvailableSdkSnapshotVersions :: IO [SdkVersion]
getAvailableSdkSnapshotVersions = wrapErr "Fetching list of available SDK snapshot versions" $ do
    response <- requiredAny "HTTP connection to docs.daml.com failed" $ do
        request <- parseRequest "GET http://docs.daml.com/snapshots.json"
        httpBS request { responseTimeout = responseTimeoutMicro 2000000 }

    when (getResponseStatusCode response /= 200) $ do
        throwIO $ assistantErrorBecause
            "Fetching list of available SDK snapshot versions from docs.daml.com failed"
            (pack . show $ getResponseStatus response)

    versionsMap :: M.HashMap Text Text <-
        fromRightM
            (throwIO . assistantErrorBecause "Snapshot versions list from docs.daml.com does not contain valid JSON" . pack)
            (eitherDecodeStrict' (getResponseBody response))

    pure . sort $ mapMaybe (eitherToMaybe . parseVersion) (M.keys versionsMap)

-- | Same as getAvailableSdkVersions, but writes result to cache.
refreshAvailableSdkVersions :: CachePath -> IO [SdkVersion]
refreshAvailableSdkVersions cachePath = do
    versions <- getAvailableSdkVersions
    saveAvailableSdkVersions cachePath versions
    pure versions

-- | Same as getAvailableSdkVersions, but result is cached based on the duration
-- of the update-check value in daml-config.yaml (defaults to 1 day).
getAvailableSdkVersionsCached :: DamlPath -> CachePath -> IO ([SdkVersion], CacheAge)
getAvailableSdkVersionsCached damlPath cachePath =
    cacheAvailableSdkVersions damlPath cachePath getAvailableSdkVersions

-- | Get the latest released SDK version, cached as above.
getLatestSdkVersionCached :: DamlPath -> CachePath -> IO (Maybe SdkVersion)
getLatestSdkVersionCached damlPath cachePath = do
    versionsE <- tryAssistant $ getAvailableSdkVersionsCached damlPath cachePath
    pure $ do
        (versions, age) <- eitherToMaybe versionsE
        case age of
            Stale -> Nothing
            Fresh -> maximumMay versions

-- | Get the latest snapshot SDK version.
getLatestSdkSnapshotVersion :: IO (Maybe SdkVersion)
getLatestSdkSnapshotVersion = do
    versionsE <- tryAssistant getAvailableSdkSnapshotVersions
    pure $ do
        versions <- eitherToMaybe versionsE
        maximumMay versions

latestReleaseVersionUrl :: String
latestReleaseVersionUrl = "https://docs.daml.com/latest"

getLatestReleaseVersion :: IO SdkVersion
getLatestReleaseVersion = do
    manager <- newTlsManager
    request <- parseRequest latestReleaseVersionUrl
    Http.withResponse request manager $ \resp -> do
        case responseStatus resp of
            s | s == ok200 -> do
                    body <- responseBody resp
                    case parseVersion $ decodeUtf8 body of
                        Right v -> pure v
                        Left invalidVersion ->
                            throwIO $
                            assistantErrorBecause
                                (pack $
                                 "Failed to parse SDK version from " <> latestReleaseVersionUrl)
                                (pack $ ivMessage invalidVersion)
            otherStatus ->
                throwIO $
                assistantErrorBecause
                    (pack $ "Bad response status code when requesting " <> latestReleaseVersionUrl)
                    (pack $ show otherStatus)
