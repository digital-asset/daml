-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Version
    ( getInstalledSdkVersions
    , getSdkVersionFromSdkPath
    , getSdkVersionFromProjectPath
    , getAssistantSdkVersion
    , getDefaultSdkVersion
    , getAvailableSdkVersions
    , getLatestSdkVersion
    , getAvailableSdkSnapshotVersions
    , getAvailableSdkSnapshotVersionsUncached
    , getLatestSdkSnapshotVersion
    , getLatestReleaseVersion
    , isSnapshotOfInterest
    , extractVersionsFromSnapshots
    , UseCache (..)
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
import Data.Either.Extra
import Data.Aeson (FromJSON(..), eitherDecodeStrict')
import Data.Aeson.Types (listParser, withObject, (.:), Value, Parser)
import qualified Data.Text as T
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

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.UTF8 as BSU
import qualified Data.SemVer as V
import Data.Function ((&))
import Control.Lens (view)

import qualified Data.Map.Strict as M
import qualified Data.List.NonEmpty as NonEmpty

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

isSnapshotOfInterest :: SdkVersion -> Bool
isSnapshotOfInterest sdkVersion =
    let v = unwrapSdkVersion sdkVersion
    in
    null (view V.release v) && null (view V.metadata v) && view V.major v > 0

-- | Get the list of available snapshot versions. This will fetch all snapshot
-- versions and then prune them into releases
getAvailableSdkVersions :: UseCache -> IO ([SdkVersion], CacheAge)
getAvailableSdkVersions useCache = do
    (versions, cacheAge) <- wrapErr "Fetching list of available SDK versions" $ getAvailableSdkSnapshotVersions useCache
    pure (extractVersionsFromSnapshots versions, cacheAge)

extractVersionsFromSnapshots :: [SdkVersion] -> [SdkVersion]
extractVersionsFromSnapshots snapshots =
    let -- For grouping things by their major or minor version
        distinguishBy :: Ord k => (a -> k) -> [a] -> M.Map k (NonEmpty.NonEmpty a)
        distinguishBy f as = M.fromListWith (<>) [(f a, pure a) | a <- as]

        -- Group versions by their major version number, filtering out snapshots
        majorMap :: M.Map Int (NonEmpty.NonEmpty SdkVersion)
        majorMap = distinguishBy (view V.major . unwrapSdkVersion) (filter isSnapshotOfInterest snapshots)
    in
    case M.maxView majorMap of
      Just (latestMajorVersions, withoutLatestMajor) ->
        let -- For old majors, only the latest stable patch
            oldMajors :: [SdkVersion]
            oldMajors = map maximum (M.elems withoutLatestMajor)

            latestMajorMinorVersions :: M.Map Int (NonEmpty.NonEmpty SdkVersion)
            latestMajorMinorVersions =
                distinguishBy (view V.minor . unwrapSdkVersion) (NonEmpty.toList latestMajorVersions)

            -- For the most recent major version, output the latest minor version
            latestMajorLatestMinorVersions :: [SdkVersion]
            latestMajorLatestMinorVersions = map maximum (M.elems latestMajorMinorVersions)
        in
        oldMajors ++ latestMajorLatestMinorVersions
      -- If the map is empty, there are no versions to return and we return an empty list.
      Nothing -> []

-- | Get the list of available snapshot versions, deferring to cache if
-- possible
getAvailableSdkSnapshotVersions :: UseCache -> IO ([SdkVersion], CacheAge)
getAvailableSdkSnapshotVersions useCache =
  cacheAvailableSdkVersions useCache getAvailableSdkSnapshotVersionsUncached

-- | Get the list of available snapshot versions. This will fetch
-- https://api.github.com/repos/digital-asset/daml/releases and parse the
-- obtained list of versions.
getAvailableSdkSnapshotVersionsUncached :: IO [SdkVersion]
getAvailableSdkSnapshotVersionsUncached = do
  releasesPages <- requestReleasesPaged "https://api.github.com/repos/digital-asset/daml/releases"
  versionss <- forM releasesPages $ \(url, body) ->
    fromRightM
      (throwIO . assistantErrorBecause ("Snapshot versions list from " <> pack url <> " does not contain valid JSON") . pack)
      (extractVersions body)
  let versions = concat versionss
  pure versions
  where
  requestReleasesPaged :: String -> IO [(String, ByteString)]
  requestReleasesPaged url = do
    (versions, mNext) <- requestReleasesSinglePage url
    rest <- case mNext of
              Nothing -> pure []
              Just url -> requestReleasesPaged url
    pure ((url, versions) : rest)

  requestReleasesSinglePage :: String -> IO (ByteString, Maybe String)
  requestReleasesSinglePage url =
    requiredAny "HTTP connection to github.com failed" $ do
        urlRequest <- parseRequest url
        let request =
                urlRequest
                    & addRequestHeader "User-Agent" "Daml-Assistant/0.0"
                    & addRequestHeader "Accept" "application/vnd.github+json"
                    & addToRequestQueryString [("per_page", Just "100")]
        res <- httpBS request { responseTimeout = responseTimeoutMicro 10000000 }
        pure (getResponseBody res, nextPage res)

  nextPage :: Response a -> Maybe String
  nextPage res = go (concatMap BSC.words (getResponseHeader "Link" res))
    where
      go ws =
        case ws of
          (link:rel:rest)
            | rel == "rel=\"next\"," -> Just (takeWhile (/= '>') (tail (BSU.toString link)))
            | otherwise -> go rest
          _ -> Nothing

  extractVersions :: ByteString -> Either String [SdkVersion]
  extractVersions bs = unParsedSdkVersions <$> eitherDecodeStrict' bs

newtype ParsedSdkVersions = ParsedSdkVersions { unParsedSdkVersions :: [SdkVersion] }
  deriving (Show, Eq, Ord)

instance FromJSON ParsedSdkVersions where
  parseJSON v = ParsedSdkVersions <$> listParser parseSingleVersion v
    where
      parseSingleVersion :: Value -> Parser SdkVersion
      parseSingleVersion =
        withObject "Version" $ \v -> do
          rawTagName <- (v .: "tag_name" :: Parser T.Text)
          case parseVersion (T.dropWhile ('v' ==) rawTagName) of
            Left (InvalidVersion src msg) -> fail $ "Invalid version string `" <> unpack src <> "` for reason: " <> msg
            Right sdkVersion -> pure sdkVersion

-- | Get the latest released SDK version
getLatestSdkVersion :: UseCache -> IO (Maybe SdkVersion)
getLatestSdkVersion useCache = do
    versionsE <- tryAssistant $ getAvailableSdkVersions useCache
    pure $ do
        (versions, age) <- eitherToMaybe versionsE
        case age of
            Stale -> Nothing
            Fresh -> maximumMay versions

-- | Get the latest snapshot SDK version.
getLatestSdkSnapshotVersion :: IO (Maybe SdkVersion)
getLatestSdkSnapshotVersion = do
    versionsE <- tryAssistant (getAvailableSdkSnapshotVersions DontUseCache)
    pure $ do
        versions <- eitherToMaybe versionsE
        maximumMay (fst versions)

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
