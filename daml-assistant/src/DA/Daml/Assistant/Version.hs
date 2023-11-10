-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Version
    ( getInstalledSdkVersions
    , getSdkVersionFromSdkPath
    , getSdkVersionFromProjectPath
    , getAssistantSdkVersion
    , getDefaultSdkVersion
    , getAvailableReleaseVersions
    , getAvailableSdkSnapshotVersions
    , getAvailableSdkSnapshotVersionsUncached
    , findAvailableSdkSnapshotVersion
    , getLatestSdkSnapshotVersion
    , getLatestReleaseVersion
    , isReleaseVersion
    , extractReleasesFromSnapshots
    , UseCache (..)
    , freshMaximumOfVersions
    ) where

import Control.Exception.Safe
import Control.Lens (view)
import Control.Monad.Extra
import DA.Daml.Assistant.Cache
import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util
import DA.Daml.Project.Config
import DA.Daml.Project.Consts hiding (getDamlPath, getProjectPath)
import Data.Aeson (FromJSON(..), eitherDecodeStrict')
import Data.Aeson.Types (listParser, withObject, (.:), Parser)
import Data.ByteString (ByteString)
import Data.ByteString.Char8 qualified as BSC
import Data.ByteString.UTF8 qualified as BSU
import Data.Either.Extra
import Data.Function ((&))
import Data.List.NonEmpty qualified as NonEmpty
import Data.Map.Strict qualified as M
import Data.Maybe
import Data.SemVer qualified as V
import Data.Text qualified as T
import Network.HTTP.Client
    ( Request(responseTimeout)
    , responseTimeoutMicro
    )
import Network.HTTP.Simple
import Safe
import System.Directory
import System.Environment.Blank
import System.FilePath

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

isReleaseVersion :: SdkVersion -> Bool
isReleaseVersion sdkVersion =
    let v = unwrapSdkVersion sdkVersion
    in
    null (view V.release v) && null (view V.metadata v) && view V.major v > 0

-- | Get the list of available release versions. This will fetch all snapshot
-- versions and then prune them into releases
getAvailableReleaseVersions :: UseCache -> IO ([SdkVersion], CacheAge)
getAvailableReleaseVersions useCache = do
    (versions, cacheAge) <- wrapErr "Fetching list of available SDK versions" $ getAvailableSdkSnapshotVersions useCache
    pure (extractReleasesFromSnapshots versions, cacheAge)

extractReleasesFromSnapshots :: [SdkVersion] -> [SdkVersion]
extractReleasesFromSnapshots snapshots =
    let -- For grouping things by their major or minor version
        distinguishBy :: Ord k => (a -> k) -> [a] -> M.Map k (NonEmpty.NonEmpty a)
        distinguishBy f as = M.fromListWith (<>) [(f a, pure a) | a <- as]

        -- Group versions by their major version number, filtering out snapshots
        majorMap :: M.Map Int (NonEmpty.NonEmpty SdkVersion)
        majorMap = distinguishBy (view V.major . unwrapSdkVersion) (filter isReleaseVersion snapshots)
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
  cacheAvailableSdkVersions useCache (getAvailableSdkSnapshotVersionsUncached >>= flattenSnapshotsList)

-- | Find the first occurence of a version on Github, without the cache. Keep in
  -- mind that versions are not sorted.
findAvailableSdkSnapshotVersion :: (SdkVersion -> Bool) -> IO (Maybe SdkVersion)
findAvailableSdkSnapshotVersion pred =
  getAvailableSdkSnapshotVersionsUncached >>= searchSnapshotsUntil pred

data SnapshotsList = SnapshotsList
  { versions :: IO [SdkVersion]
  , next :: Maybe (IO SnapshotsList)
  }

flattenSnapshotsList :: SnapshotsList -> IO [SdkVersion]
flattenSnapshotsList SnapshotsList { versions, next } = do
  versions <- versions
  rest <- case next of
            Nothing -> pure []
            Just io -> io >>= flattenSnapshotsList
  return (versions ++ rest)

searchSnapshotsUntil :: (SdkVersion -> Bool) -> SnapshotsList -> IO (Maybe SdkVersion)
searchSnapshotsUntil pred SnapshotsList { versions, next } = do
  versions <- versions
  case filter pred versions of
    (v:_) -> pure (Just v)
    _ -> case next of
      Nothing -> pure Nothing
      Just io -> io >>= searchSnapshotsUntil pred

-- | Get the list of available snapshot versions, until finding a version of
-- interest. This will fetch https://api.github.com/repos/digital-asset/daml/releases
-- and parse the obtained JSON.
-- We do *not* use
-- https://docs.github.com/en/rest/releases/releases?apiVersion=2022-11-28#get-the-latest-release
-- because it sorts by time of upload, so a minor version bump like 2.5.15 may
-- supersede 2.7.2 if the minor release on 2.5.12 was released later
getAvailableSdkSnapshotVersionsUncached :: IO SnapshotsList
getAvailableSdkSnapshotVersionsUncached = do
  requestReleasesSnapshotsList "https://api.github.com/repos/digital-asset/daml/releases"
  where
  requestReleasesSnapshotsList :: String -> IO SnapshotsList
  requestReleasesSnapshotsList url = do
    (raw, mNext) <- requestReleasesSinglePage url
    pure SnapshotsList
      { versions =
        fromRightM
          (throwIO . assistantErrorBecause ("Snapshot versions list from " <> pack url <> " does not contain valid JSON") . pack)
          (extractVersions raw)
      , next = fmap requestReleasesSnapshotsList mNext
      }

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
  extractVersions bs = map unParsedSdkVersion . unParsedSdkVersions <$> eitherDecodeStrict' bs

newtype ParsedSdkVersions = ParsedSdkVersions { unParsedSdkVersions :: [ParsedSdkVersion] }
data ParsedSdkVersion = ParsedSdkVersion { unParsedSdkVersion :: SdkVersion, isPrerelease :: Bool }
  deriving (Show, Eq, Ord)

instance FromJSON ParsedSdkVersions where
  parseJSON v = ParsedSdkVersions <$> listParser parseJSON v

instance FromJSON ParsedSdkVersion where
  parseJSON =
    withObject "Version" $ \v -> do
      rawTagName <- (v .: "tag_name" :: Parser T.Text)
      isPrerelease <- (v .: "prerelease" :: Parser Bool)
      case parseVersion (T.dropWhile ('v' ==) rawTagName) of
        Left (InvalidVersion src msg) -> fail $ "Invalid version string `" <> unpack src <> "` for reason: " <> msg
        Right sdkVersion -> pure ParsedSdkVersion { unParsedSdkVersion = sdkVersion, isPrerelease }

maximumOfNonEmptyVersions :: IO ([SdkVersion], CacheAge) -> IO SdkVersion
maximumOfNonEmptyVersions getVersions = do
    (versions, _cacheAge) <- getVersions
    case maximumMay versions of
      Nothing -> throwIO $ assistantError $ pack "Version list is empty."
      Just m -> pure m

-- | Get the latest released SDK version
freshMaximumOfVersions :: IO ([SdkVersion], CacheAge) -> IO (Maybe SdkVersion)
freshMaximumOfVersions getVersions = do
    (versions, cacheAge) <- getVersions
    case cacheAge of
      Stale -> pure Nothing
      Fresh -> pure (maximumMay versions)

-- | Get the latest snapshot SDK version.
getLatestSdkSnapshotVersion :: UseCache -> IO SdkVersion
getLatestSdkSnapshotVersion useCache = do
    maximumOfNonEmptyVersions (getAvailableSdkSnapshotVersions useCache)

getLatestReleaseVersion :: UseCache -> IO SdkVersion
getLatestReleaseVersion useCache =
    maximumOfNonEmptyVersions (getAvailableReleaseVersions useCache)
