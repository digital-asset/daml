-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Version
    ( getInstalledSdkVersions
    , getSdkVersionFromSdkPath
    , getReleaseVersionFromSdkPath
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
    , resolveReleaseVersion
    , resolveSdkVersionToRelease
    ) where

import DA.Daml.Assistant.Types
import DA.Daml.Assistant.Util
import DA.Daml.Assistant.Cache
import DA.Daml.Project.Config
import DA.Daml.Project.Consts hiding (getDamlPath, getProjectPath)
import System.Environment.Blank
import Control.Exception.Safe
import Control.Monad.Extra
import Data.Maybe
import Data.Aeson (FromJSON(..), eitherDecodeStrict')
import Data.Aeson.Types (listParser, withObject, (.:), Parser, Value(Object), explicitParseField)
import qualified Data.Text as T
import Safe
import Network.HTTP.Simple
import Network.HTTP.Client
    ( Request(responseTimeout)
    , responseTimeoutMicro
    )

import Data.ByteString (ByteString)
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.UTF8 as BSU
import qualified Data.SemVer as V
import Data.Function ((&))
import Control.Lens (view)
import System.Directory (listDirectory, doesFileExist)
import System.FilePath ((</>))
import Data.List (find)

import qualified Data.Map.Strict as M
import qualified Data.List.NonEmpty as NonEmpty

import GHC.Stack

-- | Determine SDK version of running daml assistant. Fails with an
-- AssistantError exception if the version cannot be determined.
getAssistantSdkVersion :: UseCache -> IO ReleaseVersion
getAssistantSdkVersion useCache = do
    exePath <- requiredIO "Failed to determine executable path of assistant."
        getExecutablePath
    sdkPath <- required "Failed to determine SDK path of assistant." =<<
        findM hasSdkConfig (ascendants exePath)
    getReleaseVersionFromSdkPath useCache (SdkPath sdkPath)
    where
        hasSdkConfig :: FilePath -> IO Bool
        hasSdkConfig p = doesFileExist (p </> sdkConfigName)

-- | Determine SDK version from an SDK directory. Fails with an
-- AssistantError exception if the version cannot be determined.
getReleaseVersionFromSdkPath :: UseCache -> SdkPath -> IO ReleaseVersion
getReleaseVersionFromSdkPath useCache sdkPath = do
    sdkVersion <- getSdkVersionFromSdkPath sdkPath
    let errMsg =
            "Failed to retrieve release version for sdk version " <> V.toText (unwrapSdkVersion sdkVersion)
                <> " from sdk path " <> T.pack (unwrapSdkPath sdkPath)
    requiredE errMsg =<< resolveSdkVersionToRelease useCache sdkVersion

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
getSdkVersionFromProjectPath :: UseCache -> ProjectPath -> IO ReleaseVersion
getSdkVersionFromProjectPath useCache projectPath =
    requiredIO ("Failed to read SDK version from " <> pack projectConfigName) $ do
        configE <- tryConfig $ readProjectConfig projectPath
        case releaseVersionFromProjectConfig =<< configE of
            Right (Just v) ->
                -- TODO: resolve full release version
                resolveReleaseVersion useCache v
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
getInstalledSdkVersions :: DamlPath -> IO [ReleaseVersion]
getInstalledSdkVersions damlPath = do
    let sdkPath = SdkPath (unwrapDamlPath damlPath </> "sdk")
    sdksOrErr <- try (listDirectory (unwrapSdkPath sdkPath))
    case sdksOrErr of
      Left SomeException{} -> pure []
      Right sdks -> do
          versionsFound <- catMaybes <$> mapM resolveSdk sdks
          pure versionsFound
    where
    resolveSdk :: String -> IO (Maybe ReleaseVersion)
    resolveSdk path = do
      case parseVersion (T.pack path) of
        Left _ -> pure Nothing
        Right unresolvedVersion -> do
          let sdkPath = mkSdkPath damlPath path
          sdkVersionOrErr <- tryAssistant (getSdkVersionFromSdkPath sdkPath)
          pure $ case sdkVersionOrErr of
            Left _ -> Nothing
            Right sdkVersion -> Just (mkReleaseVersion unresolvedVersion sdkVersion)

-- | Get the default SDK version for commands run outside of a
-- project. This is defined as the latest installed version
-- without a release tag (e.g. this will prefer version 0.12.17
-- over version 0.12.18-nightly even though the latter came later).
--
-- Raises an AssistantError exception if the version cannot be
-- obtained, either because we cannot determine the installed
-- versions or it is empty.
getDefaultSdkVersion :: DamlPath -> IO ReleaseVersion
getDefaultSdkVersion damlPath = do
    installedVersions <- getInstalledSdkVersions damlPath
    required "There are no installed SDK versions." $
        maximumMay installedVersions

isReleaseVersion :: ReleaseVersion -> Bool
isReleaseVersion sdkVersion =
    let v = releaseVersionFromReleaseVersion sdkVersion
    in
    null (view V.release v) && null (view V.metadata v) && view V.major v > 0

-- | Get the list of available release versions. This will fetch all snapshot
-- versions and then prune them into releases
getAvailableReleaseVersions :: UseCache -> IO ([ReleaseVersion], CacheAge)
getAvailableReleaseVersions useCache = do
    (versions, cacheAge) <- wrapErr "Fetching list of available SDK versions" $ getAvailableSdkSnapshotVersions useCache
    pure (extractReleasesFromSnapshots versions, cacheAge)

extractReleasesFromSnapshots :: [ReleaseVersion] -> [ReleaseVersion]
extractReleasesFromSnapshots snapshots =
    let -- For grouping things by their major or minor version
        distinguishBy :: Ord k => (a -> k) -> [a] -> M.Map k (NonEmpty.NonEmpty a)
        distinguishBy f as = M.fromListWith (<>) [(f a, pure a) | a <- as]

        -- Group versions by their major version number, filtering out snapshots
        majorMap :: M.Map Int (NonEmpty.NonEmpty ReleaseVersion)
        majorMap = distinguishBy (view V.major . releaseVersionFromReleaseVersion) (filter isReleaseVersion snapshots)
    in
    case M.maxView majorMap of
      Just (latestMajorVersions, withoutLatestMajor) ->
        let -- For old majors, only the latest stable patch
            oldMajors :: [ReleaseVersion]
            oldMajors = map maximum (M.elems withoutLatestMajor)

            latestMajorMinorVersions :: M.Map Int (NonEmpty.NonEmpty ReleaseVersion)
            latestMajorMinorVersions =
                distinguishBy (view V.minor . releaseVersionFromReleaseVersion) (NonEmpty.toList latestMajorVersions)

            -- For the most recent major version, output the latest minor version
            latestMajorLatestMinorVersions :: [ReleaseVersion]
            latestMajorLatestMinorVersions = map maximum (M.elems latestMajorMinorVersions)
        in
        oldMajors ++ latestMajorLatestMinorVersions
      -- If the map is empty, there are no versions to return and we return an empty list.
      Nothing -> []

-- | Get the list of available snapshot versions, deferring to cache if
-- possible
getAvailableSdkSnapshotVersions :: UseCache -> IO ([ReleaseVersion], CacheAge)
getAvailableSdkSnapshotVersions useCache =
  cacheAvailableSdkVersions useCache (\_ -> getAvailableSdkSnapshotVersionsUncached (damlPath useCache) >>= flattenSnapshotsList)

-- | Find the first occurence of a version on Github, without the cache. Keep in
  -- mind that versions are not sorted.
findAvailableSdkSnapshotVersion :: DamlPath -> (ReleaseVersion -> Bool) -> IO (Maybe ReleaseVersion)
findAvailableSdkSnapshotVersion damlPath pred =
  getAvailableSdkSnapshotVersionsUncached damlPath >>= searchSnapshotsUntil pred

data SnapshotsList = SnapshotsList
  { versions :: IO [ReleaseVersion]
  , next :: Maybe (IO SnapshotsList)
  }

flattenSnapshotsList :: SnapshotsList -> IO [ReleaseVersion]
flattenSnapshotsList SnapshotsList { versions, next } = do
  versions <- versions
  rest <- case next of
            Nothing -> pure []
            Just io -> io >>= flattenSnapshotsList
  return (versions ++ rest)

searchSnapshotsUntil :: (ReleaseVersion -> Bool) -> SnapshotsList -> IO (Maybe ReleaseVersion)
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
getAvailableSdkSnapshotVersionsUncached :: DamlPath -> IO SnapshotsList
getAvailableSdkSnapshotVersionsUncached damlPath = do
  damlConfigE <- tryConfig (readDamlConfig damlPath)
  let releasesEndpoint =
          case queryDamlConfig ["releases-endpoint"] =<< damlConfigE of
            Right (Just url) -> url
            _ -> "https://api.github.com/repos/digital-asset/daml/releases"
  requestReleasesSnapshotsList releasesEndpoint
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

  extractVersions :: ByteString -> Either String [ReleaseVersion]
  extractVersions bs = map unParsedSdkVersion . unParsedSdkVersions <$> eitherDecodeStrict' bs

newtype ParsedSdkVersions = ParsedSdkVersions { unParsedSdkVersions :: [ParsedSdkVersion] }
data ParsedSdkVersion = ParsedSdkVersion
  { unParsedSdkVersion :: ReleaseVersion
  , isPrerelease :: Bool
  }
  deriving (Show, Eq)

instance FromJSON ParsedSdkVersions where
  parseJSON v = ParsedSdkVersions <$> listParser parseJSON v

instance FromJSON ParsedSdkVersion where
  parseJSON =
    withObject "Version" $ \v -> do
      rawTagName <- (v .: "tag_name" :: Parser T.Text)
      releaseVersion <- handleInvalidVersion "release version" (parseVersion (T.dropWhile ('v' ==) rawTagName))
      isPrerelease <- (v .: "prerelease" :: Parser Bool)
      mbRawSdkVersion <- releaseResponseSubsetSdkVersion <$> parseJSON (Object v)
      sdkVersion <- case mbRawSdkVersion of
        Nothing -> fail $ "Couldn't find Linux SDK in release version: '" <> T.unpack rawTagName <> "'"
        Just rawSdkVersion -> handleInvalidVersion "sdk version" (parseSdkVersion rawSdkVersion)
      pure ParsedSdkVersion
        { unParsedSdkVersion = mkReleaseVersion releaseVersion sdkVersion
        , isPrerelease
        }
    where
      handleInvalidVersion :: String -> Either InvalidVersion a -> Parser a
      handleInvalidVersion versionName (Left (InvalidVersion src msg)) =
        fail $ "Invalid " <> versionName <> " string `" <> unpack src <> "` for reason: " <> msg
      handleInvalidVersion _ (Right a) = pure a

maximumOfNonEmptyVersions :: IO ([ReleaseVersion], CacheAge) -> IO ReleaseVersion
maximumOfNonEmptyVersions getVersions = do
    (versions, _cacheAge) <- getVersions
    case maximumMay versions of
      Nothing -> throwIO $ assistantError $ pack "Version list is empty."
      Just m -> pure m

-- | Get the latest released SDK version
freshMaximumOfVersions :: IO ([ReleaseVersion], CacheAge) -> IO (Maybe ReleaseVersion)
freshMaximumOfVersions getVersions = do
    (versions, cacheAge) <- getVersions
    case cacheAge of
      Stale -> pure Nothing
      Fresh -> pure (maximumMay versions)

-- | Get the latest snapshot SDK version.
getLatestSdkSnapshotVersion :: UseCache -> IO ReleaseVersion
getLatestSdkSnapshotVersion useCache = do
    maximumOfNonEmptyVersions (getAvailableSdkSnapshotVersions useCache)

getLatestReleaseVersion :: UseCache -> IO ReleaseVersion
getLatestReleaseVersion useCache =
    maximumOfNonEmptyVersions (getAvailableReleaseVersions useCache)

data CouldNotResolveVersion
  = CouldNotResolveReleaseVersion UnresolvedReleaseVersion
  | CouldNotResolveSdkVersion SdkVersion
  deriving (Show, Eq, Ord)

instance Exception CouldNotResolveVersion where
    displayException (CouldNotResolveReleaseVersion version) = "Could not resolve release version " <> T.unpack (V.toText (unwrapUnresolvedReleaseVersion version))
    displayException (CouldNotResolveSdkVersion version) = "Could not resolve SDK version " <> T.unpack (V.toText (unwrapSdkVersion version)) <> " to a release version. Possible fix: `daml version --force-reload yes`?"

resolveReleaseVersion :: HasCallStack => UseCache -> UnresolvedReleaseVersion -> IO ReleaseVersion
resolveReleaseVersion _ targetVersion | isHeadVersion targetVersion = pure headReleaseVersion
resolveReleaseVersion useCache targetVersion = do
    resolved <- resolveReleaseVersionFromDamlPath (damlPath useCache) targetVersion
    case resolved of
      Just resolved -> pure resolved
      Nothing -> do
        let isTargetVersion version =
              unwrapUnresolvedReleaseVersion targetVersion == releaseVersionFromReleaseVersion version
        (releaseVersions, _) <- getAvailableSdkSnapshotVersions useCache
        case filter isTargetVersion releaseVersions of
          (x:_) -> pure x
          [] -> do
              releasedVersionE <- resolveReleaseVersionFromGithub targetVersion
              case releasedVersionE of
                Left _ ->
                  throwIO (CouldNotResolveReleaseVersion targetVersion)
                Right releasedVersion -> do
                  _ <- cacheAvailableSdkVersions useCache (\pre -> pure (releasedVersion : fromMaybe [] pre))
                  pure releasedVersion

resolveSdkVersionToRelease :: UseCache -> SdkVersion -> IO (Either CouldNotResolveVersion ReleaseVersion)
resolveSdkVersionToRelease _ targetVersion | isHeadVersion targetVersion = pure (Right headReleaseVersion)
resolveSdkVersionToRelease useCache targetVersion = do
    resolved <- resolveSdkVersionFromDamlPath (damlPath useCache) targetVersion
    case resolved of
      Just resolved -> pure (Right resolved)
      Nothing -> do
        let isTargetVersion version =
              targetVersion == sdkVersionFromReleaseVersion version
        (releaseVersions, _) <- getAvailableSdkSnapshotVersions useCache
        case filter isTargetVersion releaseVersions of
          (x:_) -> pure $ Right x
          [] -> pure $ Left $ CouldNotResolveSdkVersion targetVersion

resolveReleaseVersionFromDamlPath :: DamlPath -> UnresolvedReleaseVersion -> IO (Maybe ReleaseVersion)
resolveReleaseVersionFromDamlPath damlPath targetVersion = do
  let isMatchingVersion releaseVersion =
          unwrapUnresolvedReleaseVersion targetVersion == releaseVersionFromReleaseVersion releaseVersion
  resolvedVersions <- getInstalledSdkVersions damlPath
  pure (find isMatchingVersion resolvedVersions)

resolveSdkVersionFromDamlPath :: DamlPath -> SdkVersion -> IO (Maybe ReleaseVersion)
resolveSdkVersionFromDamlPath damlPath targetSdkVersion = do
  let isMatchingVersion releaseVersion =
          targetSdkVersion == sdkVersionFromReleaseVersion releaseVersion
  resolvedVersions <- getInstalledSdkVersions damlPath
  pure (find isMatchingVersion resolvedVersions)

-- | Subset of the github release response that we care about
data GithubReleaseResponseSubset = GithubReleaseResponseSubset
  { assetNames :: [T.Text] }

instance FromJSON GithubReleaseResponseSubset where
  -- Akin to `GithubReleaseResponseSubset . fmap name . assets` but lifted into a parser over json
  parseJSON = withObject "GithubReleaseResponse" $ \v ->
    GithubReleaseResponseSubset <$> explicitParseField (listParser (withObject "GithubRelease" (.: "name"))) v "assets"

releaseResponseSubsetSdkVersion :: GithubReleaseResponseSubset -> Maybe T.Text
releaseResponseSubsetSdkVersion responseSubset =
  let extractMatchingName :: T.Text -> Maybe T.Text
      extractMatchingName name = do
        withoutExt <- T.stripSuffix "-linux.tar.gz" name
        T.stripPrefix "daml-sdk-" withoutExt
  in
  listToMaybe $ mapMaybe extractMatchingName (assetNames responseSubset)

data GithubReleaseError
  = FailedToFindLinuxSdkInRelease String
  | Couldn'tParseSdkVersion String InvalidVersion
  deriving (Show, Eq)

instance Exception GithubReleaseError where
  displayException (FailedToFindLinuxSdkInRelease url) =
    "Couldn't find Linux SDK in release at url: '" <> url <> "'"
  displayException (Couldn'tParseSdkVersion url v) =
    "Couldn't parse SDK in release at url '" <> url <> "': " <> displayException v

-- | Since ~2.8.snapshot, the "enterprise version" (the version the user inputs) and the daml sdk version (the version of the daml repo) can differ
-- As such, we derive the latter via the github api `assets` endpoint, looking for a file matching the expected `daml-sdk-$VERSION-$OS.tar.gz`
resolveReleaseVersionFromGithub :: UnresolvedReleaseVersion -> IO (Either GithubReleaseError ReleaseVersion)
resolveReleaseVersionFromGithub unresolvedVersion = do
  let tag = T.unpack (rawVersionToTextWithV (unwrapUnresolvedReleaseVersion unresolvedVersion))
  let url = "https://api.github.com/repos/digital-asset/daml/releases/tags/" <> tag
  req <- parseRequest url
  res <- httpJSON $ setRequestHeaders [("User-Agent", "request")] req
  pure $
    case releaseResponseSubsetSdkVersion (getResponseBody res) of
      Nothing -> Left (FailedToFindLinuxSdkInRelease url)
      Just sdkVersionStr ->
        case parseSdkVersion sdkVersionStr of
          Left issue -> Left (Couldn'tParseSdkVersion url issue)
          Right sdkVersion -> Right (mkReleaseVersion unresolvedVersion sdkVersion)

