-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Discover releases from the digital-asset/daml github.
module DAML.Assistant.Install.Github
    ( latestURL
    , versionURL
    ) where

import DAML.Assistant.Types
import DAML.Assistant.Util
import Data.Aeson
import Network.HTTP.Simple
import Control.Exception.Safe
import Control.Monad
import Data.List
import Data.Either.Extra
import qualified System.Info
import qualified Data.Text as T

-- | General git tag. We only care about the tags of the form "v<VERSION>"
-- where <VERSION> is an SDK version. For example, "v0.11.1".
newtype Tag = Tag { unTag :: Text } deriving (Eq, Show, FromJSON)

-- | Name of github release artifact. We only care about artifacts
-- with the name "daml-sdk-<VERSION>-<OS>.tar.gz" where <VERSION>
-- is an SDK version and <OS> is "linux", "macos", or "win". For
-- example, "daml-sdk-0.11.1-linux.tar.gz".
newtype AssetName = AssetName { unAssetName :: Text } deriving (Eq, Show, FromJSON)

-- | GitHub release metadata, such as can be optained through the
-- GitHub releases API v3. This is only a small fragment of the
-- data available. For more information please visit:
--
--      https://developer.github.com/v3/repos/releases/
--
data Release = Release
    { releaseTag          :: Tag
    , releaseIsPrerelease :: Bool
    , releaseAssets       :: [Asset]
    } deriving (Eq, Show)

instance FromJSON Release where
    parseJSON = withObject "Release" $ \r ->
        Release
        <$> r .: "tag_name"
        <*> r .: "prerelease"
        <*> r .: "assets"

-- | GitHub release artifact metadata, as contained in the "assets"
-- field of the release metadata structure. This is only a small
-- fragment of the data available. For more information please visit:
--
--      https://developer.github.com/v3/repos/releases/
--
data Asset = Asset
    { assetName :: AssetName
    , assetDownloadURL :: InstallURL
    } deriving (Eq, Show)

instance FromJSON Asset where
    parseJSON = withObject "Asset" $ \r ->
        Asset
        <$> r .: "name"
        <*> r .: "browser_download_url"

-- | Convert a version to a git tag.
versionToTag :: SdkVersion -> Tag
versionToTag v = Tag ("v" <> versionToText v)

-- | Attempt to convert a git tag into an SDK version. Not all
-- git tags correspond to versions, resulting in an error. The
-- tags that do correspond to versions have the form "v<VERSION>"
-- where <VERSION> is a valid SDK version (i.e. a semantic
-- version).
tagToVersion :: Tag -> Either AssistantError SdkVersion
tagToVersion (Tag t) =
    mapLeft (assistantErrorBecause ("Tag " <> t <> "does not represent a valid SDK version.")) $
        if T.take 1 t == "v" then
            mapLeft (pack . displayException) $ parseVersion (T.drop 1 t)
        else
            Left "Tag must start with v followed by semantic version."

-- | Make a request to the Github API. There is an unauthenticated user rate limit
-- of 60 requests per hour, so we should limit requests. (One way for the user to
-- avoid API requests is to give the version directly.)
makeAPIRequest :: FromJSON t => Text -> IO t
makeAPIRequest path = do
    request <- parseRequest (unpack ("GET https://api.github.com/repos/digital-asset/daml" <> path))
    response <- httpJSON (setRequestHeader "User-Agent" ["daml"] request)
    when (getResponseStatusCode response /= 200) $
        throwString . show $ getResponseStatus response
    pure (getResponseBody response)

-- | Get the latest stable (i.e. non-prerelease) release.
getLatestRelease :: IO Release
getLatestRelease =
    requiredIO "Failed to get latest SDK release from github." $
        makeAPIRequest "/releases/latest"

-- | Extract an install URL from release data.
getReleaseURL :: Release -> Either AssistantError InstallURL
getReleaseURL Release{..} = do
    version <- tagToVersion releaseTag
    let target = versionToAssetName version
    asset <- fromMaybeM
        (Left (assistantErrorBecause "Could not find required SDK distribution in github release."
            ("Looked for " <> unAssetName target <> " but got [" <>
                T.intercalate ", " (map (unAssetName . assetName) releaseAssets) <> "].")))
        (find ((== target) . assetName) releaseAssets)
    pure (assetDownloadURL asset)

-- | Github release artifact name.
versionToAssetName :: SdkVersion -> AssetName
versionToAssetName v =
    AssetName
        ("daml-sdk-" <> versionToText v
        <> "-" <> osName <> ".tar.gz")

-- | OS-specific part of the asset name.
osName :: Text
osName = case System.Info.os of
    "darwin"  -> "macos"
    "linux"   -> "linux"
    "mingw32" -> "win"
    p -> error ("daml: Unknown operating system " ++ p)

-- | Install URL for particular version.
versionURL :: SdkVersion -> InstallURL
versionURL v = InstallURL $ T.concat
    [ "https://github.com/digital-asset/daml/releases/download/"
    , unTag (versionToTag v)
    , "/"
    , unAssetName (versionToAssetName v)
    ]

-- | Get install URL for latest stable version.
latestURL :: IO InstallURL
latestURL = getLatestRelease >>= fromRightM throwIO . getReleaseURL
