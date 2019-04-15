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
newtype Tag = Tag Text deriving (Eq, Show, FromJSON)

-- | Name of asset in a github release. We only care about assets
-- with the name "daml-sdk-<VERSION>-<OS>.tar.gz" where <VERSION>
-- is an SDK version and <OS> is "linux", "macos", or "win". For
-- example, "daml-sdk-0.11.1-linux.tar.gz".
newtype AssetName = AssetName { unAssetName :: Text } deriving (Eq, Show, FromJSON)

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

data Asset = Asset
    { assetName :: AssetName
    , assetDownloadURL :: InstallURL
    } deriving (Eq, Show)

instance FromJSON Asset where
    parseJSON = withObject "Asset" $ \r ->
        Asset
        <$> r .: "name"
        <*> r .: "browser_download_url"

versionToTag :: SdkVersion -> Tag
versionToTag v = Tag ("v" <> versionToText v)

tagToVersion :: Tag -> Either AssistantError SdkVersion
tagToVersion (Tag t) =
    mapLeft (assistantErrorBecause ("Tag " <> t <> "does not represent a valid SDK version.")) $
        if T.take 1 t == "v" then
            mapLeft (pack . displayException) $ parseVersion (T.drop 1 t)
        else
            Left "Tag must start with v followed by semantic version."


makeAPIRequest :: FromJSON t => Text -> IO t
makeAPIRequest path = do
    request <- parseRequest (unpack ("GET https://api.github.com/repos/digital-asset/daml" <> path))
    response <- httpJSON (setRequestHeader "User-Agent" ["daml"] request)
    when (getResponseStatusCode response /= 200) $
        throwString . show $ getResponseStatus response
    pure (getResponseBody response)

getLatestRelease :: IO Release
getLatestRelease =
    requiredIO "Failed to get latest SDK release from github." $
        makeAPIRequest "/releases/latest"

getVersionRelease :: SdkVersion -> IO Release
getVersionRelease v = do
    let Tag t = versionToTag v
    requiredIO ("Failed to get SDK release " <> versionToText v <> " from github.") $
        makeAPIRequest ("/releases/tags/" <> t)

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

versionToAssetName :: SdkVersion -> AssetName
versionToAssetName v =
    AssetName
        ("daml-sdk-" <> versionToText v
        <> "-" <> osName <> ".tar.gz")

osName :: Text
osName = case System.Info.os of
    "darwin"  -> "macos"
    "linux"   -> "linux"
    "mingw32" -> "win"
    p -> error ("daml: Unknown operating system " ++ p)

versionURL :: SdkVersion -> IO InstallURL
versionURL v = getVersionRelease v >>= fromRightM throwIO . getReleaseURL

latestURL :: IO InstallURL
latestURL = getLatestRelease >>= fromRightM throwIO . getReleaseURL
