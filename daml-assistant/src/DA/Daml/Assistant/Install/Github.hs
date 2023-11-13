-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Discover releases from the digital-asset/daml github.
module DA.Daml.Assistant.Install.Github
    ( versionLocation
    , tagToVersion
    , osName
    , getSdkVersionFromEnterpriseVersion
    ) where

import Control.Exception.Safe
import DA.Daml.Assistant.Types
import Data.Aeson
import Data.Aeson.Types (explicitParseField, listParser)
import Data.Either.Extra
import Data.Maybe (fromMaybe, listToMaybe, mapMaybe)
import Data.Text qualified as T
import Network.HTTP.Simple (getResponseBody, httpJSON, parseRequest, setRequestHeaders)
import System.Info qualified

-- | General git tag. We only care about the tags of the form "v<VERSION>"
-- where <VERSION> is an SDK version. For example, "v0.11.1".
newtype Tag = Tag { unTag :: Text } deriving (Eq, Show, FromJSON)


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

-- | OS-specific part of the asset name.
osName :: Text
osName = case System.Info.os of
    "darwin"  -> "macos"
    "linux"   -> "linux"
    "mingw32" -> "windows"
    p -> error ("daml: Unknown operating system " ++ p)

-- | Install location for particular version.
versionLocation :: SdkVersion -> SdkVersion -> InstallLocation
versionLocation releaseVersion sdkVersion = InstallLocation
    { ilUrl = T.concat
        [ "https://github.com/digital-asset/daml/releases/download/"
        , unTag (versionToTag releaseVersion)
        , "/daml-sdk-"
        , versionToText sdkVersion
        , "-"
        , osName
        , ".tar.gz"
        ]
    , ilHeaders = []
    }

-- | Subset of the github release response that we care about
data GithubReleaseResponseSubset = GithubReleaseResponseSubset
  { assetNames :: [T.Text] }

instance FromJSON GithubReleaseResponseSubset where
  -- Akin to `GithubReleaseResponseSubset . fmap name . assets` but lifted into a parser over json
  parseJSON = withObject "GithubReleaseResponse" $ \v ->
    GithubReleaseResponseSubset <$> explicitParseField (listParser (withObject "GithubRelease" (.: "name"))) v "assets"

-- | Since ~2.8.snapshot, the "enterprise version" (the version the user inputs) and the daml sdk version (the version of the daml repo) can differ
-- As such, we derive the latter via the github api `assets` endpoint, looking for a file matching the expected `daml-sdk-$VERSION-$OS.tar.gz`
getSdkVersionFromEnterpriseVersion :: SdkVersion -> IO SdkVersion
getSdkVersionFromEnterpriseVersion v = do
  req <- parseRequest $ "https://api.github.com/repos/digital-asset/daml/releases/tags/" <> T.unpack (unTag $ versionToTag v)
  res <- httpJSON $ setRequestHeaders [("User-Agent", "request")] req
  let mSdkVersionStr =
        listToMaybe $ flip mapMaybe (assetNames $ getResponseBody res) $ \name -> do
          withoutExt <- T.stripSuffix "-linux.tar.gz" name
          T.stripPrefix "daml-sdk-" withoutExt
      sdkVersionStr = fromMaybe (error "Failed to find linux sdk in release") mSdkVersionStr
  either throwIO pure $ parseVersion sdkVersionStr
