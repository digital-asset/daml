-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Discover releases from the digital-asset/daml github.
module DA.Daml.Project.ReleaseResolution
    ( githubVersionLocation
    , artifactoryVersionLocation
    -- , tagToVersion
    , osName
    , releaseResponseSubsetSdkVersion
    -- , getSdkVersionFromEnterpriseVersion
    , queryArtifactoryApiKey
    , ArtifactoryApiKey(..)
    , resolveReleaseVersionFromGithub
    , alternateVersionLocation
    ) where

import Control.Exception.Safe
import DA.Daml.Project.Types
import Data.Aeson
import Data.Aeson.Types (explicitParseField, listParser)
import Data.Either.Extra
import DA.Daml.Project.Config
import Data.Maybe (listToMaybe, mapMaybe)
import Network.HTTP.Simple (getResponseBody, httpJSON, parseRequest, setRequestHeaders)
import qualified System.Info
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.SemVer as V

-- | General git tag. We only care about the tags of the form "v<VERSION>"
-- where <VERSION> is an SDK version. For example, "v0.11.1".
newtype Tag = Tag { unTag :: Text } deriving (Eq, Show, FromJSON)


-- | Convert a version to a git tag.
versionToTag :: V.Version -> Tag
versionToTag v = Tag ("v" <> V.toText v)

    {-
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
    -}

-- | OS-specific part of the asset name.
osName :: Text
osName = case System.Info.os of
    "darwin"  -> "macos"
    "linux"   -> "linux"
    "mingw32" -> "windows"
    p -> error ("daml: Unknown operating system " ++ p)

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
  let tag = T.unpack (unTag (versionToTag (unwrapUnresolvedReleaseVersion unresolvedVersion)))
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

newtype ArtifactoryApiKey = ArtifactoryApiKey
    { unwrapArtifactoryApiKey :: Text
    } deriving (Eq, Show, FromJSON)

queryArtifactoryApiKey :: DamlConfig -> Maybe ArtifactoryApiKey
queryArtifactoryApiKey damlConfig =
     eitherToMaybe (queryDamlConfigRequired ["artifactory-api-key"] damlConfig)

-- | Install location for particular version.
artifactoryVersionLocation :: ReleaseVersion -> ArtifactoryApiKey -> InstallLocation
artifactoryVersionLocation releaseVersion apiKey = InstallLocation
    { ilUrl = T.concat
        [ "https://digitalasset.jfrog.io/artifactory/sdk-ee/"
        , V.toText (sdkVersionFromReleaseVersion releaseVersion)
        , "/daml-sdk-"
        , V.toText (sdkVersionFromReleaseVersion releaseVersion)
        , "-"
        , osName
        , "-ee.tar.gz"
        ]
    , ilHeaders =
        [("X-JFrog-Art-Api", T.encodeUtf8 (unwrapArtifactoryApiKey apiKey))]
    }

-- | Install location from Github for particular version.
githubVersionLocation :: ReleaseVersion -> InstallLocation
githubVersionLocation releaseVersion =
  alternateVersionLocation releaseVersion "https://github.com/digital-asset/daml/releases/download"

-- | Install location for particular version.
alternateVersionLocation :: ReleaseVersion -> Text -> InstallLocation
alternateVersionLocation releaseVersion url = InstallLocation
    { ilUrl =
        case releaseVersion of
          SplitReleaseVersion releaseVersion sdkVersion ->
            T.concat
              [ url
              , "/"
              , unTag (versionToTag releaseVersion)
              , "/daml-sdk-"
              , V.toText sdkVersion
              , "-"
              , osName
              , ".tar.gz"
              ]
          OldReleaseVersion releaseVersion ->
            T.concat
              [ url
              , "/"
              , unTag (versionToTag releaseVersion)
              , "/daml-sdk-"
              , V.toText releaseVersion
              , "-"
              , osName
              , ".tar.gz"
              ]
    , ilHeaders = []
    }
