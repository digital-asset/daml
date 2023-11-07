-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Discover releases from the digital-asset/daml github.
module DA.Daml.Project.ReleaseResolution
    ( githubVersionLocation
    , artifactoryVersionLocation
    , osName
    , queryArtifactoryApiKey
    , ArtifactoryApiKey(..)
    , alternateVersionLocation
    ) where

import DA.Daml.Project.Types
import Data.Aeson
import Data.Either.Extra
import DA.Daml.Project.Config
import qualified System.Info
import Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as T
import qualified Data.SemVer as V

-- | OS-specific part of the asset name.
osName :: Text
osName = case System.Info.os of
    "darwin"  -> "macos"
    "linux"   -> "linux"
    "mingw32" -> "windows"
    p -> error ("daml: Unknown operating system " ++ p)

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
        , sdkVersionToText (sdkVersionFromReleaseVersion releaseVersion)
        , "/daml-sdk-"
        , sdkVersionToText (sdkVersionFromReleaseVersion releaseVersion)
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
        T.concat
          [ url
          , "/"
          , rawVersionToTextWithV (releaseVersionFromReleaseVersion releaseVersion)
          , "/daml-sdk-"
          , V.toText (unwrapSdkVersion (sdkVersionFromReleaseVersion releaseVersion))
          , "-"
          , osName
          , ".tar.gz"
          ]
    , ilHeaders = []
    }
