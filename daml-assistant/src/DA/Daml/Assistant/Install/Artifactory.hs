-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Discover releases from the artifactory.
module DA.Daml.Assistant.Install.Artifactory
    ( versionLocation
    , queryArtifactoryApiKey
    , ArtifactoryApiKey(..)
    ) where

import DA.Daml.Assistant.Install.Github hiding (versionLocation)
import DA.Daml.Assistant.Types
import DA.Daml.Project.Config
import Data.Aeson
import Data.Either.Extra
import Data.Text qualified as T
import Data.Text.Encoding qualified as T

newtype ArtifactoryApiKey = ArtifactoryApiKey
    { unwrapArtifactoryApiKey :: Text
    } deriving (Eq, Show, FromJSON)

queryArtifactoryApiKey :: DamlConfig -> Maybe ArtifactoryApiKey
queryArtifactoryApiKey damlConfig =
     eitherToMaybe (queryDamlConfigRequired ["artifactory-api-key"] damlConfig)

-- | Install location for particular version.
-- NOTE THIS TAKES THE SDK VERSION, not the release version that `Github.versionLocation` uses
versionLocation :: SdkVersion -> ArtifactoryApiKey -> InstallLocation
versionLocation sdkVersion apiKey = InstallLocation
    { ilUrl = T.concat
        [ "https://digitalasset.jfrog.io/artifactory/sdk-ee/"
        , versionToText sdkVersion
        , "/daml-sdk-"
        , versionToText sdkVersion
        , "-"
        , osName
        , "-ee.tar.gz"
        ]
    , ilHeaders =
        [("X-JFrog-Art-Api", T.encodeUtf8 (unwrapArtifactoryApiKey apiKey))]
    }
