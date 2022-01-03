-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

newtype ArtifactoryApiKey = ArtifactoryApiKey
    { unwrapArtifactoryApiKey :: Text
    } deriving (Eq, Show, FromJSON)

queryArtifactoryApiKey :: DamlConfig -> Maybe ArtifactoryApiKey
queryArtifactoryApiKey damlConfig =
     eitherToMaybe (queryDamlConfigRequired ["artifactory-api-key"] damlConfig)

-- | Install location for particular version.
versionLocation :: SdkVersion -> ArtifactoryApiKey -> InstallLocation
versionLocation v apiKey = InstallLocation
    { ilUrl = T.concat
        [ "https://digitalasset.jfrog.io/artifactory/sdk-ee/"
        , versionToText v
        , "/daml-sdk-"
        , versionToText v
        , "-"
        , osName
        , "-ee.tar.gz"
        ]
    , ilHeaders =
        [("X-JFrog-Art-Api", T.encodeUtf8 (unwrapArtifactoryApiKey apiKey))]
    }
