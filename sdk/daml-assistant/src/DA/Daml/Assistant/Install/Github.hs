-- Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Discover releases from the digital-asset/daml github.
module DA.Daml.Assistant.Install.Github
    ( versionLocation
    , tagToVersion
    , osName
    ) where

import DA.Daml.Assistant.Types
import Data.Aeson
import Control.Exception.Safe
import Data.Either.Extra
import qualified System.Info
import qualified Data.Text as T

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
versionLocation :: SdkVersion -> InstallLocation
versionLocation v = InstallLocation
    { ilUrl = T.concat
        [ "https://github.com/digital-asset/daml/releases/download/"
        , unTag (versionToTag v)
        , "/daml-sdk-"
        , versionToText v
        , "-"
        , osName
        , ".tar.gz"
        ]
    , ilHeaders = []
    }
