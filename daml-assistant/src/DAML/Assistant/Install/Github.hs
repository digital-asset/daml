-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

-- | Discover releases from the digital-asset/daml github.
module DAML.Assistant.Install.Github
    ( versionURL
    , getLatestVersion
    ) where

import DAML.Assistant.Types
import DAML.Assistant.Util
import Data.Aeson
import Network.HTTP.Client
import Network.HTTP.Client.TLS
import Control.Exception.Safe
import Control.Monad
import Data.Either.Extra
import qualified System.Info
import qualified Data.Text as T
import qualified Data.Text.Encoding as T

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

-- | Get the version of the latest stable (i.e. non-prerelease) release.
-- We avoid the Github API because of very low rate limits. As such, we
-- discover the latest version by parsing an HTTP redirect. We make a
-- request to:
--
--     https://github.com/digital-asset/daml/releases/latest
--
-- Which always redirects to the latest stable release, for example:
--
--     https://github.com/digital-asset/daml/releases/tag/v0.12.3
--
-- So we take that URL to get the tag, and from there the version of
-- the latest stable release.
getLatestVersion ::  IO SdkVersion
getLatestVersion = do

    manager <- newTlsManager -- TODO: share a single manager throughout the daml install process.
    request <- parseRequest "HEAD https://github.com/digital-asset/daml/releases/latest"
    finalRequest <- requiredIO "Failed to get latest SDK version from GitHub." $
        withResponseHistory request manager $ pure . hrFinalRequest

    let pathText = T.decodeUtf8 (path finalRequest)
        (parent, tag) = T.breakOnEnd "/" pathText
        prefix = "/digital-asset/daml/releases/tag/"

    when (parent /= prefix) $ do
        throwIO $ assistantErrorBecause
            "Failed to get latest SDK version from GitHub."
            ("Unexpected final HTTP redirect location.\n    Expected: " <> prefix <> "TAG\n    Got: " <> pathText)

    fromRightM throwIO (tagToVersion (Tag tag))


-- | OS-specific part of the asset name.
osName :: Text
osName = case System.Info.os of
    "darwin"  -> "macos"
    "linux"   -> "linux"
    "mingw32" -> "windows"
    p -> error ("daml: Unknown operating system " ++ p)

-- | Install URL for particular version.
versionURL :: SdkVersion -> InstallURL
versionURL v = InstallURL $ T.concat
    [ "https://github.com/digital-asset/daml/releases/download/"
    , unTag (versionToTag v)
    , "/daml-sdk-"
    , versionToText v
    , "-"
    , osName
    , ".tar.gz"
    ]
