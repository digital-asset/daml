-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE NoImplicitPrelude #-}
{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Conf.Consts
    ( latestConfVersion
    , defaultBintrayAPIURL
    , defaultBintrayDownloadURL
    , defaultRepositoryURLs
    , defaultLogLevel
    , defaultIsScript
    , defaultUpgradeIntervalSeconds
    , wildcardTag
    , defaultIncludedTags
    , defaultExcludedTags
    , defaultNameSpace
    , sdkNameSpace
    , createDownloadUrl
    ) where

import DA.Sdk.Prelude
import DA.Sdk.Cli.Conf.Types
import Servant.Client                (BaseUrl(..), Scheme(Https))
import Control.Monad.Logger          (LogLevel (..))

latestConfVersion :: Int
latestConfVersion = 2

defaultBintrayAPIURL, defaultBintrayDownloadURL :: BaseUrl
defaultBintrayAPIURL = BaseUrl Https "bintray.com" 443 "/api/v1"
defaultBintrayDownloadURL = createDownloadUrl defaultBintrayAPIURL

defaultRepositoryURLs :: RepositoryURLs
defaultRepositoryURLs = RepositoryURLs defaultBintrayAPIURL defaultBintrayDownloadURL

defaultUpgradeIntervalSeconds :: Int
defaultUpgradeIntervalSeconds = 3600

defaultLogLevel :: LogLevel
defaultLogLevel = LevelInfo

defaultIsScript :: Bool
defaultIsScript = False

createDownloadUrl :: BaseUrl -> BaseUrl
createDownloadUrl bUrl = bUrl { baseUrlHost = dlHost, baseUrlPath = "" }
  where
    host = baseUrlHost bUrl
    dlHost = "dl." <> host


--------------------------------------------------------------------------------
-- Tags
--------------------------------------------------------------------------------

wildcardTag :: Text
wildcardTag = "*"

defaultIncludedTags :: [Text]
defaultIncludedTags = ["visible-external"]

defaultExcludedTags :: [Text]
defaultExcludedTags = ["bad", "deprecated"]

defaultNameSpace :: NameSpace
defaultNameSpace = sdkNameSpace

sdkNameSpace :: NameSpace
sdkNameSpace = NameSpace "sdk"
