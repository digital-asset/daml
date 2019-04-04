-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DA.Sdk.Cli.Conf.NameToProp
    ( nameValToProp
    , nameInfo
    , infoKeysToPath
    , parsePositive
    ) where

import DA.Sdk.Cli.Conf.Types
import DA.Sdk.Prelude
import DA.Sdk.Version
import qualified Data.Text as T
import Servant.Client (BaseUrl(..), parseBaseUrl)
import Text.Read (readEither)
import Data.Bool

nameValToProp :: Name -> Text -> Either Text Prop
-- nameValToProp NameHomePath            = ??? -- should we let the user change the home?
nameValToProp NameUserEmail              = Right . PropUserEmail
nameValToProp NameProjectName            = Right . PropProjectName
nameValToProp NameProjectSDKVersion      = fmap PropProjectSDKVersion . parseSemVersionE
nameValToProp NameProjectParties         = fmap PropProjectParties . parseCommaSepNonEmptyVal
nameValToProp NameDAMLSource             = Right . PropDAMLSource
nameValToProp NameDAMLScenario           = fmap PropDAMLScenario . parseDAMLScenario
nameValToProp NameBintrayUsername        = fmap PropBintrayUsername . requireNonEmpty "Bintray username cannot be empty" id
nameValToProp NameBintrayKey             = fmap PropBintrayKey . requireNonEmpty "Bintray key cannot be empty" id
nameValToProp NameBintrayAPIURL          = fmap PropBintrayAPIURL . parseUrl
nameValToProp NameBintrayDownloadURL     = fmap PropBintrayDownloadURL . parseUrl
nameValToProp NameSDKDefaultVersion      = fmap (PropSDKDefaultVersion . Just) . parseSemVersionE
nameValToProp NameSDKIncludedTags        = fmap PropSDKIncludedTags . parseCommaSepNonEmptyVal
nameValToProp NameSDKExcludedTags        = fmap PropSDKExcludedTags . parseCommaSepNonEmptyVal
nameValToProp NameUpgradeIntervalSeconds = fmap PropUpgradeIntervalSeconds . parsePositive "interval between two upgrades"
nameValToProp NameUpdateChannel          = Right . PropUpdateChannel . Just
nameValToProp NameNameSpaces             = fmap (PropNameSpaces . map NameSpace) . parseCommaSepNonEmptyVal
nameValToProp NameSelfUpdate             = fmap PropSelfUpdate . parseUpdateSetting
nameValToProp NameOutputPath             = Right . PropOutputPath . Just . textToPath
nameValToProp n                          = const . Left $ "Cannot set property '" <> (infoKeysToPath . infoKeys . nameInfo) n <> "'"

parsePositive :: Text -> Text -> Either Text Int
parsePositive what raw = case readEither (T.unpack raw) of
    Left _          -> Left $ "Cannot parse '" <> raw <> "' as " <> what
    Right i | i < 0 -> Left $ "The " <> what <> " cannot be a negative number (found: '" <> T.pack (show i) <> "')"
    Right i         -> Right i

parseSemVersionE :: Text -> Either Text SemVersion
parseSemVersionE raw = maybe (Left $ "Cannot parse '" <> raw <>"' as version") Right $ parseSemVersion raw

requireNonEmpty :: e -> (Text -> a) -> Text -> Either e a
requireNonEmpty e f t = bool (Right $ f t) (Left e) (T.null t)

parseCommaSepNonEmptyVal :: Text -> Either Text [Text]
parseCommaSepNonEmptyVal = traverse (requireNonEmpty "Party cannot be empty" id) . T.split (== ',')

-- for now we can only set the scenario but in future we may need unset too
parseDAMLScenario :: Text -> Either Text (Maybe Text)
parseDAMLScenario = fmap Just . requireNonEmpty "Scenario cannot be empty" id

parseUrl :: Text -> Either Text BaseUrl
parseUrl raw = either (\e -> Left $ "Cannot parse URL '" <> raw <> "':" <> T.pack (displayException e)) Right $ parseBaseUrl $ T.unpack raw

-- | Return the path given the infoKeys
--
-- See NameInfo
--
-- Examples:
--
-- >>> infoKeysToPath ["bintray", "username"]
-- "bintray.username"
infoKeysToPath :: [Text] -> Text
infoKeysToPath = T.intercalate "."

-- | Return info about a property. Only returns info about properties that can
-- be specified in a config file.
nameInfo :: Name -> NameInfo
nameInfo NameConfVersion         = NameInfo ["version"]
    (Just "The config file version. This helps the SDK Assistant manage backwards compatibility of da.yaml files.")
nameInfo NameLogLevel            = NameInfo ["cli", "log-level"] Nothing
    -- (Just "What level of logging should the SDK Assistant print? Options are: debug, info, warn, error")
nameInfo NameIsScript            = NameInfo ["cli", "script-mode"]
    (Just "Run SDK Assistant in non-interactive mode without attempting self-upgrade.")
nameInfo NameUpdateChannel       = NameInfo ["cli", "update-channel"]
    (Just "Update channel to use for the SDK Assistant self-updates.")
nameInfo NameHomePath            = NameInfo ["cli", "home-path"] Nothing
nameInfo NameUpgradeIntervalSeconds =
                                   NameInfo ["cli", "upgrade-interval"] Nothing
nameInfo NameUserEmail           = NameInfo ["user", "email"]
    (Just "Your email address.")
nameInfo NameProjectName         = NameInfo ["project", "name"]
    (Just "The project name.")
nameInfo NameProjectSDKVersion   = NameInfo ["project", "sdk-version"]
    (Just "The project SDK version.")
nameInfo NameProjectParties      = NameInfo ["project", "parties"]
    (Just "Names of parties active in the project.")
nameInfo NameDAMLSource          = NameInfo ["project", "source"]
    (Just "The DAML file the Sandbox should load.")
nameInfo NameDAMLScenario        = NameInfo ["project", "scenario"]
    (Just "Optional: Scenario that the Sandbox should run. For example: Example.myScenario")
nameInfo NameOutputPath          = NameInfo ["project", "output-path"]
    (Just "Output path for DAML compilation.")
nameInfo NameBintrayUsername     = NameInfo ["bintray", "username"]
    (Just "Your Bintray username. Required in order for the SDK Assistant to download SDK releases.")
nameInfo NameBintrayKey          = NameInfo ["bintray", "key"]
    (Just "Your Bintray API Key. Required in order for the SDK Assistant to download SDK releases.")
nameInfo NameBintrayAPIURL       = NameInfo ["bintray", "api-url"]
    (Just "The Bintray API URL.")
nameInfo NameBintrayDownloadURL  = NameInfo ["bintray", "download-url"]
    (Just "The Bintray download URL.")
nameInfo NameSDKDefaultVersion   = NameInfo ["sdk", "default-version"]
    (Just "The SDK version to use by default when starting new projects and similar.")
nameInfo NameSDKIncludedTags     = NameInfo ["sdk", "included-tags"]
    (Just "Only SDK releases with at least one of these tags will be available.")
nameInfo NameSDKExcludedTags     = NameInfo ["sdk", "excluded-tags"]
    (Just "SDK releases tagged with one of these tags will be ignored.")
nameInfo NameTermWidth           = NameInfo ["term", "width"]
    (Just "Rendering width of the terminal.")
nameInfo NameNameSpaces          = NameInfo ["templates", "namespaces"]
    (Just "The SDK template namespaces you are subscribed to.")
nameInfo NameSelfUpdate          = NameInfo ["update-setting"]
    (Just "The current self-update setting. Possible values are: Always, RemindLater, RemindNext, Never.")