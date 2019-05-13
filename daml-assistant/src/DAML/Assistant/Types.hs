-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Types
    ( module DAML.Assistant.Types
    , module DAML.Project.Types
    , Text, pack, unpack -- convenient re-exports
    ) where

import DAML.Project.Types
import qualified Data.Text as T
import Data.Aeson (FromJSON)
import Data.Text (Text, pack, unpack)
import Data.Maybe
import Control.Exception.Safe

data AssistantError = AssistantError
    { errContext  :: Maybe Text -- ^ Context in which error occurs.
    , errMessage  :: Maybe Text -- ^ User-friendly error message.
    , errInternal :: Maybe Text -- ^ Internal error message, i.e. what actually happened.
    } deriving (Eq, Show)

instance Exception AssistantError where
    displayException AssistantError {..} = unpack . T.unlines . catMaybes $
        [ Just ("daml: " <> fromMaybe "An unknown error has occured" errMessage)
        , fmap ("  context: " <>) errContext
        , fmap ("  details: " <>) errInternal
        ]

-- | Standard error message.
assistantError :: Text -> AssistantError
assistantError msg = AssistantError
    { errContext = Nothing
    , errMessage = Just msg
    , errInternal = Nothing
    }

-- | Standard error message with additional internal cause.
assistantErrorBecause ::  Text -> Text -> AssistantError
assistantErrorBecause msg e = (assistantError msg) { errInternal = Just e }

data Env = Env
    { envDamlPath      :: DamlPath
    , envDamlAssistantPath :: DamlAssistantPath
    , envDamlAssistantSdkVersion :: Maybe DamlAssistantSdkVersion
    , envProjectPath   :: Maybe ProjectPath
    , envSdkPath       :: Maybe SdkPath
    , envSdkVersion    :: Maybe SdkVersion
    , envLatestStableSdkVersion :: Maybe SdkVersion
    } deriving (Eq, Show)

data BuiltinCommand
    = Version
    | Exec String [String]
    | Install InstallOptions
    deriving (Eq, Show)

data Command
    = Builtin BuiltinCommand
    | Dispatch SdkCommandInfo UserCommandArgs
    deriving (Eq, Show)

newtype UserCommandArgs = UserCommandArgs
    { unwrapUserCommandArgs :: [String]
    } deriving (Eq, Show)

data InstallOptions = InstallOptions
    { iTargetM :: Maybe RawInstallTarget
    , iActivate :: ActivateInstall
    , iForce :: ForceInstall
    , iQuiet :: QuietInstall
    , iSetPath :: SetPath
    } deriving (Eq, Show)

-- | An install URL is a fully qualified HTTP[S] URL to an SDK release tarball. For example:
-- "https://github.com/digital-asset/daml/releases/download/v0.11.1/daml-sdk-0.11.1-macos.tar.gz"
newtype InstallURL = InstallURL
    { unwrapInstallURL :: Text
    } deriving (Eq, Show, FromJSON)

newtype RawInstallTarget = RawInstallTarget String deriving (Eq, Show)
newtype ForceInstall = ForceInstall Bool deriving (Eq, Show)
newtype QuietInstall = QuietInstall Bool deriving (Eq, Show)
newtype ActivateInstall = ActivateInstall Bool deriving (Eq, Show)
newtype SetPath = SetPath Bool deriving (Eq, Show)
