-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


module DA.Daml.Assistant.Types
    ( module DA.Daml.Assistant.Types
    , module DA.Daml.Project.Types
    , YesNoAuto (..)
    , Text, pack, unpack -- convenient re-exports
    ) where

import DA.Daml.Project.Types
import qualified Data.Text as T
import Data.Aeson (FromJSON)
import Data.Text (Text, pack, unpack)
import Data.Maybe
import Options.Applicative.Extended (YesNoAuto (..))
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

-- | Standard error message with additional details.
assistantErrorDetails :: String -> [(String, String)] -> AssistantError
assistantErrorDetails msg details =
    assistantErrorBecause (pack msg) . pack . concat $
        ["\n    " <> k <> ": " <> v | (k,v) <- details]

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
    = Version VersionOptions
    | Exec String [String]
    | Install InstallOptions
    | Uninstall SdkVersion
    deriving (Eq, Show)

data Command
    = Builtin BuiltinCommand
    | Dispatch SdkCommandInfo UserCommandArgs
    deriving (Eq, Show)

newtype UserCommandArgs = UserCommandArgs
    { unwrapUserCommandArgs :: [String]
    } deriving (Eq, Show)

-- | Command-line options for daml version command.
data VersionOptions = VersionOptions
    { vAll :: Bool -- ^ show all versions (stable + snapshot)
    , vSnapshots :: Bool -- ^ show all snapshot versions
    , vAssistant :: Bool -- ^ show assistant version
    } deriving (Eq, Show)

-- | Command-line options for daml install command.
data InstallOptions = InstallOptions
    { iTargetM :: Maybe RawInstallTarget -- ^ version to install
    , iSnapshots :: Bool -- ^ include snapshots for latest target
    , iAssistant :: InstallAssistant -- ^ install the assistant
    , iActivate :: ActivateInstall -- ^ install the assistant if true (deprecated, delete with 0.14.x)
    , iForce :: ForceInstall -- ^ force reinstall if already installed
    , iQuiet :: QuietInstall -- ^ don't print messages
    , iSetPath :: SetPath -- ^ set the user's PATH (on Windows)
    , iBashCompletions :: BashCompletions -- ^ install bash completions for the daml assistant
    , iZshCompletions :: ZshCompletions -- ^ install Zsh completions for the daml assistant
    } deriving (Eq, Show)

-- | An install URL is a fully qualified HTTP[S] URL to an SDK release tarball. For example:
-- "https://github.com/digital-asset/daml/releases/download/v0.11.1/daml-sdk-0.11.1-macos.tar.gz"
newtype InstallURL = InstallURL
    { unwrapInstallURL :: Text
    } deriving (Eq, Show, FromJSON)

newtype RawInstallTarget = RawInstallTarget String deriving (Eq, Show)
newtype ForceInstall = ForceInstall { unForceInstall :: Bool } deriving (Eq, Show)
newtype QuietInstall = QuietInstall { unQuietInstall :: Bool } deriving (Eq, Show)
newtype ActivateInstall = ActivateInstall { unActivateInstall :: Bool } deriving (Eq, Show)
newtype SetPath = SetPath Bool deriving (Eq, Show)
newtype InstallAssistant = InstallAssistant { unwrapInstallAssistant :: YesNoAuto } deriving (Eq, Show)
newtype BashCompletions = BashCompletions { unwrapBashCompletions :: YesNoAuto } deriving (Eq, Show)
newtype ZshCompletions = ZshCompletions { unwrapZshCompletions :: YesNoAuto } deriving (Eq, Show)
