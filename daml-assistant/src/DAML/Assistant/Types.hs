-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Assistant.Types
    ( module DAML.Assistant.Types
    , Text, pack, unpack -- convenient re-exports
    ) where

import qualified Data.Yaml as Y
import qualified Data.Text as T
import Data.Text (Text, pack, unpack)
import Data.Maybe
import Control.Exception.Safe
import System.FilePath
import Control.Monad

data AssistantError = AssistantError
    { errContext  :: Maybe Text -- ^ Context in which error occurs.
    , errMessage  :: Maybe Text -- ^ User-friendly error message.
    , errInternal :: Maybe Text -- ^ Internal error message, i.e. what actually happened.
    } deriving (Eq, Show)

instance Exception AssistantError where
    displayException AssistantError {..} = unpack . T.unlines . catMaybes $
        [ Just ("daml: " <> fromMaybe "An unknown error has occured" errMessage)
        , fmap ("   context: " <>) errContext
        , fmap ("   reason:  " <>) errInternal
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

data CommandError = CommandError
    { cmdErrCommand :: Maybe Text
    , cmdErrMessage :: Text
    } deriving (Eq, Show)

instance Exception CommandError

data Env = Env
    { envDamlPath      :: DamlPath
    , envProjectPath   :: Maybe ProjectPath
    , envSdkPath       :: Maybe SdkPath
    , envSdkVersion    :: Maybe SdkVersion
    } deriving (Eq, Show)

newtype DamlConfig = DamlConfig
    { unwrapDamlConfig :: Y.Value
    } deriving (Eq, Show, Y.FromJSON)

newtype SdkConfig = SdkConfig
    { unwrapSdkConfig :: Y.Value
    } deriving (Eq, Show, Y.FromJSON)

newtype ProjectConfig = ProjectConfig
    { unwrapProjectConfig :: Y.Value
    } deriving (Eq, Show, Y.FromJSON)

newtype SdkVersion = SdkVersion
    { unwrapSdkVersion :: Text
    } deriving (Eq, Show, Y.FromJSON)

newtype SdkChannel = SdkChannel
    { unwrapSdkChannel :: Text
    } deriving (Eq, Show, Y.FromJSON)

newtype SdkSubVersion = SdkSubVersion
    { unwrapSdkSubVersion :: Text
    } deriving (Eq, Show, Y.FromJSON)

splitVersion :: SdkVersion -> (SdkChannel, SdkSubVersion)
splitVersion (SdkVersion v) =
    let (a,b) = T.breakOn "-" v
    in (SdkChannel a, SdkSubVersion b)

joinVersion :: (SdkChannel, SdkSubVersion) -> SdkVersion
joinVersion (SdkChannel a, SdkSubVersion "") = SdkVersion a
joinVersion (SdkChannel a, SdkSubVersion b) = SdkVersion (a <> "-" <> b)


-- | File path of daml installation root (by default ~/.daml on unix, %APPDATA%/daml on windows).
newtype DamlPath = DamlPath
    { unwrapDamlPath :: FilePath
    } deriving (Eq, Show)

-- | File path of project root.
newtype ProjectPath = ProjectPath
    { unwrapProjectPath :: FilePath
    } deriving (Eq, Show)

-- | File path of sdk root.
newtype SdkPath = SdkPath
    { unwrapSdkPath :: FilePath
    } deriving (Eq, Show)

-- | Default way of constructing sdk paths.
defaultSdkPath :: DamlPath -> SdkVersion -> SdkPath
defaultSdkPath (DamlPath root) (SdkVersion version) =
    SdkPath (root </> "sdk" </> unpack version)

-- | File path of sdk command binary, relative to sdk root.
newtype SdkCommandPath = SdkCommandPath
    { unwrapSdkCommandPath :: FilePath
    } deriving (Eq, Show)

instance Y.FromJSON SdkCommandPath where
    parseJSON value = do
        path <- Y.parseJSON value
        unless (isRelative path) $
            fail "SDK command path must be relative."
        pure $ SdkCommandPath path

-- | Absolute file path of binary to dispatch.
newtype DispatchPath = DispatchPath
    { unwrapDispatchPath :: FilePath
    } deriving (Eq, Show)

makeDispatchPath :: SdkPath -> SdkCommandPath -> DispatchPath
makeDispatchPath (SdkPath root) (SdkCommandPath path) = DispatchPath (root </> path)

data BuiltinCommand
    = Version
    | Install InstallOptions
    deriving (Eq, Show)

data Command
    = BuiltinCommand BuiltinCommand
    | SdkCommand SdkCommandInfo UserCommandArgs
    deriving (Eq, Show)

newtype UserCommandArgs = UserCommandArgs
    { unwrapUserCommandArgs :: [String]
    } deriving (Eq, Show)

newtype SdkCommandName = SdkCommandName
    { unwrapSdkCommandName :: Text
    } deriving (Eq, Show, Y.FromJSON)

newtype SdkCommandArgs = SdkCommandArgs
    { unwrapSdkCommandArgs :: [String]
    } deriving (Eq, Show, Y.FromJSON)

data SdkCommandInfo = SdkCommandInfo
    { sdkCommandName :: SdkCommandName  -- ^ name of command
    , sdkCommandPath :: SdkCommandPath -- ^ file path of binary relative to sdk directory
    , sdkCommandArgs :: SdkCommandArgs -- ^ extra args to pass before user-supplied args (defaults to [])
    , sdkCommandDesc :: Maybe Text     -- ^ description of sdk command (optional)
    } deriving (Eq, Show)

instance Y.FromJSON SdkCommandInfo where
    parseJSON = Y.withObject "SdkCommandInfo" $ \p ->
        SdkCommandInfo <$> (p Y..: "name")
                       <*> (p Y..: "path")
                       <*> fmap (fromMaybe (SdkCommandArgs [])) (p Y..:? "args")
                       <*> (p Y..:? "desc")


data InstallOptions = InstallOptions
    { iTargetM  :: Maybe InstallTarget
    , iForce    :: Bool -- ^ proceed with install even if sdk directory exists
    , iQuiet    :: Bool -- ^ be very quiet
    , iActivate :: Bool -- ^ activate the installed sdk by linking the assistant to .daml/bin
    , iInitial  :: Bool -- ^ create .daml folder structure before installing
    } deriving (Eq, Show)

data InstallTarget
    = InstallChannel SdkChannel
    | InstallVersion SdkVersion
    | InstallPath    FilePath
    deriving (Eq, Show)
