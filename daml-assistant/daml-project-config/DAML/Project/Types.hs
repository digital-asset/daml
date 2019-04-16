-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}

module DAML.Project.Types
    ( module DAML.Project.Types
    ) where

import qualified Data.Yaml as Y
import qualified Data.Text as T
import qualified Data.SemVer as V
import qualified Control.Lens as L
import Data.Text (Text)
import Data.Maybe
import System.FilePath
import Control.Monad
import Control.Exception.Safe

data ConfigError
    = ConfigFileInvalid Text Y.ParseException
    | ConfigFieldInvalid Text [Text] String
    | ConfigFieldMissing Text [Text]
    deriving (Show)

instance Exception ConfigError where
    displayException (ConfigFileInvalid name err) =
        concat ["Invalid ", T.unpack name, " config file:", displayException err]
    displayException (ConfigFieldInvalid name path msg) =
        concat ["Invalid ", T.unpack (T.intercalate "." path)
            , " field in ", T.unpack name, " config: ", msg]
    displayException (ConfigFieldMissing name path) =
        concat ["Missing required ", T.unpack (T.intercalate "." path)
            , " field in ", T.unpack name, " config."]

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
    { unwrapSdkVersion :: V.Version
    } deriving (Eq, Ord, Show)

instance Y.FromJSON SdkVersion where
    parseJSON y = do
        verE <- V.fromText <$> Y.parseJSON y
        case verE of
            Left e -> fail ("Invalid SDK version: " <> e)
            Right v -> pure (SdkVersion v)

versionToString :: SdkVersion -> String
versionToString = V.toString . unwrapSdkVersion

versionToText :: SdkVersion -> Text
versionToText = V.toText . unwrapSdkVersion

data InvalidVersion = InvalidVersion
    { ivSource :: !Text -- ^ invalid version
    , ivMessage :: !String -- ^ error message
    } deriving (Show, Eq)

instance Exception InvalidVersion where
    displayException (InvalidVersion bad msg) =
        "Invalid SDK version  " <> show bad <> ": " <> msg

parseVersion :: Text -> Either InvalidVersion SdkVersion
parseVersion src =
    case V.fromText src of
        Left msg -> Left (InvalidVersion src msg)
        Right v -> Right (SdkVersion v)

isStableVersion :: SdkVersion -> Bool
isStableVersion = null . L.view V.release . unwrapSdkVersion

-- | File path of daml installation root (by default ~/.daml on unix, %APPDATA%/daml on windows).
newtype DamlPath = DamlPath
    { unwrapDamlPath :: FilePath
    } deriving (Eq, Show)

-- | Absolute file path to the assistant.
newtype DamlAssistantPath = DamlAssistantPath
    { unwrapDamlAssistantPath :: FilePath
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
defaultSdkPath (DamlPath root) (SdkVersion v) =
    SdkPath (root </> "sdk" </> V.toString (L.set V.metadata [] v))

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
        SdkCommandInfo
            <$> (p Y..: "name")
            <*> (p Y..: "path")
            <*> fmap (fromMaybe (SdkCommandArgs [])) (p Y..:? "args")
            <*> (p Y..:? "desc")
