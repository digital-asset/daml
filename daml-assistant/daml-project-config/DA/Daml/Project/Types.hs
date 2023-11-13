-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE FlexibleInstances #-}

module DA.Daml.Project.Types
    ( module DA.Daml.Project.Types
    ) where

import Control.Exception.Safe
import Control.Lens qualified as L
import Control.Monad
import Data.Maybe
import Data.SemVer qualified as V
import Data.Text (Text)
import Data.Text qualified as T
import Data.Yaml qualified as Y
import System.FilePath

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

newtype MultiPackageConfig = MultiPackageConfig
    { unwrapMultiPackageConfig :: Y.Value
    } deriving (Eq, Show, Y.FromJSON)

newtype SdkVersion = SdkVersion
    { unwrapSdkVersion :: V.Version
    } deriving (Eq, Ord, Show)

newtype DamlAssistantSdkVersion = DamlAssistantSdkVersion
    { unwrapDamlAssistantSdkVersion :: SdkVersion
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

isHeadVersion :: SdkVersion -> Bool
isHeadVersion v = "0.0.0" == versionToString v

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

-- | File path of daml installation root (by default ~/.daml on unix, %APPDATA%/daml on windows).
newtype DamlPath = DamlPath
    { unwrapDamlPath :: FilePath
    } deriving (Eq, Show)

-- | File path to a cache directory, e.g. ~/.cache.
newtype CachePath = CachePath
    { unwrapCachePath :: FilePath
    } deriving (Eq, Show)

-- | Absolute file path to the assistant executable, e.g., /home/foobar/.daml/bin/daml.
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
    , sdkCommandForwardCompletion :: ForwardCompletion -- ^ Can we forward optparse-applicative completions to
                                                       -- this command
    , sdkCommandSdkPath :: SdkPath -- ^ SDK path so we can get the absolute path to the command.
    } deriving (Eq, Show)

data ForwardCompletion
    = Forward EnrichedCompletion -- ^ Forward completions
    | NoForward -- ^ No forwarding, fall back to basic completion
    deriving (Eq, Show)

-- | True if --bash-completion-enriched was part of argv.
newtype EnrichedCompletion = EnrichedCompletion { getEnrichedCompletion :: Bool }
    deriving (Eq, Show)

hasEnrichedCompletion :: [String] -> EnrichedCompletion
hasEnrichedCompletion = EnrichedCompletion . elem "--bash-completion-enriched"

instance Y.FromJSON (SdkPath -> EnrichedCompletion -> SdkCommandInfo) where
    parseJSON = Y.withObject "SdkCommandInfo" $ \p -> do
        name <- p Y..: "name"
        path <- p Y..: "path"
        args <- fmap (fromMaybe (SdkCommandArgs [])) (p Y..:? "args")
        desc <- p Y..:? "desc"
        completion <- fromMaybe False <$> p Y..:? "completion"
        return $ \sdkPath enriched -> SdkCommandInfo
          name
          path
          args
          desc
          (if completion then Forward enriched else NoForward)
          sdkPath
