-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE FlexibleInstances #-}

module DA.Daml.Project.Types
    ( module DA.Daml.Project.Types
    ) where

import qualified Data.Yaml as Y
import qualified Data.Text as T
import qualified Data.SemVer as V
import qualified Control.Lens as L
import Data.Text (Text, pack, unpack)
import Data.Maybe
import System.FilePath
import Control.Monad
import Control.Exception.Safe
import Network.HTTP.Types.Header (RequestHeaders)
import Data.Either.Extra (eitherToMaybe)

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

newtype UnresolvedReleaseVersion = UnresolvedReleaseVersion
    { unwrapUnresolvedReleaseVersion :: V.Version
    } deriving (Eq, Ord, Show)

data ReleaseVersion
  = SplitReleaseVersion
      { releaseReleaseVersion :: V.Version
      , releaseSdkVersion :: V.Version
      }
  | OldReleaseVersion
      { bothVersion :: V.Version
      }
  deriving (Eq, Ord, Show)

sdkVersionFromReleaseVersion :: ReleaseVersion -> V.Version
sdkVersionFromReleaseVersion (SplitReleaseVersion _ sdkVersion) = sdkVersion
sdkVersionFromReleaseVersion (OldReleaseVersion bothVersion) = bothVersion

releaseVersionFromReleaseVersion :: ReleaseVersion -> V.Version
releaseVersionFromReleaseVersion (SplitReleaseVersion releaseVersion _) = releaseVersion
releaseVersionFromReleaseVersion (OldReleaseVersion bothVersion) = bothVersion

mkReleaseVersion :: UnresolvedReleaseVersion -> SdkVersion -> ReleaseVersion
mkReleaseVersion release sdk =
    let unwrappedRelease = unwrapUnresolvedReleaseVersion release
        unwrappedSdk = unwrapSdkVersion sdk
    in
    if unwrappedSdk == unwrappedRelease
       then OldReleaseVersion unwrappedSdk
       else SplitReleaseVersion unwrappedRelease unwrappedSdk

newtype SdkVersion = SdkVersion
    { unwrapSdkVersion :: V.Version
    } deriving (Eq, Ord, Show)

newtype DamlAssistantSdkVersion = DamlAssistantSdkVersion
    { unwrapDamlAssistantSdkVersion :: ReleaseVersion
    } deriving (Eq, Ord, Show)

instance Y.FromJSON UnresolvedReleaseVersion where
    parseJSON y = do
        verE <- V.fromText <$> Y.parseJSON y
        case verE of
            Left e -> fail ("Invalid release version: " <> e)
            Right v -> pure (UnresolvedReleaseVersion v)

instance Y.FromJSON SdkVersion where
    parseJSON y = do
        verE <- V.fromText <$> Y.parseJSON y
        case verE of
            Left e -> fail ("Invalid SDK version: " <> e)
            Right v -> pure (SdkVersion v)

versionToString :: ReleaseVersion -> String
versionToString (OldReleaseVersion bothVersion) = V.toString bothVersion
versionToString (SplitReleaseVersion releaseVersion _) = V.toString releaseVersion

versionToText :: ReleaseVersion -> Text
versionToText (OldReleaseVersion bothVersion) = V.toText bothVersion
versionToText (SplitReleaseVersion releaseVersion _) = V.toText releaseVersion

sdkVersionToText :: SdkVersion -> Text
sdkVersionToText = V.toText . unwrapSdkVersion

class IsVersion a where
    isHeadVersion :: a -> Bool

instance IsVersion ReleaseVersion where
    isHeadVersion v = "0.0.0" == versionToString v

instance IsVersion UnresolvedReleaseVersion where
    isHeadVersion v = "0.0.0" == V.toString (unwrapUnresolvedReleaseVersion v)

instance IsVersion SdkVersion where
    isHeadVersion v = "0.0.0" == T.unpack (sdkVersionToText v)

headReleaseVersion :: ReleaseVersion
headReleaseVersion =
    OldReleaseVersion $ case V.fromText "0.0.0" of
                          Left msg -> error ("headReleaseVersion: Couldn't parse '0.0.0' as a version. Error: " ++ msg)
                          Right v -> v

data InvalidVersion = InvalidVersion
    { ivSource :: !Text -- ^ invalid version
    , ivMessage :: !String -- ^ error message
    } deriving (Show, Eq)

instance Exception InvalidVersion where
    displayException (InvalidVersion bad msg) =
        "Invalid SDK version  " <> show bad <> ": " <> msg

parseVersion :: Text -> Either InvalidVersion UnresolvedReleaseVersion
parseVersion = parseUnresolvedVersion

parseUnresolvedVersion :: Text -> Either InvalidVersion UnresolvedReleaseVersion
parseUnresolvedVersion src =
    case V.fromText src of
        Left msg -> Left (InvalidVersion src msg)
        Right v -> Right (UnresolvedReleaseVersion v)

parseSdkVersion :: Text -> Either InvalidVersion SdkVersion
parseSdkVersion src =
    case V.fromText src of
        Left msg -> Left (InvalidVersion src msg)
        Right v -> Right (SdkVersion v)

releaseVersionToCacheString :: ReleaseVersion -> String
releaseVersionToCacheString (SplitReleaseVersion release sdk) = V.toString release <> " " <> V.toString sdk
releaseVersionToCacheString (OldReleaseVersion both) = V.toString both

releaseVersionFromCacheString :: String -> Maybe ReleaseVersion
releaseVersionFromCacheString src =
    let parseVersionM = eitherToMaybe . V.fromText . pack
    in
    case words src of
      [both] -> OldReleaseVersion <$> parseVersionM both
      [release, sdk] -> SplitReleaseVersion <$> parseVersionM release <*> parseVersionM sdk
      _ -> Nothing

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
defaultSdkPath :: DamlPath -> ReleaseVersion -> SdkPath
defaultSdkPath (DamlPath root) (OldReleaseVersion v) =
    SdkPath (root </> "sdk" </> V.toString (L.set V.metadata [] v))
defaultSdkPath (DamlPath root) (SplitReleaseVersion v _) =
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

-- | An install locations is a pair of fully qualified HTTP[S] URL to an SDK release tarball and headers
-- required to access that URL. For example:
-- "https://github.com/digital-asset/daml/releases/download/v0.11.1/daml-sdk-0.11.1-macos.tar.gz"
data InstallLocation = InstallLocation
    { ilUrl :: Text
    , ilHeaders :: RequestHeaders
    } deriving (Eq, Show)

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

