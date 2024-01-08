-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{-# LANGUAGE FlexibleInstances #-}

module DA.Daml.Version.Types
    ( module DA.Daml.Version.Types
    ) where

import qualified Data.Yaml as Y
import qualified Data.SemVer as V
import qualified Control.Lens as L
import Data.Text (Text, pack)
import Control.Exception.Safe (Exception (..))
import Data.Either.Extra (eitherToMaybe)
import Data.Function (on)
import qualified SdkVersion.Class
import qualified Control.Exception as Unsafe

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
  deriving (Eq, Show)

instance Ord ReleaseVersion where
    compare = compare `on` releaseVersionFromReleaseVersion

sdkVersionFromReleaseVersion :: ReleaseVersion -> SdkVersion
sdkVersionFromReleaseVersion (SplitReleaseVersion _ sdkVersion) = SdkVersion sdkVersion
sdkVersionFromReleaseVersion (OldReleaseVersion bothVersion) = SdkVersion bothVersion

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

rawVersionToTextWithV :: V.Version -> Text
rawVersionToTextWithV v = "v" <> V.toText v

sdkVersionToText :: SdkVersion -> Text
sdkVersionToText = V.toText . unwrapSdkVersion

unresolvedReleaseVersionToString :: UnresolvedReleaseVersion -> String
unresolvedReleaseVersionToString = V.toString . unwrapUnresolvedReleaseVersion

class IsVersion a where
    isHeadVersion :: a -> Bool

instance IsVersion ReleaseVersion where
    isHeadVersion v = isHeadVersion (releaseVersionFromReleaseVersion v)

instance IsVersion UnresolvedReleaseVersion where
    isHeadVersion v = isHeadVersion (unwrapUnresolvedReleaseVersion v)

instance IsVersion SdkVersion where
    isHeadVersion v = isHeadVersion (unwrapSdkVersion v)

instance IsVersion V.Version where
    isHeadVersion v = V.initial == L.set V.release [] (L.set V.metadata [] v)

headReleaseVersion :: ReleaseVersion
headReleaseVersion = OldReleaseVersion V.initial

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

-- This is unsafe because it converts a version straight into an
-- OldReleaseVersion without checking that release and sdk version are actually
-- the same for this release.
unsafeParseOldReleaseVersion :: Text -> Either InvalidVersion ReleaseVersion
unsafeParseOldReleaseVersion src = do
    case V.fromText src of
        Left msg -> Left (InvalidVersion src msg)
        Right v -> Right (OldReleaseVersion v)

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

unresolvedBuiltinSdkVersion :: SdkVersion.Class.SdkVersioned => UnresolvedReleaseVersion
unresolvedBuiltinSdkVersion = either Unsafe.throw id $ parseUnresolvedVersion (pack SdkVersion.Class.sdkVersion)

unsafeResolveReleaseVersion :: UnresolvedReleaseVersion -> ReleaseVersion
unsafeResolveReleaseVersion (UnresolvedReleaseVersion v) = OldReleaseVersion v
