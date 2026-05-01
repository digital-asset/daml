-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0
{- HLINT ignore "Avoid restricted flags" -}
{-# LANGUAGE FlexibleInstances #-}
{-# LANGUAGE DerivingStrategies #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module DA.Daml.Version.Types
    ( module DA.Daml.Version.Types
    ) where

import qualified Data.Aeson as Aeson
import qualified Data.SemVer as V
import qualified Control.Lens as L
import Data.Text (Text)
import Control.Exception.Safe (Exception (..))
import Data.Function (on)

newtype AssemblyVersion = AssemblyVersion
  { unwrapAssemblyVersion :: V.Version
  } 
  deriving (Eq, Ord, Show)
  deriving newtype (Aeson.ToJSON, Aeson.FromJSON)

assemblyVersionToText :: AssemblyVersion -> Text
assemblyVersionToText = V.toText . unwrapAssemblyVersion

newtype ComponentVersion = ComponentVersion
  { unwrapComponentVersion :: V.Version
  }
  deriving (Eq, Ord, Show)
  deriving newtype (Aeson.ToJSON, Aeson.FromJSON)

componentVersionToText :: ComponentVersion -> Text
componentVersionToText = V.toText . unwrapComponentVersion

data VersionInfo = VersionInfo
  { -- DPM doesn't guarantee an assembly version
    viAssemblyVersion :: Maybe AssemblyVersion
  , viComponentVersion :: ComponentVersion
  } deriving (Eq, Show)

-- | Reads the assembly version of a VersionInfo. If this doesn't exist, backup to component version
extractAssemblyThenComponentVersion :: VersionInfo -> V.Version
extractAssemblyThenComponentVersion vi = maybe (unwrapComponentVersion $ viComponentVersion vi) unwrapAssemblyVersion $ viAssemblyVersion vi

instance Ord VersionInfo where
    compare = compare `on` extractAssemblyThenComponentVersion

instance Aeson.ToJSON V.Version where
  toJSON = Aeson.String . versionToText

instance Aeson.FromJSON V.Version where
  parseJSON = Aeson.withText "Version" $ either fail pure . V.fromText

class IsVersion a where
    isHeadVersion :: a -> Bool
    versionToString :: a -> String
    versionToText :: a -> Text

instance IsVersion VersionInfo where
    isHeadVersion = isHeadVersion . extractAssemblyThenComponentVersion
    versionToString = versionToString . extractAssemblyThenComponentVersion
    versionToText = versionToText . extractAssemblyThenComponentVersion

instance IsVersion AssemblyVersion where
    isHeadVersion = isHeadVersion . unwrapAssemblyVersion
    versionToString = versionToString . unwrapAssemblyVersion
    versionToText = versionToText . unwrapAssemblyVersion

instance IsVersion ComponentVersion where
    isHeadVersion = isHeadVersion . unwrapComponentVersion
    versionToString = versionToString . unwrapComponentVersion
    versionToText = versionToText . unwrapComponentVersion

instance IsVersion V.Version where
    isHeadVersion v = V.initial == L.set V.release [] (L.set V.metadata [] v)
    versionToText = V.toText
    versionToString = V.toString

data InvalidVersion = InvalidVersion
    { ivSource :: !Text -- ^ invalid version
    , ivMessage :: !String -- ^ error message
    } deriving (Show, Eq)

instance Exception InvalidVersion where
    displayException (InvalidVersion bad msg) =
        "Invalid SDK version " <> show bad <> ": " <> msg

parseVersionWrapper :: (V.Version -> a) -> Text -> Either InvalidVersion a
parseVersionWrapper wrap src =
    case V.fromText src of
        Left msg -> Left (InvalidVersion src msg)
        Right v -> Right (wrap v)

parseAssemblyVersion :: Text -> Either InvalidVersion AssemblyVersion
parseAssemblyVersion = parseVersionWrapper AssemblyVersion

parseComponentVersion :: Text -> Either InvalidVersion ComponentVersion
parseComponentVersion = parseVersionWrapper ComponentVersion
