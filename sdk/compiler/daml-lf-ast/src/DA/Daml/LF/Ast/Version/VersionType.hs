-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}

module DA.Daml.LF.Ast.Version.VersionType (
  module DA.Daml.LF.Ast.Version.VersionType
  ) where

import           Control.DeepSeq

import qualified Data.Aeson           as Aeson
import qualified Data.Aeson.Types     as Aeson
import           Data.Data
import qualified Data.Text            as T

import           Text.Read            (readMaybe)

import           GHC.Generics

import qualified DA.Daml.LF.Ast.Range as R
import           DA.Pretty

stagingRevision :: Int
stagingRevision = 1

-- | Daml-LF version of an archive payload.
data Version = Version
    { versionMajor :: MajorVersion
    , versionMinor :: MinorVersion
    }
    deriving (Eq, Data, Generic, NFData, Show, Ord, Aeson.FromJSON, Aeson.ToJSON)

data MajorVersion = V2
  deriving (Eq, Data, Generic, NFData, Ord, Show, Enum, Bounded, Read)

-- Manual ToJSON to print MajorVersion as enum ("V2"), since with only one
-- constructor, the generic one prints it as unit type (i.e. "{}")
instance Aeson.ToJSON MajorVersion where
  toJSON = Aeson.String . T.pack . show

-- Manual FromSON to print MajorVersion as enum ("V2"), since with only one
-- constructor, the generic one prints it as unit type (i.e. "{}")
instance Aeson.FromJSON MajorVersion where
  parseJSON (Aeson.String t) =
    case readMaybe (T.unpack t) of
      Just v  -> pure v -- Success!
      Nothing -> fail $ "Unknown MajorVersion: " ++ T.unpack t

  parseJSON invalid = Aeson.typeMismatch "MajorVersion (expected a string)" invalid

data MinorVersion =
    PointStable Int
  | PointStaging Int
  | PointDev
  deriving (Eq, Data, Generic, NFData, Show, Aeson.FromJSON, Aeson.ToJSON)

-- | Explicit versinon of Ord Minorversion, without any pattern wildcards, set
-- up to break when we add a constructor. We use this instance for comparing
-- versions, for example to see if some version supports some feature with
-- associated version, or to return all stable packages that have an
-- equal-or-lower version
instance Ord MinorVersion where
    compare (PointStable x) (PointStable y)   = compare x y
    compare (PointStaging x) (PointStaging y) = compare x y
    compare PointDev         PointDev         = EQ

    compare (PointStable _) (PointStaging _)  = LT
    compare (PointStaging _) (PointStable _)  = GT

    compare (PointStable _) PointDev          = LT
    compare PointDev (PointStable _)          = GT

    compare (PointStaging _) PointDev         = LT
    compare PointDev (PointStaging _)         = GT

renderMajorVersion :: MajorVersion -> String
renderMajorVersion = \case
  V2 -> "2"

renderMinorVersion :: MinorVersion -> String
renderMinorVersion = \case
  PointStable minor -> show minor
  PointStaging minor -> show minor ++ "-staging"
  PointDev -> "dev"

renderMinorVersionWithRev :: MinorVersion -> String
renderMinorVersionWithRev m = case m of
  PointStable minor -> show minor
  PointStaging minor -> show minor ++ "-rc" ++ show stagingRevision
  PointDev -> "dev"

renderVersion :: Version -> String
renderVersion (Version major minor) =
    renderMajorVersion major <> "." <> renderMinorVersion minor

-- | A datatype describing a set of language versions. Used in the definition of
-- 'Feature' below.
type VersionReq = R.Range Version

instance Pretty Version where
  pPrint = string . renderVersion

data Feature = Feature
    { featureName :: !T.Text
    , featureVersionReq :: !VersionReq
    , featureCppFlag :: T.Text
    } deriving Show
