-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass  #-}

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
    -- Suffixed with P for Patch, non-suffixed Pattern created in
    -- generate_haskell_versions.py
    PointStableP  { minorInt :: Int
                  , patch    :: Int
                  }
    -- Suffixed with P for Patch, non-suffixed Pattern created in
    -- generate_haskell_versions.py
  | PointStagingP { minorInt :: Int
                  , patch    :: Int
                  }
  | PointDev
  deriving (Eq, Data, Generic, NFData, Show, Aeson.FromJSON, Aeson.ToJSON)

-- | Explicit versinon of Ord Minorversion, without any pattern wildcards, set
-- up to break when we add a constructor. We use this instance for comparing
-- versions, for example to see if some version supports some feature with
-- associated version, or to return all stable packages that have an
-- equal-or-lower version. Note that we ignore patch versions here, since unless
-- we specifically look at patch versions, these are intened to be oppaque
instance Ord MinorVersion where
    compare (PointStableP  x _) (PointStableP y _)  = compare x y
    compare (PointStagingP x _) (PointStagingP y _) = compare x y
    compare PointDev            PointDev            = EQ

    compare (PointStableP  _ _) (PointStagingP _ _) = LT
    compare (PointStagingP _ _) (PointStableP  _ _) = GT

    compare (PointStableP _ _) PointDev           = LT
    compare PointDev           (PointStableP _ _) = GT

    compare (PointStagingP _ _) PointDev            = LT
    compare PointDev            (PointStagingP _ _) = GT

renderMajorVersion :: MajorVersion -> String
renderMajorVersion = \case
  V2 -> "2"

renderMinorVersion :: MinorVersion -> String
renderMinorVersion = \case
  PointStableP  minor _ -> show minor
  PointStagingP minor _ -> show minor
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
