-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}

module DA.Daml.LF.Ast.Version.VersionType (
  module DA.Daml.LF.Ast.Version.VersionType
  ) where

import           Control.DeepSeq

import           Data.Data
import qualified Data.Text as T

import           GHC.Generics

import qualified DA.Daml.LF.Ast.Range as R
import           DA.Pretty

-- | Daml-LF version of an archive payload.
data Version = Version
    { versionMajor :: MajorVersion
    , versionMinor :: MinorVersion
    }
    deriving (Eq, Data, Generic, NFData, Show, Ord)

data MajorVersion = V2
  deriving (Eq, Data, Generic, NFData, Ord, Show, Enum, Bounded)

data MinorVersion =
    PointStable Int
  | PointStaging Int
  | PointDev
  deriving (Eq, Data, Generic, NFData, Ord, Show)

renderMajorVersion :: MajorVersion -> String
renderMajorVersion = \case
  V2 -> "2"

renderMinorVersion :: MinorVersion -> String
renderMinorVersion = \case
  PointStable minor -> show minor
  PointStaging minor -> show minor
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
