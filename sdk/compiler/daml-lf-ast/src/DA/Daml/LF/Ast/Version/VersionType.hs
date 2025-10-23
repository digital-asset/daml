-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}

module DA.Daml.LF.Ast.Version.VersionType(module DA.Daml.LF.Ast.Version.VersionType) where

import           Control.DeepSeq

import           Data.Data
import           Data.List (intercalate)
import           Data.Maybe (catMaybes)

import           GHC.Generics

import qualified DA.Daml.LF.Ast.Range as R
import           DA.Pretty

-- | Daml-LF version of an archive payload.
data Version = Version
    { versionMajor :: MajorVersion
    , versionMinor :: MinorVersion
    }
    deriving (Eq, Data, Generic, NFData, Show)

data MajorVersion = V2
  deriving (Eq, Data, Generic, NFData, Ord, Show, Enum, Bounded)

data MinorVersion =
    PointStable Int
  | PointStaging Int
  | PointDev
  deriving (Eq, Data, Generic, NFData, Show)

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
  PointStaging minor -> show minor
  PointDev -> "dev"

renderVersion :: Version -> String
renderVersion (Version major minor) =
    renderMajorVersion major <> "." <> renderMinorVersion minor

-- | A datatype describing a set of language versions. Used in the definition of
-- 'Feature' below.
newtype VersionReq = VersionReq {unVersionReq :: MajorVersion -> R.Range MinorVersion}

-- >>> show (VersionReq (\V2 -> allMinorVersions))
-- Variable not in scope: allV2MinorVersions :: Range MinorVersion
instance Show VersionReq where
    show (VersionReq req) =
        concat
            [ "VersionReq (\\case V2 -> "
            , show (req V2)
            , ")"
            ]

{-|
Renders a FeatureVersionReq.

>>> let r1 = R.Inclusive (PointStable 1) (PointStable 2)
>>> let r2 = R.Inclusive (PointStable 3) PointDev
>>> renderFeatureVersionReq (VersionReq (\V2 ->  R.Empty))
"none"
>>> renderFeatureVersionReq (VersionReq (\V2 -> R.Empty))
"none"
>>> renderFeatureVersionReq (VersionReq (\V2 -> r2))
"2.3 to 2.dev"
>>> renderFeatureVersionReq (VersionReq (\V2 -> r2))
"2.3 to 2.dev"
-}
renderFeatureVersionReq :: VersionReq -> String
renderFeatureVersionReq (VersionReq req) = renderRanges (req V2)
  where
    renderRanges R.Empty = "none"
    renderRanges v2Range =
      intercalate ", or " $
        catMaybes [ renderRange (Version V2) v2Range ]

    renderRange cons = \case
        R.Empty -> Nothing
        R.Inclusive low high
          | low == high -> Just $ renderVersion (cons low)
          | otherwise ->
              Just $
                unwords
                    [ renderVersion (cons low)
                    , "to"
                    , renderVersion (cons high)
                    ]

instance Pretty Version where
  pPrint = string . renderVersion

instance Pretty VersionReq where
  pPrint = string . renderFeatureVersionReq
