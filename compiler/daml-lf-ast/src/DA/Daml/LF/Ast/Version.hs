-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
module DA.Daml.LF.Ast.Version(module DA.Daml.LF.Ast.Version) where

import           Data.Data
import GHC.Generics
import           DA.Pretty
import           Control.DeepSeq
import qualified Data.Text as T
import qualified Text.Read as Read

-- | DAML-LF version of an archive payload.
data Version
  = V1{versionMinor :: MinorVersion}
  deriving (Eq, Data, Generic, NFData, Ord, Show)

data MinorVersion = PointStable Int | PointDev
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | DAML-LF version 1.5
version1_5 :: Version
version1_5 = V1 $ PointStable 5

-- | DAML-LF version 1.6
version1_6 :: Version
version1_6 = V1 $ PointStable 6

-- | DAML-LF version 1.7
version1_7 :: Version
version1_7 = V1 $ PointStable 7

-- | The DAML-LF version used by default.
versionDefault :: Version
versionDefault = version1_7

-- | The DAML-LF development version.
versionDev :: Version
versionDev = V1 PointDev

supportedOutputVersions :: [Version]
supportedOutputVersions = [version1_6, version1_7, versionDev]

supportedInputVersions :: [Version]
supportedInputVersions = version1_5 : supportedOutputVersions


data Feature = Feature
    { featureName :: !T.Text
    , featureMinVersion :: !Version
    , featureCppFlag :: !T.Text
        -- ^ CPP flag to test for availability of the feature.
    }

featureNumeric :: Feature
featureNumeric = Feature
    { featureName = "Numeric type"
    , featureMinVersion = version1_7
    , featureCppFlag = "DAML_NUMERIC"
    }

featureAnyType :: Feature
featureAnyType = Feature
   { featureName = "Any type"
   , featureMinVersion = version1_7
   , featureCppFlag = "DAML_ANY_TYPE"
   }

featureTypeRep :: Feature
featureTypeRep = Feature
    { featureName = "TypeRep type"
    , featureMinVersion = version1_7
    , featureCppFlag = "DAML_TYPE_REP"
    }

featureStringInterning :: Feature
featureStringInterning = Feature
    { featureName = "String interning"
    , featureMinVersion = version1_7
    , featureCppFlag = "DAML_STRING_INTERNING"
    }

featureGenMap :: Feature
featureGenMap = Feature
    { featureName = "Generic map"
    , featureMinVersion = versionDev
    , featureCppFlag = "DAML_GENMAP"
    }

-- Unstable, experimental features. This should stay in 1.dev forever.
-- Features implemented with this flag should be moved to a separate
-- feature flag once the decision to add them permanently has been made.
featureUnstable :: Feature
featureUnstable = Feature
    { featureName = "Unstable, experimental features"
    , featureMinVersion = versionDev
    , featureCppFlag = "DAML_UNSTABLE"
    }

allFeatures :: [Feature]
allFeatures =
    [ featureNumeric
    , featureAnyType
    , featureTypeRep
    , featureStringInterning
    , featureGenMap
    , featureUnstable
    ]

allFeaturesForVersion :: Version -> [Feature]
allFeaturesForVersion version = filter (supports version) allFeatures

supports :: Version -> Feature -> Bool
supports version feature = version >= featureMinVersion feature

renderMinorVersion :: MinorVersion -> String
renderMinorVersion = \case
  PointStable minor -> show minor
  PointDev -> "dev"

parseMinorVersion :: String -> Maybe MinorVersion
parseMinorVersion = \case
  (Read.readMaybe -> Just i) -> Just $ PointStable i
  "dev" -> Just PointDev
  _ -> Nothing

renderVersion :: Version -> String
renderVersion = \case
    V1 minor -> "1." ++ renderMinorVersion minor

parseVersion :: String -> Maybe Version
parseVersion = \case
    '1':'.':minor -> V1 <$> parseMinorVersion minor
    _ -> Nothing

instance Pretty Version where
  pPrint = string . renderVersion
