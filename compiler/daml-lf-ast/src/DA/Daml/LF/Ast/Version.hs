-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
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

-- | DAML-LF version 1.6
version1_6 :: Version
version1_6 = V1 $ PointStable 6

-- | DAML-LF version 1.7
version1_7 :: Version
version1_7 = V1 $ PointStable 7

-- | DAML-LF version 1.8
version1_8 :: Version
version1_8 = V1 $ PointStable 8

-- | The DAML-LF version used by default.
versionDefault :: Version
versionDefault = version1_8

-- | The DAML-LF development version.
versionDev :: Version
versionDev = V1 PointDev

supportedOutputVersions :: [Version]
supportedOutputVersions = [version1_6, version1_7, version1_8, versionDev]

supportedInputVersions :: [Version]
supportedInputVersions = supportedOutputVersions


data Feature = Feature
    { featureName :: !T.Text
    , featureMinVersion :: !Version
    , featureCppFlag :: Maybe T.Text
        -- ^ CPP flag to test for availability of the feature.
    } deriving Show

featureNumeric :: Feature
featureNumeric = Feature
    { featureName = "Numeric type"
    , featureMinVersion = version1_7
    , featureCppFlag = Just "DAML_NUMERIC"
    }

featureAnyType :: Feature
featureAnyType = Feature
   { featureName = "Any type"
   , featureMinVersion = version1_7
   , featureCppFlag = Just "DAML_ANY_TYPE"
   }

featureTypeRep :: Feature
featureTypeRep = Feature
    { featureName = "TypeRep type"
    , featureMinVersion = version1_7
    , featureCppFlag = Just "DAML_TYPE_REP"
    }

featureStringInterning :: Feature
featureStringInterning = Feature
    { featureName = "String interning"
    , featureMinVersion = version1_7
    , featureCppFlag = Nothing
    }

featureGenericComparison :: Feature
featureGenericComparison = Feature
    { featureName = "Generic order relation"
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_GENERIC_COMPARISON"
    }

featureGenMap :: Feature
featureGenMap = Feature
    { featureName = "Generic map"
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_GENMAP"
    }

featureTypeSynonyms :: Feature
featureTypeSynonyms = Feature
    { featureName = "LF type synonyms"
    , featureMinVersion = version1_8
    , featureCppFlag = Nothing
    }

featurePackageMetadata :: Feature
featurePackageMetadata = Feature
    { featureName = "Package metadata"
    , featureMinVersion = version1_8
    , featureCppFlag = Nothing
    }

-- Unstable, experimental features. This should stay in 1.dev forever.
-- Features implemented with this flag should be moved to a separate
-- feature flag once the decision to add them permanently has been made.
featureUnstable :: Feature
featureUnstable = Feature
    { featureName = "Unstable, experimental features"
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_UNSTABLE"
    }

featureToTextContractId :: Feature
featureToTextContractId = Feature
    { featureName = "TO_TEXT_CONTRACT_ID primitive"
    -- TODO Change as part of #7139
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_TO_TEXT_CONTRACT_ID"
    }

featureChoiceObservers :: Feature  -- issue #7709
featureChoiceObservers = Feature
    { featureName = "Choice observers"
    -- TODO Change as part of #7139
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_CHOICE_OBSERVERS"
    }

featureTypeInterning :: Feature
featureTypeInterning = Feature
    { featureName = "Type interning"
    -- TODO Change as part of #7139
    , featureMinVersion = versionDev
    , featureCppFlag = Nothing
    }

featureExceptions :: Feature
featureExceptions = Feature
    { featureName = "DAML Exceptions"
    , featureMinVersion = versionDev
        -- TODO (#8020): Update LF version number when we stabilize exceptions.
        -- https://github.com/digital-asset/daml/issues/8020
        -- https://github.com/digital-asset/daml/issues/7139
    , featureCppFlag = Just "DAML_EXCEPTIONS"
    }

allFeatures :: [Feature]
allFeatures =
    [ featureNumeric
    , featureAnyType
    , featureTypeRep
    , featureTypeSynonyms
    , featureStringInterning
    , featureGenericComparison
    , featureGenMap
    , featurePackageMetadata
    , featureUnstable
    , featureToTextContractId
    , featureChoiceObservers
    , featureTypeInterning
    , featureExceptions
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
