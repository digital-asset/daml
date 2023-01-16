-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
module DA.Daml.LF.Ast.Version(module DA.Daml.LF.Ast.Version) where

import           Data.Data
import GHC.Generics
import           DA.Pretty
import           Control.DeepSeq
import qualified Data.Map.Strict as MS
import qualified Data.Text as T
import qualified Text.Read as Read

-- | Daml-LF version of an archive payload.
data Version
  = V1{versionMinor :: MinorVersion}
  deriving (Eq, Data, Generic, NFData, Ord, Show)

data MinorVersion = PointStable Int | PointDev
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | Daml-LF version 1.6
version1_6 :: Version
version1_6 = V1 $ PointStable 6

-- | Daml-LF version 1.7
version1_7 :: Version
version1_7 = V1 $ PointStable 7

-- | Daml-LF version 1.8
version1_8 :: Version
version1_8 = V1 $ PointStable 8

-- | Daml-LF version 1.11
version1_11 :: Version
version1_11 = V1 $ PointStable 11

-- | Daml-LF version 1.12
version1_12 :: Version
version1_12 = V1 $ PointStable 12

-- | Daml-LF version 1.13
version1_13 :: Version
version1_13 = V1 $ PointStable 13

-- | Daml-LF version 1.14
version1_14 :: Version
version1_14 = V1 $ PointStable 14

-- | Daml-LF version 1.15
version1_15 :: Version
version1_15 = V1 $ PointStable 15

-- | The Daml-LF version used by default.
versionDefault :: Version
versionDefault = version1_14

-- | The Daml-LF development version.
versionDev :: Version
versionDev = V1 PointDev

supportedOutputVersions :: [Version]
supportedOutputVersions = [version1_14, version1_15, versionDev]

supportedInputVersions :: [Version]
supportedInputVersions = [version1_8, version1_11, version1_12, version1_13] ++ supportedOutputVersions

data Feature = Feature
    { featureName :: !T.Text
    , featureMinVersion :: !Version
    , featureCppFlag :: Maybe T.Text
        -- ^ CPP flag to test for availability of the feature.
    } deriving Show

-- | Kept for serialization of stable packages.
featureStringInterning :: Feature
featureStringInterning = Feature
    { featureName = "String interning"
    , featureMinVersion = version1_7
    , featureCppFlag = Nothing
    }

-- | Kept for serialization of stable packages.
featureTypeInterning :: Feature
featureTypeInterning = Feature
    { featureName = "Type interning"
    , featureMinVersion = version1_11
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

featureWithAuthority :: Feature
featureWithAuthority = Feature
    { featureName = "withAuthorityOf primitive"
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_WITH_AUTHORITY"
    }

featureBigNumeric :: Feature
featureBigNumeric = Feature
    { featureName = "BigNumeric type"
    , featureMinVersion = version1_13
    , featureCppFlag = Just "DAML_BIGNUMERIC"
    }

featureExceptions :: Feature
featureExceptions = Feature
    { featureName = "Daml Exceptions"
    , featureMinVersion = version1_14
    , featureCppFlag = Just "DAML_EXCEPTIONS"
    }

featureNatSynonyms :: Feature
featureNatSynonyms = Feature
    { featureName = "Nat type synonyms"
    , featureMinVersion = version1_14
    , featureCppFlag = Just "DAML_NAT_SYN"
    }

featureSimpleInterfaces :: Feature
featureSimpleInterfaces = Feature
    { featureName = "Daml Interfaces"
    , featureMinVersion = version1_15
    , featureCppFlag = Just "DAML_INTERFACE"
    }

featureExtendedInterfaces :: Feature
featureExtendedInterfaces = Feature
    { featureName = "Guards in interfaces"
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_INTERFACE_EXTENDED"
    }

featureExperimental :: Feature
featureExperimental = Feature
    { featureName = "Daml Experimental"
    , featureMinVersion = versionDev
    , featureCppFlag = Just "DAML_EXPERIMENTAL"
    }

allFeatures :: [Feature]
allFeatures =
    [ featureStringInterning
    , featureTypeInterning
    , featureBigNumeric
    , featureExceptions
    , featureNatSynonyms
    , featureSimpleInterfaces
    , featureExtendedInterfaces
    , featureUnstable
    , featureExperimental
    , featureWithAuthority
    ]

featureVersionMap :: MS.Map T.Text Version
featureVersionMap = MS.fromList
    [ (key, version)
    | feature <- allFeatures
    , let version = featureMinVersion feature
    , Just key <- [featureCppFlag feature]
    ]

-- | Return minimum version associated with a feature flag.
versionForFeature :: T.Text -> Maybe Version
versionForFeature key = MS.lookup key featureVersionMap

-- | Same as 'versionForFeature' but errors out if the feature doesn't exist.
versionForFeaturePartial :: T.Text -> Version
versionForFeaturePartial key =
    case versionForFeature key of
        Just version -> version
        Nothing ->
            error . T.unpack . T.concat $
                [ "Unknown feature: "
                , key
                , ". Available features are: "
                , T.intercalate ", " (MS.keys featureVersionMap)
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
