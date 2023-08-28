-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE DerivingStrategies #-}

module DA.Daml.LF.Ast.Version (
    Version,

    version1_6,
    version1_7,
    version1_8,
    version1_11,
    version1_12,
    version1_13,
    version1_14,
    version1_15,
    versionDefault,
    versionDev,

    supportedOutputVersions,
    supportedInputVersions,

    renderVersion,
    parseVersion,
    v1MinorVersion,

    Feature (..),
    FeatureVersionReq (..),

    featureStringInterning,
    featureTypeInterning,
    featureUnstable,
    featureBigNumeric,
    featureExceptions,
    featureNatSynonyms,
    featureSimpleInterfaces,
    featureExtendedInterfaces,
    featureChoiceFuncs,
    featureTemplateTypeRepToText,
    featureDynamicExercise,
    featurePackageUpgrades,
    featureNatTypeErasure,
    featureExperimental,

    supports,

    allFeaturesForVersion,
    versionForFeaturePartial,
) where

import           Data.Data
import GHC.Generics
import           DA.Pretty
import           Control.DeepSeq
import qualified Data.Map.Strict as MS
import qualified Data.Text as T

data InternalVersion
    = V1_0
    | V1_6
    | V1_7
    | V1_8
    | V1_11
    | V1_12
    | V1_13
    | V1_14
    | V1_15
    | V1_dev
    deriving anyclass (NFData)
    deriving stock (Eq, Ord, Data, Show, Generic)

-- | Daml-LF version of an archive payload.
newtype Version
    = MkVersion InternalVersion
    deriving anyclass (NFData)
    deriving stock (Eq, Data, Generic)
    -- no 'Ord'!
    deriving newtype (Show)
    -- 'show' without newtype wrapper

-- | Daml-LF version 1.6
version1_6 :: Version
version1_6 = MkVersion V1_6

-- | Daml-LF version 1.7
version1_7 :: Version
version1_7 = MkVersion V1_7

-- | Daml-LF version 1.8
version1_8 :: Version
version1_8 = MkVersion V1_8

-- | Daml-LF version 1.11
version1_11 :: Version
version1_11 = MkVersion V1_11

-- | Daml-LF version 1.12
version1_12 :: Version
version1_12 = MkVersion V1_12

-- | Daml-LF version 1.13
version1_13 :: Version
version1_13 = MkVersion V1_13

-- | Daml-LF version 1.14
version1_14 :: Version
version1_14 = MkVersion V1_14

-- | Daml-LF version 1.15
version1_15 :: Version
version1_15 = MkVersion V1_15

-- | The Daml-LF version used by default.
versionDefault :: Version
versionDefault = version1_15

-- | The Daml-LF development version.
versionDev :: Version
versionDev = MkVersion V1_dev

supportedOutputVersions :: [Version]
supportedOutputVersions = [version1_14, version1_15, versionDev]

supportedInputVersions :: [Version]
supportedInputVersions = [version1_8, version1_11, version1_12, version1_13] ++ supportedOutputVersions

data FeatureVersionReq
    = FromVersion !Version
    deriving (Show)

data Feature = Feature
    { featureName :: !T.Text
    , featureVersionReq :: !FeatureVersionReq
    , featureCppFlag :: Maybe T.Text
        -- ^ CPP flag to test for availability of the feature.
    } deriving Show

-- | Kept for serialization of stable packages.
featureStringInterning :: Feature
featureStringInterning = Feature
    { featureName = "String interning"
    , featureVersionReq = FromVersion version1_7
    , featureCppFlag = Nothing
    }

-- | Kept for serialization of stable packages.
featureTypeInterning :: Feature
featureTypeInterning = Feature
    { featureName = "Type interning"
    , featureVersionReq = FromVersion version1_11
    , featureCppFlag = Nothing
    }

-- Unstable, experimental features. This should stay in 1.dev forever.
-- Features implemented with this flag should be moved to a separate
-- feature flag once the decision to add them permanently has been made.
featureUnstable :: Feature
featureUnstable = Feature
    { featureName = "Unstable, experimental features"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_UNSTABLE"
    }

featureBigNumeric :: Feature
featureBigNumeric = Feature
    { featureName = "BigNumeric type"
    , featureVersionReq = FromVersion version1_13
    , featureCppFlag = Just "DAML_BIGNUMERIC"
    }

featureExceptions :: Feature
featureExceptions = Feature
    { featureName = "Daml Exceptions"
    , featureVersionReq = FromVersion version1_14
    , featureCppFlag = Just "DAML_EXCEPTIONS"
    }

featureNatSynonyms :: Feature
featureNatSynonyms = Feature
    { featureName = "Nat type synonyms"
    , featureVersionReq = FromVersion version1_14
    , featureCppFlag = Just "DAML_NAT_SYN"
    }

featureSimpleInterfaces :: Feature
featureSimpleInterfaces = Feature
    { featureName = "Daml Interfaces"
    , featureVersionReq = FromVersion version1_15
    , featureCppFlag = Just "DAML_INTERFACE"
    }

featureExtendedInterfaces :: Feature
featureExtendedInterfaces = Feature
    { featureName = "Guards in interfaces"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_INTERFACE_EXTENDED"
    }

featureChoiceFuncs :: Feature
featureChoiceFuncs = Feature
    { featureName = "choiceController and choiceObserver functions"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_CHOICE_FUNCS"
    }

featureTemplateTypeRepToText :: Feature
featureTemplateTypeRepToText = Feature
    { featureName = "templateTypeRepToText function"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_TEMPLATE_TYPEREP_TO_TEXT"
    }

featureDynamicExercise :: Feature
featureDynamicExercise = Feature
    { featureName = "dynamicExercise function"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_DYNAMIC_EXERCISE"
    }

featurePackageUpgrades :: Feature
featurePackageUpgrades = Feature
    { featureName = "Package upgrades POC"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_PACKAGE_UPGRADES"
    }

featureNatTypeErasure :: Feature
featureNatTypeErasure = Feature
    { featureName = "Erasing types of kind Nat"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_NAT_TYPE_ERASURE"
    }

featureExperimental :: Feature
featureExperimental = Feature
    { featureName = "Daml Experimental"
    , featureVersionReq = FromVersion versionDev
    , featureCppFlag = Just "DAML_EXPERIMENTAL"
    }

-- TODO: https://github.com/digital-asset/daml/issues/15882
-- Ought we have "featureChoiceAuthority" ?

allFeatures :: [Feature]
allFeatures =
    [ featureStringInterning
    , featureTypeInterning
    , featureBigNumeric
    , featureExceptions
    , featureNatSynonyms
    , featureSimpleInterfaces
    , featureExtendedInterfaces
    , featureChoiceFuncs
    , featureTemplateTypeRepToText
    , featureUnstable
    , featureExperimental
    , featureDynamicExercise
    , featurePackageUpgrades
    , featureNatTypeErasure
    ]

featureVersionMap :: MS.Map T.Text Version
featureVersionMap = MS.fromList
    [ (key, version)
    | feature <- allFeatures
    , version <- case featureVersionReq feature of
        FromVersion v -> [v]
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
supports (MkVersion v) feature =
    case featureVersionReq feature of
        FromVersion (MkVersion from) -> v >= from

-- | Returns 'Just minor' if the major version == 1, otherwise 'Nothing'
v1MinorVersion :: Version -> Maybe String
v1MinorVersion (MkVersion v) = case v of
    V1_0 -> Just "0"
    V1_6 -> Just "6"
    V1_7 -> Just "7"
    V1_8 -> Just "8"
    V1_11 -> Just "11"
    V1_12 -> Just "12"
    V1_13 -> Just "13"
    V1_14 -> Just "14"
    V1_15 -> Just "15"
    V1_dev -> Just "dev"

renderVersion :: Version -> String
renderVersion (MkVersion v) = case v of
    V1_0 -> "1.0"
    V1_6 -> "1.6"
    V1_7 -> "1.7"
    V1_8 -> "1.8"
    V1_11 -> "1.11"
    V1_12 -> "1.12"
    V1_13 -> "1.13"
    V1_14 -> "1.14"
    V1_15 -> "1.15"
    V1_dev -> "1.dev"

parseVersion :: String -> Maybe Version
parseVersion = fmap MkVersion . \case
    "1.0" -> pure V1_0
    "1.6" -> pure V1_6
    "1.7" -> pure V1_7
    "1.8" -> pure V1_8
    "1.11" -> pure V1_11
    "1.12" -> pure V1_12
    "1.13" -> pure V1_13
    "1.14" -> pure V1_14
    "1.15" -> pure V1_15
    "1.dev" -> pure V1_dev
    _ -> Nothing

instance Pretty Version where
  pPrint = string . renderVersion
