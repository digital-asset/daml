-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE BlockArguments #-}
{-# LANGUAGE DeriveAnyClass #-}

module DA.Daml.LF.Ast.Version(module DA.Daml.LF.Ast.Version) where

import           Data.Char (isDigit)
import           Data.Data
import           Data.List (intercalate)
import           Data.Maybe (catMaybes)
import           GHC.Generics
import           DA.Pretty
import qualified DA.Daml.LF.Ast.Range as R
import           Control.DeepSeq
import qualified Data.Text as T
import           Safe (headMay)
import           Text.ParserCombinators.ReadP (ReadP, pfail, readP_to_S, (+++), munch1)
import qualified Text.ParserCombinators.ReadP as ReadP
import qualified Data.Map.Strict as MS

-- | Daml-LF version of an archive payload.
data Version = Version
    { versionMajor :: MajorVersion
    , versionMinor :: MinorVersion
    }
    deriving (Eq, Data, Generic, NFData, Show)

data MajorVersion = V1 | V2
  deriving (Eq, Data, Generic, NFData, Ord, Show, Enum, Bounded)

data MinorVersion = PointStable Int | PointDev
  deriving (Eq, Data, Generic, NFData, Ord, Show)

-- | x `canDependOn` y if dars compiled to version x can depend on dars compiled
-- to version y.
canDependOn :: Version -> Version -> Bool
canDependOn (Version major1 minor1) (Version major2 minor2) =
  major1 == major2 && minor1 >= minor2

-- | Daml-LF version 1.6
version1_6 :: Version
version1_6 = Version V1 (PointStable 6)

-- | Daml-LF version 1.7
version1_7 :: Version
version1_7 = Version V1 (PointStable 7)

-- | Daml-LF version 1.8
version1_8 :: Version
version1_8 = Version V1 (PointStable 8)

-- | Daml-LF version 1.11
version1_11 :: Version
version1_11 = Version V1 (PointStable 11)

-- | Daml-LF version 1.12
version1_12 :: Version
version1_12 = Version V1 (PointStable 12)

-- | Daml-LF version 1.13
version1_13 :: Version
version1_13 = Version V1 (PointStable 13)

-- | Daml-LF version 1.14
version1_14 :: Version
version1_14 = Version V1 (PointStable 14)

-- | Daml-LF version 1.15
version1_15 :: Version
version1_15 = Version V1 (PointStable 15)

-- | Daml-LF version 1.16
version1_16 :: Version
version1_16 = Version V1 (PointStable 16)

-- | The Daml-LF version used by default.
versionDefault :: Version
versionDefault = version1_15

-- | The Daml-LF 1.x development version.
version1_dev :: Version
version1_dev = Version V1 PointDev

-- | Daml-LF version 2.1
version2_1 :: Version
version2_1 = Version V2 (PointStable 1)

-- | The Daml-LF 2.x development version.
version2_dev :: Version
version2_dev = Version V2 PointDev

-- Must be kept in sync with COMPILER_LF_VERSION in daml-lf.bzl.
supportedOutputVersions :: [Version]
supportedOutputVersions = 
  [ version1_14 
  , version1_15 
  , version1_16
  , version1_dev
--  , version2_1, 
--  , version2_dev
  ]

supportedInputVersions :: [Version]
supportedInputVersions =
  [version1_8, version1_11, version1_12, version1_13] ++ supportedOutputVersions

-- | The Daml-LF version used by default by the compiler if it matches the
-- provided major version, the latest non-dev version with that major version
-- otherwise. This function is meant to be used in tests who want to test the
-- closest thing to the default user experience given a major version.
--
-- >>> map (renderVersion . defaultOrLatestStable) [minBound .. maxBound]
-- ["1.15","2.1"]
defaultOrLatestStable :: MajorVersion -> Version
defaultOrLatestStable major
    | versionMajor versionDefault == major = versionDefault
    | otherwise =
        Version major $
            maximum
                [ minv
                | Version majv minv@(PointStable _) <- supportedOutputVersions
                , majv == major
                ]

isDevVersion :: Version -> Bool
isDevVersion (Version _ PointDev) = True
isDevVersion _ = False

-- | True iff package metata is required for this LF version.
requiresPackageMetadata :: Version -> Bool
requiresPackageMetadata = \case
  Version V1 n -> n > PointStable 7
  Version V2 _ -> True

-- | A datatype describing a set of language versions. Used in the definition of
-- 'Feature' below.
newtype VersionReq = VersionReq (MajorVersion -> R.Range MinorVersion)

-- | @version `satisfies` versionReq@ iff version is part of the set of versions
-- described by versionReq.
satisfies :: Version -> VersionReq -> Bool
satisfies (Version major minor) (VersionReq req) = minor `R.elem` req major

-- | The set of language versions made of only dev versions.
devOnly :: VersionReq
devOnly = VersionReq (\_ -> R.Inclusive PointDev PointDev)

-- | The minor version range [v .. dev]. Shorthand used in the definition of
-- features below.
allMinorVersionsAfter :: MinorVersion -> R.Range MinorVersion
allMinorVersionsAfter v = R.Inclusive v PointDev

-- | The minor version range [1 .. dev]. Shorthand used in the definition of
-- features below.
allMinorVersions :: R.Range MinorVersion
allMinorVersions = allMinorVersionsAfter (PointStable 1)

-- | The empty minor version range. Shorthand used in the definition of features
-- below.
noMinorVersion :: R.Range MinorVersion
noMinorVersion = R.Empty

data Feature = Feature
    { featureName :: !T.Text
    , featureVersionReq :: !VersionReq
    , featureCppFlag :: Maybe T.Text
        -- ^ CPP flag to test for availability of the feature.
    } deriving Show

-- | The earliest version of a feature for a given major version, if it exists.
featureMinVersion :: Feature -> MajorVersion -> Maybe Version
featureMinVersion Feature{featureVersionReq = VersionReq rangeForMajor} major =
  Version major <$> R.minBound (rangeForMajor major)

-- | Kept for serialization of stable packages.
featureStringInterning :: Feature
featureStringInterning = Feature
    { featureName = "String interning"
    , featureVersionReq = VersionReq \case
          V1 -> allMinorVersionsAfter (PointStable 7)
          V2 -> allMinorVersions
    , featureCppFlag = Nothing
    }

-- | Kept for serialization of stable packages.
featureTypeInterning :: Feature
featureTypeInterning = Feature
    { featureName = "Type interning"
    , featureVersionReq = VersionReq \case
          V1 -> allMinorVersionsAfter (PointStable 11)
          V2 -> allMinorVersions
    , featureCppFlag = Nothing
    }

-- Unstable, experimental features. This should stay in x.dev forever.
-- Features implemented with this flag should be moved to a separate
-- feature flag once the decision to add them permanently has been made.
featureUnstable :: Feature
featureUnstable = Feature
    { featureName = "Unstable, experimental features"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_UNSTABLE"
    }

featureBigNumeric :: Feature
featureBigNumeric = Feature
    { featureName = "BigNumeric type"
    , featureVersionReq = VersionReq \case
          V1 -> allMinorVersionsAfter (PointStable 13)
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_BIGNUMERIC"
    }

featureExceptions :: Feature
featureExceptions = Feature
    { featureName = "Daml Exceptions"
    , featureVersionReq = VersionReq \case
          V1 -> allMinorVersionsAfter (PointStable 14)
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_EXCEPTIONS"
    }

featureNatSynonyms :: Feature
featureNatSynonyms = Feature
    { featureName = "Nat type synonyms"
    , featureVersionReq = VersionReq \case
          V1 -> allMinorVersionsAfter (PointStable 14)
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_NAT_SYN"
    }

featureSimpleInterfaces :: Feature
featureSimpleInterfaces = Feature
    { featureName = "Daml Interfaces"
    , featureVersionReq = VersionReq \case
          V1 -> allMinorVersionsAfter (PointStable 15)
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_INTERFACE"
    }

featureExtendedInterfaces :: Feature
featureExtendedInterfaces = Feature
    { featureName = "Guards in interfaces"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_INTERFACE_EXTENDED"
    }

featureChoiceFuncs :: Feature
featureChoiceFuncs = Feature
    { featureName = "choiceController and choiceObserver functions"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_CHOICE_FUNCS"
    }

featureTemplateTypeRepToText :: Feature
featureTemplateTypeRepToText = Feature
    { featureName = "templateTypeRepToText function"
    , featureVersionReq = VersionReq \case
          V1 -> noMinorVersion
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_TEMPLATE_TYPEREP_TO_TEXT"
    }

featureDynamicExercise :: Feature
featureDynamicExercise = Feature
    { featureName = "dynamicExercise function"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_DYNAMIC_EXERCISE"
    }

featurePackageUpgrades :: Feature
featurePackageUpgrades = Feature
    { featureName = "Package upgrades POC"
    , featureVersionReq = VersionReq \case
        V1 ->  allMinorVersionsAfter (PointStable 16)
        V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_PACKAGE_UPGRADES"
    }

featureNatTypeErasure :: Feature
featureNatTypeErasure = Feature
    { featureName = "Erasing types of kind Nat"
    , featureVersionReq = VersionReq \case
          V1 -> noMinorVersion
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_NAT_TYPE_ERASURE"
    }

-- This feature does not impact the compiler, but does control the evaluation
-- order integration tests via @SUPPORTS-LF-FEATURE.
featureRightToLeftEvaluation :: Feature
featureRightToLeftEvaluation = Feature
    { featureName = "Right-to-left evaluation order"
    , featureVersionReq = VersionReq \case
          V1 -> noMinorVersion
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_RIGHT_TO_LEFT_EVALUATION"
    }

-- This is used to remove references to Scenarios in LFv2
featureScenarios :: Feature
featureScenarios = Feature
    { featureName = "Scenarios"
    , featureVersionReq = VersionReq \case
          V1 -> allMinorVersions
          V2 -> noMinorVersion
    , featureCppFlag = Just "DAML_SCENARIOS"
    }

featureExperimental :: Feature
featureExperimental = Feature
    { featureName = "Daml Experimental"
    , featureVersionReq = devOnly
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
    , featureScenarios
    , featureUnstable
    , featureExperimental
    , featureDynamicExercise
    , featurePackageUpgrades
    , featureNatTypeErasure
    , featureRightToLeftEvaluation
    ]

-- | A map from feature CPP flags to features.
featureMap :: MS.Map T.Text Feature
featureMap = MS.fromList
    [ (key, feature)
    | feature <- allFeatures
    , Just key <- [featureCppFlag feature]
    ]

-- | Return the version requirements associated with a feature flag.
versionReqForFeature :: T.Text -> Maybe VersionReq
versionReqForFeature key = featureVersionReq <$> MS.lookup key featureMap

-- | Same as 'versionForFeature' but errors out if the feature doesn't exist.
versionReqForFeaturePartial :: T.Text -> VersionReq
versionReqForFeaturePartial key =
    case versionReqForFeature key of
        Just version -> version
        Nothing ->
            error . T.unpack . T.concat $
                [ "Unknown feature: "
                , key
                , ". Available features are: "
                , T.intercalate ", " (MS.keys featureMap)
                ]

-- | All the language features that the given language version supports.
allFeaturesForVersion :: Version -> [Feature]
allFeaturesForVersion version = filter (supports version) allFeatures

-- | Whether the given language version supports the given language feature.
supports :: Version -> Feature -> Bool
supports version feature = version `satisfies` featureVersionReq feature

renderMajorVersion :: MajorVersion -> String
renderMajorVersion = \case
  V1 -> "1"
  V2 -> "2"

readSimpleInt :: ReadP Int
readSimpleInt = read <$> munch1 isDigit

readMajorVersion :: ReadP MajorVersion
readMajorVersion = do
  n <- readSimpleInt
  case n of
    1 -> pure V1
    2 -> pure V2
    _ -> pfail

-- >>> parseMajorVersion "1"
-- Just V1
-- >>> parseMajorVersion "garbage"
-- Nothing
parseMajorVersion :: String -> Maybe MajorVersion
parseMajorVersion = headMay . map fst . readP_to_S readMajorVersion

renderMinorVersion :: MinorVersion -> String
renderMinorVersion = \case
  PointStable minor -> show minor
  PointDev -> "dev"

readMinorVersion :: ReadP MinorVersion
readMinorVersion = readStable +++ readDev
  where
    readStable = PointStable <$> readSimpleInt
    readDev = PointDev <$ ReadP.string "dev"

-- >>> parseMinorVersion "14"
-- Just (PointStable 14)
-- >>> parseMinorVersion "dev"
-- Just PointDev
-- >>> parseMinorVersion "garbage"
-- Nothing
parseMinorVersion :: String -> Maybe MinorVersion
parseMinorVersion = headMay . map fst . readP_to_S readMinorVersion

renderVersion :: Version -> String
renderVersion (Version major minor) =
    renderMajorVersion major <> "." <> renderMinorVersion minor

readVersion :: ReadP Version
readVersion = do
  major <- readMajorVersion
  _ <- ReadP.char '.'
  minor <- readMinorVersion
  pure (Version major minor)

-- >>> parseVersion "1.dev"
-- Just (Version {versionMajor = V1, versionMinor = PointDev})
-- >>> parseVersion "1.15"
-- Just (Version {versionMajor = V1, versionMinor = PointStable 15})
-- >>> parseVersion "1.garbage"
-- Nothing
parseVersion :: String -> Maybe Version
parseVersion = headMay . map fst . readP_to_S readVersion

-- >>> show (VersionReq (\case V1 -> noMinorVersion; V2 -> allV2MinorVersions))
-- "VersionReq (\\case V1 -> Empty; V2 -> Inclusive_ PointDev PointDev)"
instance Show VersionReq where
    show (VersionReq req) =
        concat
            [ "VersionReq (\\case V1 -> "
            , show (req V1)
            , "; V2 -> "
            , show (req V2)
            , ")"
            ]

{-|
Renders a FeatureVersionReq.

>>> let r1 = R.Inclusive (PointStable 1) (PointStable 2)
>>> let r2 = R.Inclusive (PointStable 3) PointDev

>>> renderFeatureVersionReq (VersionReq (\case V1 -> R.Empty; V2 ->  R.Empty))
"none"
>>> renderFeatureVersionReq (VersionReq (\case V1 ->  r1; V2 -> R.Empty))
"1.1 to 1.2"
>>> renderFeatureVersionReq (VersionReq (\case V1 -> R.Empty; V2 -> r2))
"2.3 to 2.dev"
>>> renderFeatureVersionReq (VersionReq (\case V1 -> r1; V2 -> r2))
"1.1 to 1.2, or 2.3 to 2.dev"
-}
renderFeatureVersionReq :: VersionReq -> String
renderFeatureVersionReq (VersionReq req) = renderRanges (req V1) (req V2)
  where
    renderRanges R.Empty R.Empty = "none"
    renderRanges v1Range v2Range =
      intercalate ", or " $
        catMaybes
            [ renderRange (Version V1) v1Range
            , renderRange (Version V2) v2Range
            ]

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
