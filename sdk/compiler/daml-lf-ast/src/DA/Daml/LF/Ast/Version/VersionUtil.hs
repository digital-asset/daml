-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE BlockArguments #-}

module DA.Daml.LF.Ast.Version.VersionUtil (
  module DA.Daml.LF.Ast.Version.VersionUtil
) where

import           Data.Char (isDigit)
import qualified DA.Daml.LF.Ast.Range as R
import qualified Data.Text as T
import           Safe (headMay)
import           Text.ParserCombinators.ReadP (ReadP, pfail, readP_to_S, (+++), munch1)
import qualified Text.ParserCombinators.ReadP as ReadP
import qualified Data.Map.Strict as MS

import Control.Lens (Getting, view)
import Control.Monad.Reader.Class

import DA.Daml.LF.Ast.Version.VersionType

-- | x `canDependOn` y if dars compiled to version x can depend on dars compiled
-- to version y.
canDependOn :: Version -> Version -> Bool
canDependOn (Version major1 minor1) (Version major2 minor2) =
  major1 == major2 && minor1 >= minor2

isDevVersion :: Version -> Bool
isDevVersion (Version _ PointDev) = True
isDevVersion _ = False

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

-- Unstable, experimental features. This should stay in x.dev forever.
-- Features implemented with this flag should be moved to a separate
-- feature flag once the decision to add them permanently has been made.
featureUnstable :: Feature
featureUnstable = Feature
    { featureName = "Unstable, experimental features"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_UNSTABLE"
    }

featureTextMap :: Feature
featureTextMap = Feature
    { featureName = "TextMap type"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_TEXTMAP"
    }

featureBigNumeric :: Feature
featureBigNumeric = Feature
    { featureName = "BigNumeric type"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_BIGNUMERIC"
    }

featureExceptions :: Feature
featureExceptions = Feature
    { featureName = "Daml Exceptions"
    , featureVersionReq = VersionReq \case
          V2 -> allMinorVersions
    , featureCppFlag = Just "DAML_EXCEPTIONS"
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
    -- TODO: https://github.com/digital-asset/daml/issues/20786: complete implementing this feature
    , featureVersionReq = VersionReq $ const noMinorVersion
    , featureCppFlag = Just "DAML_CHOICE_FUNCS"
    }

featureTemplateTypeRepToText :: Feature
featureTemplateTypeRepToText = Feature
    { featureName = "templateTypeRepToText function"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_TEMPLATE_TYPEREP_TO_TEXT"
    }

featureContractKeys :: Feature
featureContractKeys = Feature
    { featureName = "Contract Keys"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_CONTRACT_KEYS"
    }

featureFlatArchive :: Feature
featureFlatArchive = Feature
    { featureName = "Flat Archive"
    , featureVersionReq = VersionReq $ \case
          V2 -> allMinorVersionsAfter (PointStable 2)
    , featureCppFlag = Just "DAML_FLATARCHIVE"
    }

featurePackageImports :: Feature
featurePackageImports = Feature
    { featureName = "Explicit package imports"
    , featureVersionReq = VersionReq $ \case
          V2 -> allMinorVersionsAfter (PointStable 2)
    , featureCppFlag = Just "DAML_PackageImports"
    }

featureComplexAnyType :: Feature
featureComplexAnyType = Feature
    { featureName = "Complex Any type"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_COMPLEX_ANY_TYPE"
    }

featureCryptoUtility :: Feature
featureCryptoUtility = Feature
    { featureName = "Crypto Utility Function"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_CRYPTO_UTILITY"
    }

featureExperimental :: Feature
featureExperimental = Feature
    { featureName = "Daml Experimental"
    , featureVersionReq = devOnly
    , featureCppFlag = Just "DAML_EXPERIMENTAL"
    }

-- | CPP flags of past features that have become part of LF but that some
-- clients might still depend on being defined.
foreverCppFlags :: [T.Text]
foreverCppFlags =
    [ "DAML_NAT_SYN"
    , "DAML_INTERFACE"
    , "DAML_RIGHT_TO_LEFT_EVALUATION"
    ]

-- TODO: https://github.com/digital-asset/daml/issues/15882
-- Ought we have "featureChoiceAuthority" ?

allFeatures :: [Feature]
allFeatures =
    [ featureTextMap
    , featureBigNumeric
    , featureExceptions
    , featureExtendedInterfaces
    , featureChoiceFuncs
    , featureTemplateTypeRepToText
    , featureContractKeys
    , featureUnstable
    , featureExperimental
    , featureCryptoUtility
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

readSimpleInt :: ReadP Int
readSimpleInt = read <$> munch1 isDigit

readMajorVersion :: ReadP MajorVersion
readMajorVersion = do
  n <- readSimpleInt
  case n of
    2 -> pure V2
    _ -> pfail

-- >>> parseMajorVersion "2"
-- Just V2
-- >>> parseMajorVersion "garbage"
-- Nothing
parseMajorVersion :: String -> Maybe MajorVersion
parseMajorVersion = headMay . map fst . readP_to_S readMajorVersion

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

readVersion :: ReadP Version
readVersion = do
  major <- readMajorVersion
  _ <- ReadP.char '.'
  minor <- readMinorVersion
  pure (Version major minor)

-- >>> parseVersion "2.dev"
-- Just (Version {versionMajor = V2, versionMinor = PointDev})
-- >>> parseVersion "2.15"
-- Just (Version {versionMajor = V2, versionMinor = PointStable 15})
-- >>> parseVersion "2.garbage"
-- Nothing
parseVersion :: String -> Maybe Version
parseVersion = headMay . map fst . readP_to_S readVersion

-- The extended implementation
ifVersionWith :: MonadReader r m
              => Getting Version r Version -- ^ A lens for the 'Version' in the environment
              -> (Version -> Bool)         -- ^ The predicate to apply to the 'Version'
              -> (Version -> m a)          -- ^ The action to run if the predicate is True
              -> (Version -> m a)          -- ^ The action to run if the predicate is False
              -> m a
ifVersionWith l p b1 b2 = do
    v <- view l
    if p v
      then b1 v
      else b2 v

ifVersion :: MonadReader r m
          => Getting Version r Version -- ^ A lens for the 'Version' in the environment
          -> (Version -> Bool)         -- ^ The predicate to apply to the 'Version'
          -> m a                       -- ^ The action to run if the predicate is True
          -> m a                       -- ^ The action to run if the predicate is False
          -> m a
ifVersion l p b1 b2 = ifVersionWith l p (const b1) (const b2)

ifSupports :: MonadReader r m => Getting Version r Version -> Feature -> m a -> m a -> m a
ifSupports l f = ifVersion l (`supports` f)

whenSupports :: MonadReader r m => Getting Version r Version -> Feature -> (Version -> m ()) -> m ()
whenSupports l f b = do
  ifVersionWith l (`supports` f) (const $ return ()) b

whenSupportsNot :: MonadReader r m => Getting Version r Version -> Feature -> (Version -> m ()) -> m ()
whenSupportsNot l f b = do
  ifVersionWith l (`supports` f) b (const $ return ())
