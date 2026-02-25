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
import           Text.ParserCombinators.ReadP (ReadP, pfail, readP_to_S, (+++), munch1, eof)
import qualified Text.ParserCombinators.ReadP as ReadP
import qualified Data.Map.Strict as MS

import Control.Lens (Getting, view)
import Control.Monad.Reader.Class

import DA.Daml.LF.Ast.Version.VersionType
import DA.Daml.LF.Ast.Version.GeneratedFeatures

-- | x `canDependOn` y if dars compiled to version x can depend on dars compiled
-- to version y.
canDependOn :: Version -> Version -> Bool
canDependOn (Version major1 minor1) (Version major2 minor2) =
  major1 == major2 && minor1 >= minor2

isDevVersion :: Version -> Bool
isDevVersion (Version _ PointDev) = True
isDevVersion _ = False

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

-- | A map from feature CPP flags to features.
featureMap :: MS.Map T.Text Feature
featureMap = MS.fromList
    [ (key, feature)
    | feature <- allFeatures
    , key <- [featureCppFlag feature]
    ]
-- | Return the feature associated with a feature flag.
nameToFeatureOpt :: T.Text -> Maybe Feature
nameToFeatureOpt = flip MS.lookup featureMap

-- | Same as 'nameToFeatureOpt' but errors out if the feature doesn't exist.
nameToFeature :: T.Text -> Feature
nameToFeature key =
  case nameToFeatureOpt key of
    Just version -> version
    Nothing ->
        error . T.unpack . T.concat $
            [ "Unknown feature: "
            , key
            , ". Available features are: "
            , T.intercalate ", " (MS.keys featureMap)
            ]

-- | Return the version requirements associated with a feature flag.
versionReqForFeature :: T.Text -> Maybe VersionReq
versionReqForFeature key = featureVersionReq <$> nameToFeatureOpt key

-- | Same as 'versionForFeature' but errors out if the feature doesn't exist.
versionReqForFeaturePartial :: T.Text -> VersionReq
versionReqForFeaturePartial = featureVersionReq . nameToFeature

-- | All the language features that the given language version supports.
allFeaturesForVersion :: Version -> [Feature]
allFeaturesForVersion version = filter (supports version) allFeatures

-- | Whether the given language version supports the given language feature.
supports :: Version -> Feature -> Bool
supports version feature = version `R.elem` featureVersionReq feature

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

-- >>> (headMay . map fst . readP_to_S (readMinorVersion <* eof)) "3-staging"
-- Just (PointStaging 3)
readMinorVersion :: ReadP MinorVersion
readMinorVersion = readStable +++ readStaging +++ readStagingWithRevision +++ readDev
  where
    readStable = PointStable <$> readSimpleInt
    readStaging = PointStaging <$> readSimpleInt <* ReadP.string "-staging"
    readStagingWithRevision =
      (\i r-> if r == stagingRevision
        then PointStaging i
        else error $ "unsupported staging revision "
                      ++ show r
                      ++ " whilst readming minorVersion, supported staging revision: "
                      ++ show stagingRevision)
        <$> readSimpleInt <*> (ReadP.string "-rc" *> readSimpleInt)
    readDev = PointDev <$ ReadP.string "dev"

-- >>> parseMinorVersion "3-staging"
-- Just (PointStaging 3)
-- >>> parseMinorVersion "4-rc1"
-- Just (PointStaging 4)
-- >>> parseMinorVersion "4-rc2"
-- unsupported staging revision 2 whilst readming minorVersion, supported staging revision: 1
-- >>> parseMinorVersion "14"
-- Just (PointStable 14)
-- >>> parseMinorVersion "dev"
-- Just PointDev
-- >>> parseMinorVersion "2garbage"
-- Nothing
parseMinorVersion :: String -> Maybe MinorVersion
parseMinorVersion = headMay . map fst . readP_to_S (readMinorVersion <* eof)

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
-- >>> parseVersion "2.3-staging"
-- Just (Version {versionMajor = V2, versionMinor = PointStaging 3})
-- >>> parseVersion "2.garbage"
-- Nothing
parseVersion :: String -> Maybe Version
parseVersion = headMay . map fst . readP_to_S (readVersion <* eof)

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
