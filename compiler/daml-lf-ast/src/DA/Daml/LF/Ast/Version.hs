-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
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

-- | DAML-LF version 1.4.
version1_4 :: Version
version1_4 = V1 $ PointStable 4

-- | DAML-LF version 1.5
version1_5 :: Version
version1_5 = V1 $ PointStable 5

-- TODO(MH): Roll this when freezing DAML-LF 1.6.
version1_6 :: Version
version1_6 = versionDev
-- version1_6 = V1 $ PointStable 6

-- | The DAML-LF version used by default.
versionDefault :: Version
versionDefault = version1_5

-- | The DAML-LF development version.
versionDev :: Version
versionDev = V1 PointDev

supportedInputVersions :: [Version]
supportedInputVersions = [version1_4, version1_5, versionDev]

supportedOutputVersions :: [Version]
supportedOutputVersions = supportedInputVersions


data Feature = Feature
    { featureName :: !T.Text
    , featureMinVersion :: !Version
    }

featureSerializablePolymorphicContractIds :: Feature
featureSerializablePolymorphicContractIds = Feature "Serializable polymorphic contract ids" version1_5

featureCoerceContractId :: Feature
featureCoerceContractId = Feature "Coerce function for contract ids" version1_5

featureExerciseActorsOptional :: Feature
featureExerciseActorsOptional = Feature "Optional exercise actors" version1_5

-- TODO(MH): When we remove this because we drop support for DAML-LF 1.4,
-- we should remove `legacyParse{Int, Decimal}` from `DA.Text` as well.
featureNumberFromText :: Feature
featureNumberFromText = Feature "Number parsing functions" version1_5

featureTextCodePoints :: Feature
featureTextCodePoints = Feature "Conversion between text and code points" version1_6

featureEnumTypes :: Feature
featureEnumTypes = Feature "Enum types" version1_6

featureInternedPackageIds :: Feature
featureInternedPackageIds = Feature "Package ID reference compression" version1_6

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
