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
version1_7 = versionDev -- Update once 1.7 is out.

-- | The DAML-LF version used by default.
versionDefault :: Version
versionDefault = version1_6

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
    }

-- NOTE(MH): We comment this out to leave an example how to deal with features.
-- featureTextCodePoints :: Feature
-- featureTextCodePoints = Feature "Conversion between text and code points" version1_6

featureNumeric :: Feature
featureNumeric = Feature
    { featureName = "Numeric type"
    , featureMinVersion = version1_7
    }

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
