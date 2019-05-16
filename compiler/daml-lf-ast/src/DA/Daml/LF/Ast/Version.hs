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

-- | DAML-LF version 1.1.
version1_1 :: Version
version1_1 = V1 $ PointStable 1

-- | DAML-LF version 1.2.
version1_2 :: Version
version1_2 = V1 $ PointStable 2

-- | DAML-LF version 1.3.
version1_3 :: Version
version1_3 = V1 $ PointStable 3

-- | The DAML-LF version used by default.
versionDefault :: Version
versionDefault = version1_3

-- | The DAML-LF development version.
versionDev :: Version
versionDev = V1 PointDev

supportedInputVersions :: [Version]
supportedInputVersions = [version1_1, version1_2, version1_3, versionDev]

supportedOutputVersions :: [Version]
supportedOutputVersions = supportedInputVersions


data Feature = Feature
    { featureName :: !T.Text
    , featureMinVersion :: !Version
    }

featureTextMap :: Feature
featureTextMap = Feature "Map type" version1_3

featureSha256Text :: Feature
featureSha256Text = Feature "sha256 function" version1_2

featureFlexibleControllers :: Feature
featureFlexibleControllers = Feature "Flexible controllers" version1_2

featureContractKeys :: Feature
featureContractKeys = Feature "Contract keys" version1_3

featurePartyFromText :: Feature
featurePartyFromText = Feature "partyFromText function" version1_2

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
