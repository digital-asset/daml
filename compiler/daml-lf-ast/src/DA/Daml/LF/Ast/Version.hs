-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE DeriveAnyClass #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module DA.Daml.LF.Ast.Version where

import           DA.Prelude
import           DA.Pretty
import           Control.DeepSeq
import           Data.Aeson
import qualified Data.Text as T

-- | DAML-LF version of an archive payload.
data Version
  = V1{versionMinor :: Int}
  | VDev T.Text
  deriving (Eq, Generic, NFData, Ord, Show, ToJSON)

-- | DAML-LF version 1.0.
version1_0 :: Version
version1_0 = V1 0

-- | DAML-LF version 1.1.
version1_1 :: Version
version1_1 = V1 1

-- | DAML-LF version 1.2.
version1_2 :: Version
version1_2 = V1 2

-- | DAML-LF version 1.3.
version1_3 :: Version
version1_3 = V1 3

-- | The DAML-LF version used by default.
versionDefault :: Version
versionDefault = version1_3

-- | The newest non-development DAML-LF version.
versionNewest :: Version
versionNewest = version1_3

maxV1minor :: Int
maxV1minor = 3

-- NOTE(MH): 'VDev' does not appear in this list because it is handled differently.
supportedInputVersions :: [Version]
supportedInputVersions = [version1_0, version1_1, version1_2, version1_3]

supportedOutputVersions :: [Version]
supportedOutputVersions = supportedInputVersions


data Feature = Feature
    { featureName :: !T.Text
    , featureMinVersion :: !Version
    }

featureOptional :: Feature
featureOptional = Feature "Optional type" version1_1

featureTextMap :: Feature
featureTextMap = Feature "Map type" version1_3

featurePartyOrd :: Feature
featurePartyOrd = Feature "Party comparison" version1_1

featureArrowType :: Feature
featureArrowType = Feature "Arrow type" version1_1

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

instance Pretty Version where
  pPrint = \case
    V1 minor -> "1." <> pretty minor
    VDev hash -> "dev-" <> pretty hash
