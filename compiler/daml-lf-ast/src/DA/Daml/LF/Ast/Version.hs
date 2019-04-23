-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module DA.Daml.LF.Ast.Version where

import           DA.Prelude
import           DA.Pretty
import           Control.DeepSeq
import           Data.Aeson (FromJSON, ToJSON)
import qualified Data.Text as T

-- | DAML-LF version of an archive payload.
data Version
  = V1{versionMinor :: Int}
  | VDev T.Text

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
versionDefault = version1_2

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

supportsOptional :: Version -> Bool
supportsOptional v = v >= version1_1

supportsTextMap :: Version -> Bool
supportsTextMap v = v >= version1_3

supportsPartyOrd :: Version -> Bool
supportsPartyOrd v = v >= version1_1

supportsArrowType :: Version -> Bool
supportsArrowType v = v >= version1_1

supportsSha256Text :: Version -> Bool
supportsSha256Text v = v >= version1_2

supportsDisjunctionChoices :: Version -> Bool
supportsDisjunctionChoices v = v >= version1_2

supportsContractKeys :: Version -> Bool
supportsContractKeys v = v >= version1_3

supportsPartyFromText :: Version -> Bool
supportsPartyFromText v = v >= version1_2

concatSequenceA $
  map (makeInstancesExcept [''FromJSON, ''ToJSON])
  [ ''Version
  ]

instance NFData Version

instance Pretty Version where
  pPrint = \case
    V1 minor -> "1." <> pretty minor
    VDev hash -> "dev-" <> pretty hash

instance ToJSON Version
