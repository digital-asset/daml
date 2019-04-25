-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE TemplateHaskell #-}
module DA.Daml.LF.Ast.Version where

import           DA.Prelude
import           DA.Pretty
import           Control.DeepSeq
import           Data.Aeson (FromJSON, ToJSON)
import qualified Data.Text.Lazy as TL
import qualified Text.Read as Read

-- | DAML-LF version of an archive payload.
data Version
  = V1{versionMinor :: MinorVersion}

data MinorVersion = PointStable Int | PointDev

-- | DAML-LF version 1.0.
version1_0 :: Version
version1_0 = V1 $ PointStable 0

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
versionDefault = version1_2

-- | The newest non-development DAML-LF version.
versionNewest :: Version
versionNewest = version1_3

maxV1minor :: Int
maxV1minor = 3

minorInProtobuf :: MinorVersion -> TL.Text
minorInProtobuf = TL.pack . \case
  PointStable minor -> show minor
  PointDev -> "dev"

minorFromProtobuf :: TL.Text -> Maybe MinorVersion
minorFromProtobuf = go . TL.unpack
  where go (Read.readMaybe -> Just i) = Just $ PointStable i
        go "dev" = Just PointDev
        go _ = Nothing

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
  , ''MinorVersion
  ]

instance NFData Version

instance Pretty Version where
  pPrint = \case
    V1 minor -> "1." <> pretty minor

instance ToJSON Version

instance NFData MinorVersion

instance Pretty MinorVersion where
  pPrint = \case
    PointStable minor -> pretty minor
    PointDev -> "dev"

instance ToJSON MinorVersion
