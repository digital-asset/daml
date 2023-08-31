-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayload
  ) where

import Com.Daml.DamlLfDev.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast (Package, PackageId, PackageRef)
import DA.Daml.LF.Proto3.Error
import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1
import qualified Com.Daml.DamlLfDev.DamlLf1 as LF1
import qualified Com.Daml.DamlLfDev.DamlLf2 as LF2
import Proto3.Suite (toLazyByteString, fromByteString)
import qualified Data.ByteString.Lazy as BL
import Proto3.Wire.Decode (ParseError)
import Data.Bifunctor (first)

decodePayload :: PackageId -> PackageRef -> ArchivePayload -> Either Error Package
decodePayload pkgId selfPackageRef payload = case archivePayloadSum payload of
    Just (ArchivePayloadSumDamlLf1 package) -> 
      DecodeV1.decodePackage (Just pkgId) minor selfPackageRef package
    Just (ArchivePayloadSumDamlLf2 package) -> do
      lf1Package <- first (ParseError . show) (coerceLF2toLF1 package)
      DecodeV1.decodePackage (Just pkgId) minor selfPackageRef lf1Package
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload


coerceLF2toLF1 :: LF2.Package -> Either ParseError LF1.Package
coerceLF2toLF1 package = fromByteString (BL.toStrict $ toLazyByteString package)