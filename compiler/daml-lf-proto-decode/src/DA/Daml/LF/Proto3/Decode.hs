-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayload
  ) where

import Com.Daml.DamlLfDev.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast (Package, PackageId, PackageRef, packageLfVersion, Version (..), versionMinor, MajorVersion(V2))
import DA.Daml.LF.Proto3.Error
import DA.Daml.LF.Proto3.DecodeV1 qualified as DecodeV1
import Com.Daml.DamlLfDev.DamlLf1 qualified as LF1
import Com.Daml.DamlLfDev.DamlLf2 qualified as LF2
import Proto3.Suite (toLazyByteString, fromByteString)
import Data.ByteString.Lazy qualified as BL
import Proto3.Wire.Decode (ParseError)
import Data.Bifunctor (first)

decodePayload :: PackageId -> PackageRef -> ArchivePayload -> Either Error Package
decodePayload pkgId selfPackageRef payload = case archivePayloadSum payload of
    Just (ArchivePayloadSumDamlLf1 package) -> 
      DecodeV1.decodePackage (Just pkgId) minor selfPackageRef package
    Just (ArchivePayloadSumDamlLf2 package) -> do
      -- The DamlLf2 proto is currently a copy of DamlLf1 so we can coerce one to the other.
      -- TODO(#17366): Introduce a new DamlLf2 decoder once we introduce changes to DamlLf2.
      lf1Package <- first (ParseError . show) (coerceLF2toLF1 package)
      package <- DecodeV1.decodePackage (Just pkgId) minor selfPackageRef lf1Package
      return (package { packageLfVersion = Version V2 (versionMinor $ packageLfVersion package) })
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload

-- TODO(#17366): Delete as soon as the DamlLf2 proto diverges from the DamlLF1 one.
coerceLF2toLF1 :: LF2.Package -> Either ParseError LF1.Package
coerceLF2toLF1 package = fromByteString (BL.toStrict $ toLazyByteString package)
