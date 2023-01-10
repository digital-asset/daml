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

decodePayload :: PackageId -> PackageRef -> ArchivePayload -> Either Error Package
decodePayload pkgId selfPackageRef payload = case archivePayloadSum payload of
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodePackage (Just pkgId) minor selfPackageRef package
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload
