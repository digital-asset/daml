-- Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayload
  ) where

import Com.Daml.DamlLfDev.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast (Package, PackageRef)
import DA.Daml.LF.Proto3.Error
import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1

decodePayload :: PackageRef -> ArchivePayload -> Either Error Package
decodePayload selfPackageRef payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodePackage minor selfPackageRef package
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload
