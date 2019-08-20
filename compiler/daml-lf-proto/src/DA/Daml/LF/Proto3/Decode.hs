-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayload
  , decodeModuleNameIndex
  ) where

import Data.Word (Word64)
import Da.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast (Package, ModuleName)
import DA.Daml.LF.Proto3.Error (Error(ParseError), Decode)
import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1

decodePayload :: ArchivePayload -> Decode Package
decodePayload payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodePackage minor package
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload

decodeModuleNameIndex :: ArchivePayload -> Decode (Word64 -> Maybe ModuleName)
decodeModuleNameIndex payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodeInternedModuleNameIndex package
    Nothing -> Left $ ParseError "Empty payload"
