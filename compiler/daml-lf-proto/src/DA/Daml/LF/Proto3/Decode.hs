-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayloads
  , decodePayload
  , decodeCrossReferences
  , CrossReferences
  ) where

import qualified Data.HashMap.Lazy as M
import Data.Foldable (toList)
import Data.Word (Word64)
import Da.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast (Package, PackageId, ModuleName)
import DA.Daml.LF.Proto3.Error (Error(ParseError), Decode)
import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1

type ModuleNameIndex = Word64 -> Maybe ModuleName

newtype CrossReferences = CrossReferences (PackageId -> ModuleNameIndex)

-- traverses ArchivePayloads twice; we could do only one traversal if they were
-- guaranteed to be topologically sorted, but they are not guaranteed to be so,
-- even if our own implementation happens to do that when encoding.  We could
-- also make ModuleNameIndex produce Decode ModuleName to avoid the first
-- traversal, since the function would then be nonstrict on ArchivePayload
decodePayloads :: Traversable f => f (PackageId, ArchivePayload) -> Decode (f Package)
decodePayloads payloads = do
  depModNames <- decodeCrossReferences payloads
  traverse (decodePayload depModNames . snd) payloads

decodePayload :: CrossReferences -> ArchivePayload -> Decode Package
decodePayload (CrossReferences depModNames) payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodePackage depModNames minor package
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload

decodeCrossReferences :: Foldable f => f (PackageId, ArchivePayload) -> Decode CrossReferences
decodeCrossReferences payloads =
  let decodes = (traverse . traverse) decodeModuleNameIndex $ toList payloads
      depModNames index mn w = mn `M.lookup` index >>= ($ w)
  in CrossReferences . depModNames . M.fromList <$> decodes

decodeModuleNameIndex :: ArchivePayload -> Decode ModuleNameIndex
decodeModuleNameIndex payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodeInternedModuleNameIndex package
    Nothing -> Left $ ParseError "Empty payload"
