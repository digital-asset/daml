-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Decode
  ( Error(..)
  , decodePayloads
  , decodePayload
  , decodeCrossReferences
  , CrossReferences
  ) where

import Control.Monad.Error.Class (MonadError(throwError))
import qualified Data.HashMap.Lazy as M
import Data.Foldable (toList)
import Data.Word (Word64)
import Da.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast (Package, PackageId, ModuleName)
import DA.Daml.LF.Proto3.Error (Error(ParseError), Decode)
import qualified DA.Daml.LF.Proto3.DecodeV1 as DecodeV1

type ModuleNameIndex = Word64 -> Decode ModuleName

newtype CrossReferences = CrossReferences (PackageId -> ModuleNameIndex)
-- conceptually has a reasonable Monoid instance

-- we could build up the CrossReferences iteratively if they were guaranteed to
-- be topologically sorted, but they are not guaranteed to be so, even if our
-- own implementation happens to do that when encoding.  We avoid an initial
-- traversal (and a traversal here) by deferring failures in
-- decodeCrossReferences until you try to lookup a module name
decodePayloads :: (Functor f, Foldable f) => f (PackageId, ArchivePayload) -> f (Decode Package)
decodePayloads payloads =
  let depModNames = decodeCrossReferences payloads
  in decodePayload depModNames . snd <$> payloads

decodePayload :: CrossReferences -> ArchivePayload -> Decode Package
decodePayload (CrossReferences depModNames) payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodePackage depModNames minor package
    Nothing -> Left $ ParseError "Empty payload"
    where
        minor = archivePayloadMinor payload

decodeCrossReferences :: Foldable f => f (PackageId, ArchivePayload) -> CrossReferences
decodeCrossReferences payloads =
  let decodes = (fmap . fmap) decodeModuleNameIndex $ toList payloads
      depModNames index mn w = do
        mni <- maybe (throwError $ ParseError "Missing dependency package") pure
                 $ mn `M.lookup` index
        mni w
  in CrossReferences . depModNames . M.fromList $ decodes

decodeModuleNameIndex :: ArchivePayload -> ModuleNameIndex
decodeModuleNameIndex payload = case archivePayloadSum payload of
    Just ArchivePayloadSumDamlLf0{} -> const . Left $ ParseError "Payload is DamlLf0"
    Just (ArchivePayloadSumDamlLf1 package) -> DecodeV1.decodeInternedModuleNameIndex package
    Nothing -> const . Left $ ParseError "Empty payload"
