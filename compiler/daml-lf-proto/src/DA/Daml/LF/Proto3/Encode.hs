-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Encode
  ( encodePayload
  , encodeCrossReferences
  , CrossReferences
  ) where

import Data.Foldable (toList)
import Data.Word (Word64)
import qualified Data.HashMap.Lazy as M
import qualified Data.Text.Lazy as TL
import Da.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1

type ModuleNameIndex = ModuleName -> Maybe Word64

newtype CrossReferences = CrossReferences (PackageId -> ModuleNameIndex)

encodePayload :: Package -> ArchivePayload
encodePayload package = case packageLfVersion package of
    V1 minor ->
        let payload = ArchivePayloadSumDamlLf1 (EncodeV1.encodePackage zeroPriorPackages package)
        in  ArchivePayload (TL.pack $ renderMinorVersion minor) (Just payload)

encodeCrossReferences :: Foldable f => f (PackageId, ArchivePayload) -> CrossReferences
encodeCrossReferences payloads = CrossReferences lookup
  where lookup pi mn = (pi `M.lookup` xrefs) >>= ($ mn)
        xrefs = M.fromList $ (fmap . fmap) (payloadMNI . archivePayloadSum) $ toList payloads
        payloadMNI = \case
          Just (ArchivePayloadSumDamlLf1 package) -> EncodeV1.moduleNameIndexFromEncodedModule package
          Just (ArchivePayloadSumDamlLf0 _) -> const Nothing
          Nothing -> const Nothing

-- encodeModuleNameIndex :: Package -> ModuleNameIndex
-- encodeModuleNameIndex package = case packageLfVersion package of
--     V1 _ -> EncodeV1.encodedInternedModuleNameIndex package

zeroPriorPackages :: pid -> ModuleNameIndex
zeroPriorPackages _ _ = Nothing
