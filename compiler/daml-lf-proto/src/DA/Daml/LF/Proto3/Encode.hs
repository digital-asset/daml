-- Copyright (c) 2019 The DAML Authors. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Encode
  ( encodePayload
  , encodeModuleNameIndex
  ) where

import Data.Word (Word64)
import qualified Data.Text.Lazy as TL
import Da.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1

type ModuleNameIndex = ModuleName -> Maybe Word64

encodePayload :: Package -> ArchivePayload
encodePayload package = case packageLfVersion package of
    V1 minor ->
        let payload = ArchivePayloadSumDamlLf1 (EncodeV1.encodePackage package)
        in  ArchivePayload (TL.pack $ renderMinorVersion minor) (Just payload)

encodeModuleNameIndex :: Package -> ModuleNameIndex
encodeModuleNameIndex package = case packageLfVersion package of
    V1 _ -> EncodeV1.encodedInternedModuleNameIndex package
