-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Encode
  ( encodePayload
  ) where

import qualified Data.Text.Lazy as TL
import Com.Digitalasset.Daml.Lf.Archive.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.EncodeV2 as EncodeV2

encodePayload :: Package -> ArchivePayload
encodePayload package = case packageLfVersion package of
    (Version V2 minor) ->
        let payload = ArchivePayloadSumDamlLf2 (EncodeV2.encodePackage package)
         in ArchivePayload (TL.pack $ renderMinorVersion minor) (Just payload)
