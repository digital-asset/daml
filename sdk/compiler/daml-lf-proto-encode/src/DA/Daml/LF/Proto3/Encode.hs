-- Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Encode
  ( encodePayload
  , EncodeV2.encodePackage
  , EncodeV2.encodeSinglePackageModule
  ) where

import qualified Data.Text.Lazy as TL
import Com.Digitalasset.Daml.Lf.Archive.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.EncodeV2 as EncodeV2
import qualified Proto3.Suite             as Proto
import qualified Data.ByteString.Lazy     as BSL

encodePayload :: Package -> ArchivePayload
encodePayload package = case packageLfVersion package of
    (Version V2 minor) ->
        let payload = ArchivePayloadSumDamlLf2 $ BSL.toStrict $ Proto.toLazyByteString (EncodeV2.encodePackage package)
        in ArchivePayload (TL.pack $ renderMinorVersionWithRev minor) 0 (Just payload)
