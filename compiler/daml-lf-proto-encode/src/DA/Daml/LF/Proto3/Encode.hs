-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Encode
  ( encodePayload
  ) where

import qualified Data.Text.Lazy as TL
import Com.Daml.DamlLfDev.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import qualified Com.Daml.DamlLfDev.DamlLf1 as LF1
import qualified Com.Daml.DamlLfDev.DamlLf2 as LF2
import DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1
import Proto3.Suite (toLazyByteString, fromByteString)
import qualified Data.ByteString.Lazy as BL
import Data.Either (fromRight)

encodePayload :: Package -> ArchivePayload
encodePayload package = case packageLfVersion package of
    V1 minor ->
        let payload = ArchivePayloadSumDamlLf1 (EncodeV1.encodePackage package)
        in  ArchivePayload (TL.pack $ renderMinorVersion minor) (Just payload)
    V2 minor ->
        -- The DamlLf2 proto is currently a copy of DamlLf1 so we can coerce one to the other.
        -- TODO(#17366): Introduce a new DamlLf2 encoder once we introduce changes to DamlLf2.
        let payload = ArchivePayloadSumDamlLf2 (coerceLF1toLF2 (EncodeV1.encodePackage package))
        in  ArchivePayload (TL.pack $ renderMinorVersion minor) (Just payload)


-- TODO(#17366): Delete as soon as the DamlLf2 proto diverges from the DamlLF1 one.
coerceLF1toLF2 :: LF1.Package -> LF2.Package
coerceLF1toLF2 package =
  fromRight
    (error "cannot coerce LF1 proto to LF2 proto")
    (fromByteString (BL.toStrict $ toLazyByteString package))
