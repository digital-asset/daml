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
        let payload = ArchivePayloadSumDamlLf2 (coerceLF1toLF2 (EncodeV1.encodePackage package))
        in  ArchivePayload (TL.pack $ renderMinorVersion minor) (Just payload)

coerceLF1toLF2 :: LF1.Package -> LF2.Package
coerceLF1toLF2 package =
  fromRight
    (error "cannot coerce LF1 proto to LF2 proto")
    (fromByteString (BL.toStrict $ toLazyByteString package))