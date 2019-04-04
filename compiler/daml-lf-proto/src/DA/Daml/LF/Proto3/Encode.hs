-- Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Encode
  ( encodePayload
  ) where

import qualified Data.Text.Lazy as TL

import Da.DamlLf (ArchivePayload(..), ArchivePayloadSum(..))
import DA.Daml.LF.Ast
import qualified DA.Daml.LF.Proto3.EncodeV1 as EncodeV1
import qualified DA.Daml.LF.Proto3.EncodeVDev as EncodeVDev

encodePayload :: Package -> ArchivePayload
encodePayload package = case packageLfVersion package of
    V1 minor ->
        let payload = ArchivePayloadSumDamlLf1 (EncodeV1.encodePackage package)
        in  ArchivePayload (TL.pack (show minor)) (Just payload)
    VDev minor ->
        let payload = ArchivePayloadSumDamlLfDev (EncodeVDev.encodePackage package)
        in ArchivePayload (TL.fromStrict minor) (Just payload)
