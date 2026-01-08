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
import qualified Data.Map                 as M
import           Data.Int (Int32)

encodePayload :: Package -> ArchivePayload
encodePayload package = case packageLfVersion package of
    (Version V2 minor) ->
        let payload = ArchivePayloadSumDamlLf2 $ BSL.toStrict $ Proto.toLazyByteString (EncodeV2.encodePackage package)
        in ArchivePayload
           { archivePayloadMinor = TL.pack $ renderMinorVersion minor
           , archivePayloadPatch = minorToPatch minor
           , archivePayloadSum = Just payload
           }

minorToPatch :: MinorVersion -> Int32
minorToPatch minor = M.findWithDefault 0 minor patchMap

patchMap :: M.Map MinorVersion Int32
patchMap = M.empty
