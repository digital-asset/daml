-- Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Archive.Encode
  ( encodeArchive
  , encodeArchiveLazy
  , encodeArchiveAndHash
  , encodePackageHash
  ) where

import qualified "cryptonite" Crypto.Hash as Crypto
import qualified Com.Daml.DamlLfDev.DamlLf as ProtoLF
import qualified DA.Daml.LF.Ast           as LF
import qualified DA.Daml.LF.Proto3.Encode as Encode
import DA.Daml.LF.Proto3.Util (encodeHash)
import qualified Data.ByteArray           as BA
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Lazy     as BSL
import qualified Data.Text.Lazy           as TL
import qualified Proto3.Suite             as Proto

-- | Encode a LFv1 package payload into a Daml-LF archive using the default
-- hash function.
encodeArchiveLazy :: LF.Package -> BSL.ByteString
encodeArchiveLazy = fst . encodeArchiveAndHash

encodePackageHash :: LF.Package -> LF.PackageId
encodePackageHash = snd . encodeArchiveAndHash

encodeArchiveAndHash :: LF.Package -> (BSL.ByteString, LF.PackageId)
encodeArchiveAndHash package =
    let payload = BSL.toStrict $ Proto.toLazyByteString $ Encode.encodePayload package
        hash = encodeHash (BA.convert (Crypto.hash @_ @Crypto.SHA256 payload) :: BS.ByteString)
        archive =
          ProtoLF.Archive
          { ProtoLF.archivePayload = payload
          , ProtoLF.archiveHash    = TL.fromStrict hash
          , ProtoLF.archiveHashFunction = Proto.Enumerated (Right ProtoLF.HashFunctionSHA256)
          }
    in (Proto.toLazyByteString archive, LF.PackageId hash)

encodeArchive :: LF.Package -> BS.ByteString
encodeArchive = BSL.toStrict . encodeArchiveLazy
