-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Archive.Encode
  ( encodeArchive
  , encodeArchiveLazy
  , encodeArchiveAndHash
  , encodePackageHash
  ) where

import "cryptonite" Crypto.Hash qualified as Crypto
import Com.Daml.DamlLfDev.DamlLf qualified as ProtoLF
import DA.Daml.LF.Ast qualified           as LF
import DA.Daml.LF.Proto3.Encode qualified as Encode
import DA.Daml.LF.Proto3.Util (encodeHash)
import Data.ByteArray qualified           as BA
import Data.ByteString qualified          as BS
import Data.ByteString.Lazy qualified     as BSL
import Data.Text.Lazy qualified           as TL
import Proto3.Suite qualified             as Proto

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
