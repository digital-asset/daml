-- Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0

module DA.Daml.LF.Proto3.Archive.Decode
  ( decodeArchive
  , decodeArchivePackageId
  , decodePackage
  , ArchiveError(..)
  , DecodingMode(..)
  ) where

import Control.Lens (over, _Left)
import "cryptonite" Crypto.Hash qualified as Crypto
import Com.Daml.DamlLfDev.DamlLf qualified as ProtoLF
import Control.Monad
import DA.Pretty
import DA.Daml.LF.Ast qualified as LF
import DA.Daml.LF.Proto3.Decode qualified as Decode
import DA.Daml.LF.Proto3.Util (encodeHash)
import Data.ByteArray qualified as BA
import Data.ByteString qualified as BS
import Data.Int
import Data.Text qualified as T
import Data.Text.Lazy qualified as TL
import Proto3.Suite qualified as Proto

data ArchiveError
    = ProtobufError !String
    | UnknownHashFunction !Int32
    | HashMismatch !T.Text !T.Text
  deriving (Eq, Show)

-- | Mode in which to decode the DALF. Currently, this only decides whether
-- to rewrite occurrences of `PRSelf` with `PRImport packageId`.
data DecodingMode
    = DecodeAsMain
      -- ^ Keep occurrences of `PRSelf` as is.
    | DecodeAsDependency
      -- ^ Replace `PRSelf` with `PRImport packageId`, where `packageId` is
      -- the id of the package being decoded.
    deriving (Eq, Show)

-- | Decode an LF archive, returning the package-id and the package
decodeArchive :: DecodingMode -> BS.ByteString -> Either ArchiveError (LF.PackageId, LF.Package)
decodeArchive mode bytes = do
    (packageId, payloadBytes) <- decodeArchiveHeader bytes
    package <- decodePackage mode packageId payloadBytes
    return (packageId, package)

-- | Decode an LF archive payload, returning the package
-- Used to decode a BS returned from the PackageService ledger API
decodePackage :: DecodingMode -> LF.PackageId -> BS.ByteString -> Either ArchiveError LF.Package
decodePackage mode packageId payloadBytes = do
    let selfPackageRef = case mode of
            DecodeAsMain -> LF.PRSelf
            DecodeAsDependency -> LF.PRImport packageId
    payload <- over _Left (ProtobufError . show) $ Proto.fromByteString payloadBytes
    over _Left (ProtobufError. show) $ Decode.decodePayload packageId selfPackageRef payload

-- | Decode an LF archive header, returning the package-id and the payload
decodeArchiveHeader :: BS.ByteString -> Either ArchiveError (LF.PackageId, BS.ByteString)
decodeArchiveHeader bytes = do
    archive <- over _Left (ProtobufError . show) $ Proto.fromByteString bytes
    let payloadBytes = ProtoLF.archivePayload archive
    let archiveHash = TL.toStrict (ProtoLF.archiveHash archive)

    computedHash <- case ProtoLF.archiveHashFunction archive of
      Proto.Enumerated (Right ProtoLF.HashFunctionSHA256) ->
        Right $ encodeHash (BA.convert (Crypto.hash @_ @Crypto.SHA256 payloadBytes) :: BS.ByteString)
      Proto.Enumerated (Left idx) ->
        Left (UnknownHashFunction idx)

    when (computedHash /= archiveHash) $
      Left (HashMismatch archiveHash computedHash)
    let packageId = LF.PackageId archiveHash
    pure (packageId, payloadBytes)

-- | Decode an LF archive, returning the package-id
decodeArchivePackageId :: BS.ByteString -> Either ArchiveError LF.PackageId
decodeArchivePackageId = fmap fst . decodeArchiveHeader

instance Pretty ArchiveError where
  pPrint =
    \case
      ProtobufError e -> "Protobuf error: " <> pretty e
      UnknownHashFunction i ->
        "Unknown hash function with identifier " <> pretty i
      HashMismatch h1 h2 ->
        vsep
          [ "Computed package hash doesn't match with given package hash: "
          , label_ "Package hash: " $ pretty h1
          , label_ "Computed hash: " $ pretty h2
          ]
