-- Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
-- SPDX-License-Identifier: Apache-2.0


-- | Utilities for working with DAML-LF protobuf archives
module DA.Daml.LF.Proto3.Archive
  ( decodeArchive
  , decodeArchivePackageId
  , decodePackage
  , encodeArchive
  , encodeArchiveLazy
  , encodeArchiveAndHash
  , encodePackageHash
  , ArchiveError(..)
  , DecodingMode(..)
  ) where

import           Control.Lens             (over, _Left)
import qualified "cryptonite" Crypto.Hash as Crypto
import qualified Com.Daml.DamlLf.Archive as ProtoArchive
import Control.Monad
import Data.List
import           DA.Pretty
import qualified DA.Daml.LF.Ast           as LF
import qualified DA.Daml.LF.Proto3.Decode as Decode
import qualified DA.Daml.LF.Proto3.Encode as Encode
import qualified Data.ByteArray           as BA
import qualified Data.ByteString          as BS
import qualified Data.ByteString.Lazy     as BSL
import Data.Int
import qualified Data.Text                as T
import qualified Data.Text.Lazy           as TL
import qualified Numeric
import qualified Proto3.Suite             as Proto

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
    over _Left (ProtobufError. show) $ Decode.decodePayload selfPackageRef payload

-- | Decode an LF archive header, returning the package-id and the payload
decodeArchiveHeader :: BS.ByteString -> Either ArchiveError (LF.PackageId, BS.ByteString)
decodeArchiveHeader bytes = do
    archive <- over _Left (ProtobufError . show) $ Proto.fromByteString bytes
    let payloadBytes = ProtoArchive.archivePayload archive
    let archiveHash = TL.toStrict (ProtoArchive.archiveHash archive)

    computedHash <- case ProtoArchive.archiveHashFunction archive of
      Proto.Enumerated (Right ProtoArchive.Archive_HashFunctionSHA256) ->
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

-- | Encode a LFv1 package payload into a DAML-LF archive using the default
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
          ProtoArchive.Archive
          { ProtoArchive.archivePayload = payload
          , ProtoArchive.archiveHash    = TL.fromStrict hash
          , ProtoArchive.archiveHashFunction = Proto.Enumerated (Right ProtoArchive.Archive_HashFunctionSHA256)
          }
    in (Proto.toLazyByteString archive, LF.PackageId hash)

encodeArchive :: LF.Package -> BS.ByteString
encodeArchive = BSL.toStrict . encodeArchiveLazy

-- | Encode the hash bytes of the payload in the canonical
-- lower-case ascii7 hex presentation.
encodeHash :: BS.ByteString -> T.Text
encodeHash = T.pack . reverse . foldl' toHex [] . BS.unpack
  where
    toHex xs c =
      case Numeric.showHex c "" of
        [n1, n2] -> n2 : n1 : xs
        [n2]     -> n2 : '0' : xs
        _        -> error "impossible: showHex returned [] on Word8"

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
