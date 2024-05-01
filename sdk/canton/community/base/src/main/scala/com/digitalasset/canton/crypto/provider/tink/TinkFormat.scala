// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import cats.syntax.either.*
import com.digitalasset.canton.crypto.CryptoPureApiError.KeyParseAndValidateError
import com.digitalasset.canton.crypto.{Fingerprint, HashAlgorithm, JavaKeyConversionError}
import com.digitalasset.canton.serialization.{
  DefaultDeserializationError,
  DeserializationError,
  ProtoConverter,
}
import com.google.crypto.tink.*
import com.google.crypto.tink.proto.{KeyData, KeyStatusType, Keyset, OutputPrefixType}
import com.google.protobuf.ByteString

class TinkKeyFingerprintException(message: String) extends RuntimeException(message)

object TinkKeyFormat {

  private[crypto] val tinkOuputPrefixSizeInBytes = 5

  private[crypto] def keyDataWithRawOutputPrefix(
      keyData: KeyData
  ): Either[JavaKeyConversionError, KeysetHandle] = {
    val keyId = 0
    val key = Keyset.Key
      .newBuilder()
      .setKeyData(keyData)
      .setStatus(KeyStatusType.ENABLED)
      .setKeyId(keyId)
      // Use RAW because we don't have the same key id prefix
      .setOutputPrefixType(OutputPrefixType.RAW)
      .build()
    val keyset = Keyset.newBuilder().setPrimaryKeyId(keyId).addKey(key).build()
    TinkKeyFormat
      .deserializeHandle(keyset.toByteString)
      .leftMap(err => JavaKeyConversionError.InvalidKey(s"Failed to deserialize keyset: $err"))
  }

  /** Extract the first key from the keyset. In Canton we only deal with Tink keysets of size 1. */
  private[crypto] def getFirstKey(
      keysetHandle: KeysetHandle
  ): Either[JavaKeyConversionError, Keyset.Key] =
    for {
      // No other way to access the public key directly than going via protobuf
      keysetProto <- ProtoConverter
        .protoParser(Keyset.parseFrom)(TinkKeyFormat.serializeHandle(keysetHandle))
        .leftMap(err =>
          JavaKeyConversionError.InvalidKey(s"Failed to parser tink keyset proto: $err")
        )
      key <- Either.cond(
        keysetProto.getKeyCount == 1,
        keysetProto.getKey(0),
        JavaKeyConversionError.InvalidKey(
          s"Not exactly one key in the keyset, but ${keysetProto.getKeyCount} keys."
        ),
      )
    } yield key

  private[crypto] def convertKeysetHandleToRawOutputPrefix(
      keysetHandle: KeysetHandle
  ): Either[KeyParseAndValidateError, KeysetHandle] =
    if (
      keysetHandle.getKeysetInfo
        .getKeyInfo(0)
        .getOutputPrefixType == OutputPrefixType.TINK
    )
      (for {
        keyData <- getFirstKey(keysetHandle)
        convertedKeysetHandle <- keyDataWithRawOutputPrefix(keyData.getKeyData)
      } yield convertedKeysetHandle)
        .leftMap(err =>
          KeyParseAndValidateError(s"Cannot convert to RAW output prefix: ${err.show}")
        )
    else Right(keysetHandle)

  /** We execute `cryptoFunc()` once (i.e. either a decryption or a signature verification) and if it fails
    * we try to check if the data (i.e.ciphertext or signature) has a prefix that starts
    * with \x01 that could indicate it is encoded with a Tink prefix. If it's the case we strip the Tink prefix and
    * try again. This is needed to ensure backwards compatibility with ciphertexts/signatures produced by previously
    * generated Tink keys.
    *
    * @param cryptoFunc crypto function to execute (either a decryption or a signature verification)
    * @param data the data to check for a Tink prefix (can be a ciphertext or a signature)
    */
  private[crypto] def retryIfTinkPrefix[E, A](data: ByteString)(
      cryptoFunc: ByteString => Either[E, A]
  ): Either[E, A] = {

    def hasTinkOutputPrefix(data: ByteString): Boolean = data.byteAt(0) == 0x01

    def stripTinkOutputPrefix(data: ByteString): ByteString =
      data.substring(tinkOuputPrefixSizeInBytes)

    cryptoFunc(data).leftFlatMap(err =>
      if (hasTinkOutputPrefix(data)) cryptoFunc(stripTinkOutputPrefix(data)) else Left(err)
    )
  }

  /** Computes the fingerprint for a Tink key based on the key bytes in the keyset. */
  private[crypto] def fingerprint(
      handle: KeysetHandle,
      hashAlgorithm: HashAlgorithm,
  ): Either[String, Fingerprint] = {

    @SuppressWarnings(Array("org.wartremover.warts.Var"))
    var printVar: Option[Fingerprint] = None

    val result = Either.catchOnly[TinkKeyFingerprintException] {
      CleartextKeysetHandle.write(
        handle,
        new KeysetWriter {
          override def write(keyset: proto.Keyset): Unit = {
            if (keyset.getKeyCount != 1)
              throw new TinkKeyFingerprintException(
                "All Canton-generated keysets should contain exactly one key"
              )

            val key = keyset.getKeyList.get(0)
            val keyData = key.getKeyData
            if (keyData.getKeyMaterialType != proto.KeyData.KeyMaterialType.ASYMMETRIC_PUBLIC)
              throw new TinkKeyFingerprintException("Keyset contains non-public key")

            val fp = Fingerprint.create(keyData.getValue)
            printVar = Some(fp)
          }

          override def write(keyset: proto.EncryptedKeyset): Unit =
            throw new TinkKeyFingerprintException(
              "Writing encrypted keysets should not happen; all Canton keysets are unencrypted"
            )
        },
      )
    }

    result
      .leftMap(_.getMessage)
      .flatMap(_ => printVar.toRight("Fingerprint not available for reading"))
  }

  private[crypto] def serializeHandle(handle: KeysetHandle): ByteString = {
    val out = ByteString.newOutput()
    CleartextKeysetHandle.write(handle, BinaryKeysetWriter.withOutputStream(out))
    out.toByteString
  }

  private[crypto] def deserializeHandle(
      bytes: ByteString
  ): Either[DeserializationError, KeysetHandle] =
    Either
      .catchNonFatal(
        CleartextKeysetHandle.read(BinaryKeysetReader.withInputStream(bytes.newInput()))
      )
      .leftMap(err => DefaultDeserializationError(s"Failed to deserialize tink keyset: $err"))

}
