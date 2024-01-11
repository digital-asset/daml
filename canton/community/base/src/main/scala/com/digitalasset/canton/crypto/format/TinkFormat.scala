// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.format

import cats.syntax.either.*
import com.digitalasset.canton.crypto.{Fingerprint, HashAlgorithm}
import com.digitalasset.canton.serialization.{DefaultDeserializationError, DeserializationError}
import com.google.crypto.tink.{proto, *}
import com.google.protobuf.ByteString

class TinkKeyFingerprintException(message: String) extends RuntimeException(message)

object TinkKeyFormat {

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
