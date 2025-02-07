// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.store

import cats.data.EitherT
import cats.syntax.either.*
import com.digitalasset.canton.crypto.kms.{Kms, KmsError, KmsKeyId}
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreError.{
  EncryptedPrivateStoreError,
  InvariantViolation,
}
import com.digitalasset.canton.crypto.store.db.StoredPrivateKey
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ByteString4096, ByteString6144}

import scala.concurrent.ExecutionContext

trait EncryptedCryptoPrivateStoreHelper {

  protected def encryptStoredKey(
      kms: Kms,
      wrapperKeyId: KmsKeyId,
      storedKey: StoredPrivateKey,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, StoredPrivateKey] =
    for {
      storedKeyData <- ByteString4096
        .create(storedKey.data)
        .leftMap[CryptoPrivateStoreError](err =>
          InvariantViolation(storedKey.id, s"plaintext private key does not adhere to bound: $err")
        )
        .toEitherT[FutureUnlessShutdown]
      encryptedData <- kms
        .encryptSymmetric(wrapperKeyId, storedKeyData)
        .leftMap[CryptoPrivateStoreError](kmsError => EncryptedPrivateStoreError(kmsError.show))
    } yield storedKey.copy(data = encryptedData.unwrap, wrapperKeyId = Some(wrapperKeyId.str))

  protected def decryptStoredKey(
      kms: Kms,
      storedKey: StoredPrivateKey,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, CryptoPrivateStoreError, StoredPrivateKey] =
    storedKey.wrapperKeyId match {
      case Some(keyId) =>
        for {
          storedKeyData <- ByteString6144
            .create(storedKey.data)
            .leftMap[CryptoPrivateStoreError](err =>
              InvariantViolation(
                storedKey.id,
                s"encrypted private key does not adhere to bound: $err",
              )
            )
            .toEitherT[FutureUnlessShutdown]
          decryptedData <- kms
            .decryptSymmetric(KmsKeyId(keyId), storedKeyData)
            .leftMap[CryptoPrivateStoreError](kmsError => EncryptedPrivateStoreError(kmsError.show))
        } yield storedKey.copy(data = decryptedData.unwrap, wrapperKeyId = None)
      case None =>
        EitherT.leftT[FutureUnlessShutdown, StoredPrivateKey](
          CryptoPrivateStoreError.FailedToReadKey(
            storedKey.id,
            "stored encrypted private key but wrapper key id is missing",
          )
        )
    }

  private[crypto] def checkWrapperKeyExistsOrCreateNewOne(
      kms: Kms,
      newWrapperKeyId: Option[KmsKeyId],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    newWrapperKeyId match {
      case Some(newKeyId) =>
        kms
          .keyExistsAndIsActive(newKeyId)
          .map(_ => newKeyId)
      case None =>
        for {
          kmsKeyId <- kms.generateSymmetricEncryptionKey()
          // Wait until the new symmetric key becomes active
          _ <- kms.keyExistsAndIsActive(kmsKeyId)
        } yield kmsKeyId
    }

}
