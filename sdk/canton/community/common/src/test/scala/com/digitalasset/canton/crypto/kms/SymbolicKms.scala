// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.kms

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.digitalasset.canton.config.CantonRequireTypes.String300
import com.digitalasset.canton.config.{KmsConfig, ProcessingTimeout}
import com.digitalasset.canton.crypto.provider.symbolic.SymbolicCrypto
import com.digitalasset.canton.crypto.{
  AsymmetricEncrypted,
  CryptoKey,
  CryptoPrivateStoreApi,
  Encrypted,
  EncryptionAlgorithmSpec,
  EncryptionKeyPair,
  EncryptionKeySpec,
  EncryptionPrivateKey,
  EncryptionPublicKey,
  KeyName,
  SigningAlgorithmSpec,
  SigningKeyPair,
  SigningKeySpec,
  SigningKeyUsage,
  SigningPrivateKey,
  SigningPublicKey,
  SymmetricKey,
}
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.{FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.collection.TrieMapUtil
import com.digitalasset.canton.util.{
  ByteString190,
  ByteString256,
  ByteString4096,
  ByteString6144,
  EitherTUtil,
}
import com.digitalasset.canton.version.HasToByteString
import com.google.protobuf.ByteString

import java.util.concurrent.atomic.AtomicInteger
import scala.collection.concurrent.TrieMap
import scala.concurrent.ExecutionContext

class SymbolicKms(
    private val crypto: SymbolicCrypto,
    override val config: KmsConfig,
    override val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
) extends Kms
    with NamedLogging {

  override type Config = KmsConfig

  override def name: String = "symbolic-kms"

  override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()

  // store all private keys that belong in the KMS
  private val storedPrivateKeyMap: TrieMap[KmsKeyId, CryptoKey] = TrieMap.empty
  // store public signing keys
  private val storedPublicSigningKeyMap: TrieMap[KmsKeyId, SigningPublicKey] = TrieMap.empty
  // store public encryption keys
  private val storedPublicEncryptionKeyMap: TrieMap[KmsKeyId, EncryptionPublicKey] = TrieMap.empty

  private val counter = new AtomicInteger(0)

  override protected def generateSigningKeyPairInternal(
      signingKeySpec: SigningKeySpec,
      name: Option[KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keys <- crypto.privateCrypto match {
        case api: CryptoPrivateStoreApi =>
          api
            .generateSigningKeypair(signingKeySpec, SigningKeyUsage.ProtocolOnly)(
              TraceContext.empty
            )
            .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err.show))
        case _ =>
          EitherT
            .leftT[FutureUnlessShutdown, SigningKeyPair](
              KmsError.KmsCreateKeyRequestError(
                "The selected crypto private store API does not allow exporting private keys"
              )
            )
            .leftWiden[KmsError]
      }
      kmsKeyId =
        KmsKeyId(String300.tryCreate(s"symbolic-kms-signing-key-${counter.getAndIncrement()}"))
      _ = TrieMapUtil
        .insertIfAbsent(
          storedPrivateKeyMap,
          kmsKeyId,
          keys.privateKey,
          () =>
            KmsError.KmsCreateKeyError(
              "Duplicate symbolic KMS signing private key: " + kmsKeyId.toString
            ),
        )
      _ = TrieMapUtil
        .insertIfAbsent(
          storedPublicSigningKeyMap,
          kmsKeyId,
          keys.publicKey,
          () =>
            KmsError.KmsCreateKeyError(
              "Duplicate symbolic KMS signing public key: " + kmsKeyId.toString
            ),
        )
    } yield kmsKeyId

  override protected def generateSymmetricEncryptionKeyInternal(
      name: Option[KeyName]
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    (for {
      key <- crypto.pureCrypto
        .generateSymmetricKey()
        .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err.show))
      kmsKeyId =
        KmsKeyId(
          String300.tryCreate(
            s"symbolic-kms-symmetric-encryption-key-${counter.getAndIncrement()}"
          )
        )
      _ = TrieMapUtil
        .insertIfAbsent(
          storedPrivateKeyMap,
          kmsKeyId,
          key,
          () =>
            KmsError.KmsCreateKeyError(
              "Duplicate symbolic KMS symmetric encryption key: " + kmsKeyId.toString
            ),
        )
    } yield kmsKeyId).toEitherT

  override protected def generateAsymmetricEncryptionKeyPairInternal(
      encryptionKeySpec: EncryptionKeySpec,
      name: Option[KeyName],
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsKeyId] =
    for {
      keys <- crypto.privateCrypto match {
        case api: CryptoPrivateStoreApi =>
          api
            .generateEncryptionKeypair(encryptionKeySpec)(TraceContext.empty)
            .leftMap[KmsError](err => KmsError.KmsCreateKeyError(err.show))
        case _ =>
          EitherT
            .leftT[FutureUnlessShutdown, EncryptionKeyPair](
              KmsError.KmsCreateKeyRequestError(
                "The selected crypto private store API does not allow exporting private keys"
              )
            )
            .leftWiden[KmsError]
      }
      kmsKeyId =
        KmsKeyId(
          String300.tryCreate(
            s"symbolic-kms-asymmetric-encryption-key-${counter.getAndIncrement()}"
          )
        )
      _ = TrieMapUtil
        .insertIfAbsent(
          storedPrivateKeyMap,
          kmsKeyId,
          keys.privateKey,
          () =>
            KmsError.KmsCreateKeyError(
              "Duplicate symbolic KMS encryption private key: " + kmsKeyId.toString
            ),
        )
      _ = TrieMapUtil
        .insertIfAbsent(
          storedPublicEncryptionKeyMap,
          kmsKeyId,
          keys.publicKey,
          () =>
            KmsError.KmsCreateKeyError(
              "Duplicate symbolic KMS encryption public key: " + kmsKeyId.toString
            ),
        )
    } yield kmsKeyId

  override protected def getPublicSigningKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsSigningPublicKey] =
    for {
      pubKey <- storedPublicSigningKeyMap
        .get(keyId)
        .toRight[KmsError](
          KmsError.KmsGetPublicKeyError(keyId, "public signing key does not exist")
        )
        .toEitherT[FutureUnlessShutdown]
      kmsPubKey <- EitherT.rightT[FutureUnlessShutdown, KmsError](
        KmsSigningPublicKey
          .createSymbolic(pubKey.key, pubKey.keySpec)
      )
    } yield kmsPubKey

  override protected def getPublicEncryptionKeyInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, KmsEncryptionPublicKey] =
    for {
      pubKey <- storedPublicEncryptionKeyMap
        .get(keyId)
        .toRight[KmsError](
          KmsError.KmsGetPublicKeyError(keyId, "public encryption key does not exist")
        )
        .toEitherT[FutureUnlessShutdown]
      kmsPubKey <- EitherT.rightT[FutureUnlessShutdown, KmsError](
        KmsEncryptionPublicKey
          .createSymbolic(pubKey.key, pubKey.keySpec)
      )
    } yield kmsPubKey

  override protected def keyExistsAndIsActiveInternal(
      keyId: KmsKeyId
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] =
    EitherTUtil.condUnitET[FutureUnlessShutdown](
      storedPrivateKeyMap.contains(keyId),
      KmsError.KmsCannotFindKeyError(keyId, "cannot find key in KMS store"),
    )

  override protected def encryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString6144] =
    storedPrivateKeyMap
      .get(keyId) match {
      case Some(key: SymmetricKey) =>
        crypto.pureCrypto
          .encryptSymmetricWith(SymbolicKmsMessage(data.unwrap), key)
          .toEitherT[FutureUnlessShutdown]
          .transform {
            case Left(err) => Left(KmsError.KmsEncryptError(keyId, err.show))
            case Right(enc) =>
              ByteString6144
                .create(enc.ciphertext)
                .leftMap(err =>
                  KmsError
                    .KmsEncryptError(
                      keyId,
                      s"generated ciphertext does not adhere to bound: $err)",
                    )
                )
          }
      case None =>
        EitherT.leftT(KmsError.KmsEncryptError(keyId, "KMS key does not exists"))
      case _ =>
        EitherT.leftT(KmsError.KmsEncryptError(keyId, "KMS key is not a symmetric key"))
    }

  override protected def decryptSymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString6144,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString4096] =
    storedPrivateKeyMap
      .get(keyId) match {
      case Some(key: SymmetricKey) =>
        val encryptedData = Encrypted.fromByteString[SymbolicKmsMessage](data.unwrap)
        for {
          decryptedData <- crypto.pureCrypto
            .decryptWith[SymbolicKmsMessage](encryptedData, key)(
              SymbolicKmsMessage.fromByteString(_)
            )
            .toEitherT[FutureUnlessShutdown]
            .transform {
              case Left(err) => Left(KmsError.KmsDecryptError(keyId, err.show))
              case Right(dec) =>
                ByteString4096
                  .create(dec.bytes)
                  .leftMap[KmsError](err =>
                    KmsError.KmsDecryptError(
                      keyId,
                      s"plaintext does not adhere to bound: $err)",
                    )
                  )
            }
        } yield decryptedData
      case None =>
        EitherT.leftT(KmsError.KmsDecryptError(keyId, "KMS key does not exists"))
      case _ =>
        EitherT.leftT(KmsError.KmsDecryptError(keyId, "KMS key is not a symmetric key"))
    }

  override protected def decryptAsymmetricInternal(
      keyId: KmsKeyId,
      data: ByteString256,
      encryptionAlgorithmSpec: EncryptionAlgorithmSpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString190] =
    storedPrivateKeyMap
      .get(keyId) match {
      case Some(key: EncryptionPrivateKey) =>
        for {
          decryptedData <- crypto.pureCrypto
            .decryptWith[SymbolicKmsMessage](
              AsymmetricEncrypted(data.unwrap, encryptionAlgorithmSpec, key.id),
              key,
            )(
              SymbolicKmsMessage.fromByteString(_)
            )
            .toEitherT[FutureUnlessShutdown]
            .transform {
              case Left(err) => Left(KmsError.KmsDecryptError(keyId, err.show))
              case Right(dec) =>
                ByteString190
                  .create(dec.bytes)
                  .leftMap[KmsError](err =>
                    KmsError.KmsDecryptError(
                      keyId,
                      s"plaintext does not adhere to bound: $err)",
                    )
                  )
            }
        } yield decryptedData
      case None =>
        EitherT.leftT(KmsError.KmsDecryptError(keyId, "KMS key does not exists"))
      case _ =>
        EitherT.leftT(
          KmsError.KmsDecryptError(keyId, "KMS key is not a private encryption key")
        )
    }

  override protected def signInternal(
      keyId: KmsKeyId,
      data: ByteString4096,
      signingAlgorithmSpec: SigningAlgorithmSpec,
      signingKeySpec: SigningKeySpec,
  )(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, ByteString] =
    storedPrivateKeyMap
      .get(keyId) match {
      case Some(key: SigningPrivateKey) =>
        // symbolic KMS is only used for testing so it's fine to use directly the usage coming from the signing key
        crypto.pureCrypto
          .signBytes(data.unwrap, key, key.usage)
          .leftMap[KmsError](err => KmsError.KmsSignError(keyId, err.show))
          .map(_.unwrap)
          .toEitherT[FutureUnlessShutdown]
      case None =>
        EitherT.leftT(KmsError.KmsSignError(keyId, "KMS signing key does not exist"))
      case _ =>
        EitherT.leftT(KmsError.KmsSignError(keyId, "KMS key is not a signing key"))
    }

  override protected def deleteKeyInternal(keyId: KmsKeyId)(implicit
      ec: ExecutionContext,
      tc: TraceContext,
  ): EitherT[FutureUnlessShutdown, KmsError, Unit] = {
    storedPublicSigningKeyMap.remove(keyId).discard
    storedPublicEncryptionKeyMap.remove(keyId).discard
    storedPrivateKeyMap.remove(keyId) match {
      case Some(_) => EitherT.rightT[FutureUnlessShutdown, KmsError](())
      case None =>
        EitherT.leftT[FutureUnlessShutdown, Unit](
          KmsError.KmsDeleteKeyError(keyId, "invalid key id")
        )
    }
  }

  override protected def withRetries[T](
      description: String,
      checkKeyCreation: Boolean,
  )(
      task: => EitherT[FutureUnlessShutdown, KmsError, T]
  )(implicit ec: ExecutionContext, tc: TraceContext): EitherT[FutureUnlessShutdown, KmsError, T] =
    task

  override def onClosed(): Unit = LifeCycle.close(crypto)(logger)

  private case class SymbolicKmsMessage(bytes: ByteString) extends HasToByteString {
    override def toByteString: ByteString = bytes
  }

  private object SymbolicKmsMessage {
    def fromByteString(bytes: ByteString): Either[DeserializationError, SymbolicKmsMessage] = Right(
      SymbolicKmsMessage(bytes)
    )
  }
}
