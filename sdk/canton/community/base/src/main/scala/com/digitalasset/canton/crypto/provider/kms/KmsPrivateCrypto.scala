// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.SigningKeyUsage.nonEmptyIntersection
import com.digitalasset.canton.crypto.kms.{Kms, KmsKeyId}
import com.digitalasset.canton.crypto.store.KmsMetadataStore.KmsMetadata
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, KmsCryptoPrivateStore}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ByteString256, ByteString4096}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

trait KmsPrivateCrypto extends CryptoPrivateApi with FlagCloseable {
  type KmsType <: Kms

  implicit val ec: ExecutionContext

  protected val kms: KmsType
  protected def privateStore: KmsCryptoPrivateStore
  protected def publicStore: CryptoPublicStore

  def defaultSigningAlgorithmSpec: SigningAlgorithmSpec
  def defaultSigningKeySpec: SigningKeySpec
  def defaultEncryptionKeySpec: EncryptionKeySpec

  private def getPublicSigningKey(keyId: KmsKeyId)(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    kms
      .getPublicSigningKey(keyId)
      .leftMap[SigningKeyGenerationError](err =>
        SigningKeyGenerationError.GeneralKmsError(err.show)
      )

  /** This function and [[registerEncryptionKey]] is used to register a key directly to the store (i.e. pre-generated)
    * and bypass the default key generation procedure.
    * As we are overriding the usual way to create new keys, by using pre-generated ones,
    * we need to add their public material to a node's public store.
    */
  def registerSigningKey(
      keyId: KmsKeyId,
      usage: NonEmpty[Set[SigningKeyUsage]],
      keyName: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    for {
      publicKeyNoUsage <- getPublicSigningKey(keyId)
      publicKey = publicKeyNoUsage
        .copy(usage = SigningKeyUsage.addProofOfOwnership(usage))(migrated = false)
      _ <- EitherT.right(publicStore.storeSigningKey(publicKey, keyName))
      _ = privateStore.storeKeyMetadata(
        KmsMetadata(publicKey.id, keyId, KeyPurpose.Signing, Some(publicKey.usage))
      )
    } yield publicKey

  def generateSigningKey(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    for {
      keyId <- kms
        .generateSigningKeyPair(keySpec, name)
        .leftMap[SigningKeyGenerationError](err =>
          SigningKeyGenerationError.GeneralKmsError(err.show)
        )
      _ <- kms
        .keyExistsAndIsActive(keyId)
        .leftMap[SigningKeyGenerationError](err =>
          SigningKeyGenerationError.GeneralKmsError(err.show)
        )
      publicKeyNoUsage <- kms
        .getPublicSigningKey(keyId)
        .leftMap[SigningKeyGenerationError](err =>
          SigningKeyGenerationError.GeneralKmsError(err.show)
        )
      publicKey = publicKeyNoUsage
        .copy(usage = SigningKeyUsage.addProofOfOwnership(usage))(migrated = false)
      _ = privateStore.storeKeyMetadata(
        KmsMetadata(publicKey.id, keyId, KeyPurpose.Signing, Some(publicKey.usage))
      )
    } yield publicKey

  protected[crypto] def signBytes(
      bytes: ByteString,
      signingKeyId: Fingerprint,
      usage: NonEmpty[Set[SigningKeyUsage]],
      signingAlgorithmSpec: SigningAlgorithmSpec,
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, SigningError, Signature] =
    for {
      metadata <- EitherT.right(
        privateStore
          .getKeyMetadata(signingKeyId)
      )
      signature <- metadata match {
        case Some(KmsMetadata(_, kmsKeyId, _, _)) =>
          ByteString4096.create(bytes) match {
            case Left(err) =>
              EitherT
                .leftT[FutureUnlessShutdown, Signature](
                  SigningError.InvariantViolation(s"data to sign does not adhere to bound: $err")
                )
                .leftWiden[SigningError]
            case Right(bytes) =>
              for {
                pubKey <- publicStore
                  .signingKey(signingKeyId)
                  .toRight[SigningError](SigningError.UnknownSigningKey(signingKeyId))
                _ <- EitherT.cond[FutureUnlessShutdown](
                  nonEmptyIntersection(usage, pubKey.usage),
                  (),
                  SigningError.InvalidSigningKey(
                    s"Signing key ${pubKey.fingerprint} [${pubKey.usage}] is not valid for usage $usage"
                  ),
                )
                signatureRaw <- kms
                  .sign(kmsKeyId, bytes, signingAlgorithmSpec)
                  .leftMap[SigningError](err => SigningError.FailedToSign(err.show))
              } yield new Signature(
                SignatureFormat.Raw,
                signatureRaw,
                signingKeyId,
                Some(signingAlgorithmSpec),
              )

          }
        case None =>
          EitherT
            .leftT[FutureUnlessShutdown, Signature](SigningError.UnknownSigningKey(signingKeyId))
            .leftWiden[SigningError]
      }
    } yield signature

  private def getPublicEncryptionKey(
      keyId: KmsKeyId
  )(implicit
      tc: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    kms
      .getPublicEncryptionKey(keyId)
      .leftMap[EncryptionKeyGenerationError](err =>
        EncryptionKeyGenerationError.GeneralKmsError(err.show)
      )

  def registerEncryptionKey(
      keyId: KmsKeyId,
      keyName: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      publicKey <- getPublicEncryptionKey(keyId)
      _ <- EitherT.right(publicStore.storeEncryptionKey(publicKey, keyName))
      _ = privateStore.storeKeyMetadata(KmsMetadata(publicKey.id, keyId, KeyPurpose.Encryption))
    } yield publicKey

  override def generateEncryptionKey(
      keySpec: EncryptionKeySpec,
      name: Option[KeyName],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionPublicKey] =
    for {
      keyId <- kms
        .generateAsymmetricEncryptionKeyPair(keySpec, name)
        .leftMap[EncryptionKeyGenerationError](err =>
          EncryptionKeyGenerationError.GeneralKmsError(err.show)
        )
      _ <- kms
        .keyExistsAndIsActive(keyId)
        .leftMap[EncryptionKeyGenerationError](err =>
          EncryptionKeyGenerationError.GeneralKmsError(err.show)
        )
      publicKey <- kms
        .getPublicEncryptionKey(keyId)
        .leftMap[EncryptionKeyGenerationError](err =>
          EncryptionKeyGenerationError.GeneralKmsError(err.show)
        )
      _ = privateStore.storeKeyMetadata(KmsMetadata(publicKey.id, keyId, KeyPurpose.Encryption))
    } yield publicKey

  override def decrypt[M](encrypted: AsymmetricEncrypted[M])(
      deserialize: ByteString => Either[DeserializationError, M]
  )(implicit tc: TraceContext): EitherT[FutureUnlessShutdown, DecryptionError, M] =
    for {
      metadata <- EitherT.right(
        privateStore
          .getKeyMetadata(encrypted.encryptedFor)
      )
      plaintext <- metadata match {
        case Some(KmsMetadata(_, kmsKeyId, _, _)) =>
          for {
            ciphertext <- ByteString256
              .create(encrypted.ciphertext)
              .toEitherT[FutureUnlessShutdown]
              .leftMap(err =>
                DecryptionError.InvariantViolation(
                  s"data to decrypt does not adhere to bound: $err"
                )
              )
            plaintext <- kms
              .decryptAsymmetric(kmsKeyId, ciphertext, encrypted.encryptionAlgorithmSpec)
              .leftMap[DecryptionError](err => DecryptionError.FailedToDecrypt(err.show))
            message <- EitherT
              .fromEither[FutureUnlessShutdown](deserialize(plaintext.unwrap))
              .leftMap[DecryptionError](DecryptionError.FailedToDeserialize.apply)
          } yield message
        case None =>
          EitherT
            .leftT[FutureUnlessShutdown, M](
              DecryptionError.UnknownEncryptionKey(encrypted.encryptedFor)
            )
            .leftWiden[DecryptionError]
      }
    } yield plaintext

  override def onClosed(): Unit =
    LifeCycle.close(kms)(logger)

}
