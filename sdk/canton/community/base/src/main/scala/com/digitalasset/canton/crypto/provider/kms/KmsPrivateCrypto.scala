// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.CryptoFactory.CryptoStoresAndSchemes
import com.digitalasset.canton.crypto.kms.driver.v1.DriverKms
import com.digitalasset.canton.crypto.kms.{Kms, KmsKeyId}
import com.digitalasset.canton.crypto.store.KmsMetadataStore.KmsMetadata
import com.digitalasset.canton.crypto.store.{CryptoPublicStore, KmsCryptoPrivateStore}
import com.digitalasset.canton.health.{
  ComponentHealthState,
  CompositeHealthElement,
  HealthQuasiComponent,
}
import com.digitalasset.canton.lifecycle.{FlagCloseable, FutureUnlessShutdown, LifeCycle}
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.serialization.DeserializationError
import com.digitalasset.canton.tracing.TraceContext
import com.digitalasset.canton.util.{ByteString256, ByteString4096}
import com.google.protobuf.ByteString

import scala.concurrent.ExecutionContext

class KmsPrivateCrypto(
    kms: Kms,
    privateStore: KmsCryptoPrivateStore,
    publicStore: CryptoPublicStore,
    override val defaultSigningAlgorithmSpec: SigningAlgorithmSpec,
    override val defaultSigningKeySpec: SigningKeySpec,
    override val defaultEncryptionKeySpec: EncryptionKeySpec,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(implicit ec: ExecutionContext)
    extends CryptoPrivateApi
    with NamedLogging
    with FlagCloseable
    with CompositeHealthElement[String, HealthQuasiComponent] {

  override def name: String = "kms-private-crypto"

  setDependency("kms", kms)

  override protected def combineDependentStates: ComponentHealthState = kms.getState

  override protected def initialHealthState: ComponentHealthState =
    ComponentHealthState.NotInitializedState

  private def getPublicSigningKey(
      keyId: KmsKeyId
  )(implicit
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
      publicKeyNoPoO <- getPublicSigningKey(keyId)
      publicKey = publicKeyNoPoO
        .copy(usage = SigningKeyUsage.addProofOfOwnership(usage))(publicKeyNoPoO.migrated)
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
      publicKeyNoPoO <- kms
        .getPublicSigningKey(keyId)
        .leftMap[SigningKeyGenerationError](err =>
          SigningKeyGenerationError.GeneralKmsError(err.show)
        )
      publicKey = publicKeyNoPoO
        .copy(usage = SigningKeyUsage.addProofOfOwnership(usage))(publicKeyNoPoO.migrated)
      _ = privateStore.storeKeyMetadata(
        KmsMetadata(publicKey.id, keyId, KeyPurpose.Signing, Some(publicKey.usage))
      )
    } yield publicKey

  def signBytes(
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
      signatureFormat = SignatureFormat.fromSigningAlgoSpec(signingAlgorithmSpec)
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
                // This assumes that the public key's usage and key spec is the same as the private key used for signing
                pubKey <- publicStore
                  .signingKey(signingKeyId)
                  .toRight[SigningError](SigningError.UnknownSigningKey(signingKeyId))
                _ <- CryptoKeyValidation
                  .ensureUsage(
                    usage,
                    pubKey.usage,
                    pubKey.id,
                    _ =>
                      SigningError.InvalidKeyUsage(pubKey.id, pubKey.usage.forgetNE, usage.forgetNE),
                  )
                  .toEitherT[FutureUnlessShutdown]
                signatureRaw <- kms
                  .sign(kmsKeyId, bytes, signingAlgorithmSpec, pubKey.keySpec)
                  .leftMap[SigningError](err => SigningError.FailedToSign(err.show))
              } yield Signature.create(
                signatureFormat,
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

object KmsPrivateCrypto {

  def create(
      kms: Kms,
      storesAndSchemes: CryptoStoresAndSchemes,
      kmsCryptoPrivateStore: KmsCryptoPrivateStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Either[String, KmsPrivateCrypto] = kms match {

    // For a Driver KMS we explicitly check that the driver supports the required schemes
    case driverKms: DriverKms =>
      for {
        _ <- Either.cond(
          driverKms.supportedSigningKeySpecs
            .contains(storesAndSchemes.signingKeySpec),
          (),
          s"Selected signing key spec ${storesAndSchemes.signingKeySpec} not supported by driver: ${driverKms.supportedSigningKeySpecs}",
        )
        _ <- Either.cond(
          driverKms.supportedSigningAlgoSpecs
            .contains(storesAndSchemes.signingAlgorithmSpec),
          (),
          s"Selected signing algorithm spec ${storesAndSchemes.signingAlgorithmSpec} not supported by driver: ${driverKms.supportedSigningAlgoSpecs}",
        )
        _ <- Either.cond(
          driverKms.supportedEncryptionKeySpecs
            .contains(storesAndSchemes.encryptionKeySpec),
          (),
          s"Selected encryption key spec ${storesAndSchemes.encryptionKeySpec} not supported by driver: ${driverKms.supportedEncryptionKeySpecs}",
        )
        _ <- Either.cond(
          driverKms.supportedEncryptionAlgoSpecs
            .contains(storesAndSchemes.encryptionAlgorithmSpec),
          (),
          s"Selected encryption algorithm spec ${storesAndSchemes.encryptionAlgorithmSpec} not supported by driver: ${driverKms.supportedEncryptionAlgoSpecs}",
        )
      } yield new KmsPrivateCrypto(
        kms,
        kmsCryptoPrivateStore,
        storesAndSchemes.cryptoPublicStore,
        storesAndSchemes.signingAlgorithmSpec,
        storesAndSchemes.signingKeySpec,
        storesAndSchemes.encryptionKeySpec,
        timeouts,
        loggerFactory,
      )

    // For all other KMSs we create the KMS-based private crypto directly as the schemes have already been checked
    case _ =>
      Right(
        new KmsPrivateCrypto(
          kms,
          kmsCryptoPrivateStore,
          storesAndSchemes.cryptoPublicStore,
          storesAndSchemes.signingAlgorithmSpec,
          storesAndSchemes.signingKeySpec,
          storesAndSchemes.encryptionKeySpec,
          timeouts,
          loggerFactory,
        )
      )
  }

}
