// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.kms

import cats.data.EitherT
import cats.syntax.bifunctor.*
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.{CryptoProvider, ProcessingTimeout}
import com.digitalasset.canton.crypto.*
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
    private[kms] val privateStore: KmsCryptoPrivateStore,
    private[kms] val publicStore: CryptoPublicStore,
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

  /** This function and [[registerEncryptionKey]] is used to register a key directly to the store
    * (i.e. pre-generated) and bypass the default key generation procedure. As we are overriding the
    * usual way to create new keys, by using pre-generated ones, we need to add their public
    * material to a node's public store.
    */
  def registerSigningKey(
      keyId: KmsKeyId,
      usage: NonEmpty[Set[SigningKeyUsage]],
      keyName: Option[KeyName] = None,
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningPublicKey] =
    for {
      publicKeyWithoutUsage <- getPublicSigningKey(keyId)
      publicKey = publicKeyWithoutUsage
        .copy(usage = SigningKeyUsage.addProofOfOwnership(usage))(publicKeyWithoutUsage.migrated)
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
      publicKeyWithoutUsage <- kms
        .getPublicSigningKey(keyId)
        .leftMap[SigningKeyGenerationError](err =>
          SigningKeyGenerationError.GeneralKmsError(err.show)
        )
      publicKey = publicKeyWithoutUsage
        .copy(usage = SigningKeyUsage.addProofOfOwnership(usage))(publicKeyWithoutUsage.migrated)
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

  /** Check that all allowed schemes except for the pure crypto schemes are actually supported by
    * the driver too.
    *
    * The pure schemes are only supported for the pure crypto provider, e.g., verifying a signature
    * or asymmetrically encrypt. The private crypto of the driver does not need to support them.
    */
  private def ensureSupportedSchemes[S](
      configuredAllowedSchemes: Set[S],
      pureSchemes: Set[S],
      driverSupported: Set[S],
      description: String,
  ): Either[String, Unit] =
    Either.cond(
      configuredAllowedSchemes.removedAll(pureSchemes).subsetOf(driverSupported),
      (),
      s"Allowed $description ${configuredAllowedSchemes.mkString(", ")} not supported by driver: $driverSupported",
    )

  def create(
      kms: Kms,
      cryptoSchemes: CryptoSchemes,
      cryptoPublicStore: CryptoPublicStore,
      kmsCryptoPrivateStore: KmsCryptoPrivateStore,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit executionContext: ExecutionContext): Either[String, KmsPrivateCrypto] = kms match {

    // For a Driver KMS we explicitly check that the driver supports the allowed schemes
    case driverKms: DriverKms =>
      for {
        _ <- ensureSupportedSchemes(
          // Remove the pure signing algorithms from the allowed signing key specs
          cryptoSchemes.signingKeySpecs.allowed,
          CryptoProvider.Kms.pureSigningKeys,
          driverKms.supportedSigningKeySpecs,
          "signing key specs",
        )

        _ <- ensureSupportedSchemes(
          cryptoSchemes.signingAlgoSpecs.allowed,
          CryptoProvider.Kms.pureSigningAlgorithms,
          driverKms.supportedSigningAlgoSpecs,
          "signing algorithm specs",
        )

        _ <- ensureSupportedSchemes(
          cryptoSchemes.encryptionKeySpecs.allowed,
          CryptoProvider.Kms.pureEncryptionKeys,
          driverKms.supportedEncryptionKeySpecs,
          "encryption key specs",
        )
        _ <- ensureSupportedSchemes(
          cryptoSchemes.encryptionAlgoSpecs.allowed,
          CryptoProvider.Kms.pureEncryptionAlgorithms,
          driverKms.supportedEncryptionAlgoSpecs,
          "encryption algo specs",
        )
      } yield new KmsPrivateCrypto(
        kms,
        kmsCryptoPrivateStore,
        cryptoPublicStore,
        cryptoSchemes.signingAlgoSpecs.default,
        cryptoSchemes.signingKeySpecs.default,
        cryptoSchemes.encryptionKeySpecs.default,
        timeouts,
        loggerFactory,
      )

    // For all other KMSs we create the KMS-based private crypto directly as the schemes have already been checked
    case _ =>
      Right(
        new KmsPrivateCrypto(
          kms,
          kmsCryptoPrivateStore,
          cryptoPublicStore,
          cryptoSchemes.signingAlgoSpecs.default,
          cryptoSchemes.signingKeySpecs.default,
          cryptoSchemes.encryptionKeySpecs.default,
          timeouts,
          loggerFactory,
        )
      )
  }

}
