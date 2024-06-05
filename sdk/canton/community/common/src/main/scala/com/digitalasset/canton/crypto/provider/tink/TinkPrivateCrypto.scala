// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.tink

import cats.data.EitherT
import cats.instances.future.*
import cats.syntax.either.*
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.crypto.tink.KeysetHandle
import com.google.crypto.tink.aead.AeadKeyTemplates
import com.google.crypto.tink.hybrid.HybridKeyTemplates
import com.google.crypto.tink.proto.*
import com.google.crypto.tink.signature.SignatureKeyTemplates

import java.security.GeneralSecurityException
import scala.concurrent.{ExecutionContext, Future}

class TinkPrivateCrypto private (
    pureCrypto: TinkPureCrypto,
    override val defaultSigningKeyScheme: SigningKeyScheme,
    override val defaultEncryptionKeyScheme: EncryptionKeyScheme,
    override protected val store: CryptoPrivateStoreExtended,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreApi
    with NamedLogging {

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  private def generateKey[E](
      keyTemplate: KeyTemplate,
      errFn: GeneralSecurityException => E,
  ): Either[E, KeysetHandle] =
    Either
      .catchOnly[GeneralSecurityException](KeysetHandle.generateNew(keyTemplate))
      .leftMap(errFn)

  override protected[crypto] def generateEncryptionKeypair(scheme: EncryptionKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionKeyPair] =
    generateEncryptionKeypairInternal(scheme)

  /** @param outputPrefixType by default we use RAW key templates such that the
    *                         ciphertexts are not prefixed with a Tink prefix.
    *                         MUST ONLY BE CHANGED FOR TESTING PURPOSES.
    */
  protected[crypto] def generateEncryptionKeypairInternal(
      scheme: EncryptionKeyScheme,
      outputPrefixType: OutputPrefixType = OutputPrefixType.RAW,
  ): EitherT[Future, EncryptionKeyGenerationError, EncryptionKeyPair] =
    for {
      keyTemplate <- scheme match {
        case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
          val eciesParams =
            HybridKeyTemplates.createEciesAeadHkdfParams(
              EllipticCurveType.NIST_P256,
              HashType.SHA256,
              EcPointFormat.UNCOMPRESSED,
              AeadKeyTemplates.AES128_GCM,
              Array[Byte](),
            )

          val format = EciesAeadHkdfKeyFormat
            .newBuilder()
            .setParams(eciesParams)
            .build()

          val keyTemplate = KeyTemplate
            .newBuilder()
            .setTypeUrl("type.googleapis.com/google.crypto.tink.EciesAeadHkdfPrivateKey")
            .setOutputPrefixType(outputPrefixType)
            .setValue(format.toByteString)
            .build()

          EitherT.rightT(keyTemplate)
        case EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc =>
          EitherT.leftT(
            EncryptionKeyGenerationError.UnsupportedKeyScheme(
              EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc
            )
          )
        case EncryptionKeyScheme.Rsa2048OaepSha256 =>
          EitherT.leftT(
            EncryptionKeyGenerationError.UnsupportedKeyScheme(
              EncryptionKeyScheme.Rsa2048OaepSha256
            )
          )
      }
      keysetHandle <- generateKey[EncryptionKeyGenerationError](
        keyTemplate,
        EncryptionKeyGenerationError.GeneralError,
      ).toEitherT
      publicKeysetHandle = keysetHandle.getPublicKeysetHandle
      fingerprint <- TinkKeyFormat
        .fingerprint(publicKeysetHandle, pureCrypto.defaultHashAlgorithm)
        .leftMap[EncryptionKeyGenerationError](EncryptionKeyGenerationError.FingerprintError)
        .toEitherT
      keypair = EncryptionKeyPair.create(
        id = fingerprint,
        format = CryptoKeyFormat.Tink,
        publicKeyBytes = TinkKeyFormat.serializeHandle(publicKeysetHandle),
        privateKeyBytes = TinkKeyFormat.serializeHandle(keysetHandle),
        scheme = scheme,
      )
    } yield keypair

  override protected[crypto] def generateSigningKeypair(scheme: SigningKeyScheme)(implicit
      traceContext: TraceContext
  ): EitherT[Future, SigningKeyGenerationError, SigningKeyPair] =
    generateSigningKeypairInternal(scheme)

  /** @param outputPrefixType by default we use RAW key templates such that the
    *                         signatures are not prefixed with a Tink prefix.
    *                         MUST ONLY BE CHANGED FOR TESTING PURPOSES.
    */
  protected[crypto] def generateSigningKeypairInternal(
      scheme: SigningKeyScheme,
      outputPrefixType: OutputPrefixType = OutputPrefixType.RAW,
  ): EitherT[Future, SigningKeyGenerationError, SigningKeyPair] = {
    for {
      keyTemplate <- for {
        template <- scheme match {
          case SigningKeyScheme.Ed25519 =>
            EitherT.rightT(SignatureKeyTemplates.ED25519)
          case SigningKeyScheme.EcDsaP256 =>
            EitherT.rightT(SignatureKeyTemplates.ECDSA_P256)
          case SigningKeyScheme.EcDsaP384 =>
            // Overwrite the hash function to use SHA384
            EitherT.rightT(
              SignatureKeyTemplates.createEcdsaKeyTemplate(
                HashType.SHA384,
                EllipticCurveType.NIST_P384,
                EcdsaSignatureEncoding.DER,
                outputPrefixType,
              )
            )
        }
      } yield {
        KeyTemplate
          .newBuilder(template)
          .setOutputPrefixType(outputPrefixType)
          .build()
      }
      keysetHandle <- generateKey[SigningKeyGenerationError](
        keyTemplate,
        SigningKeyGenerationError.GeneralError,
      ).toEitherT
      publicKeysetHandle = keysetHandle.getPublicKeysetHandle
      fingerprint <- TinkKeyFormat
        .fingerprint(publicKeysetHandle, pureCrypto.defaultHashAlgorithm)
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.FingerprintError)
        .toEitherT
      keypair = SigningKeyPair.create(
        id = fingerprint,
        format = CryptoKeyFormat.Tink,
        publicKeyBytes = TinkKeyFormat.serializeHandle(publicKeysetHandle),
        privateKeyBytes = TinkKeyFormat.serializeHandle(keysetHandle),
        scheme = scheme,
      )
    } yield keypair
  }

  override def name: String = "tink-private-crypto"

  override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
}

object TinkPrivateCrypto {
  def create(
      pureCrypto: TinkPureCrypto,
      defaultSigningKeyScheme: SigningKeyScheme,
      defaultEncryptionKeyScheme: EncryptionKeyScheme,
      privateStore: CryptoPrivateStoreExtended,
      timeouts: ProcessingTimeout,
      loggerFactory: NamedLoggerFactory,
  )(implicit ec: ExecutionContext): TinkPrivateCrypto =
    new TinkPrivateCrypto(
      pureCrypto,
      defaultSigningKeyScheme,
      defaultEncryptionKeyScheme,
      privateStore,
      timeouts,
      loggerFactory,
    )
}
