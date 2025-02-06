// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.crypto.tink.subtle.{Ed25519Sign, EllipticCurves}
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.DEROctetString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}

import java.security.spec.{ECGenParameterSpec, RSAKeyGenParameterSpec}
import java.security.{GeneralSecurityException, KeyPair as JKeyPair, KeyPairGenerator}
import scala.concurrent.ExecutionContext

class JcePrivateCrypto(
    pureCrypto: JcePureCrypto,
    override val defaultSigningAlgorithmSpec: SigningAlgorithmSpec,
    override val defaultSigningKeySpec: SigningKeySpec,
    override val defaultEncryptionKeySpec: EncryptionKeySpec,
    override protected val store: CryptoPrivateStoreExtended,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreApi
    with NamedLogging {

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  override protected[crypto] def generateEncryptionKeypair(keySpec: EncryptionKeySpec)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionKeyPair] =
    JcePrivateCrypto.generateEncryptionKeypair(keySpec).toEitherT

  override protected[crypto] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningKeyPair] =
    JcePrivateCrypto.generateSigningKeypair(keySpec, usage).toEitherT

  override def name: String = "jce-private-crypto"

  override protected def initialHealthState: ComponentHealthState = ComponentHealthState.Ok()
}

object JcePrivateCrypto {

  // Internal case class to ensure we don't mix up the private and public key bytestrings
  private final case class JavaEncodedKeyPair(
      id: Fingerprint,
      publicKey: ByteString,
      privateKey: ByteString,
  )

  private def fromJavaKeyPair(javaKeyPair: JKeyPair): JavaEncodedKeyPair = {
    // Encode public key as X509 subject public key info in DER
    val publicKey = ByteString.copyFrom(javaKeyPair.getPublic.getEncoded)

    // Encode private key as PKCS8 in DER
    val privateKey = ByteString.copyFrom(javaKeyPair.getPrivate.getEncoded)

    val keyId = Fingerprint.create(publicKey)

    JavaEncodedKeyPair(keyId, publicKey, privateKey)
  }

  private def fromJavaSigningKeyPair(
      javaKeyPair: JKeyPair,
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[SigningKeyGenerationError, SigningKeyPair] = {
    val javaEncodedKeyPair = fromJavaKeyPair(javaKeyPair)
    SigningKeyPair.create(
      publicFormat = CryptoKeyFormat.DerX509Spki,
      publicKeyBytes = javaEncodedKeyPair.publicKey,
      privateFormat = CryptoKeyFormat.DerPkcs8Pki,
      privateKeyBytes = javaEncodedKeyPair.privateKey,
      keySpec = keySpec,
      usage = usage,
    )
  }

  private def generateEcDsaSigningKeyPair(
      curveType: CurveType,
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[SigningKeyGenerationError, SigningKeyPair] =
    for {
      javaKeyPair <- Either
        .catchOnly[GeneralSecurityException](EllipticCurves.generateKeyPair(curveType))
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError.apply)
      keyPair <- fromJavaSigningKeyPair(javaKeyPair, keySpec, usage)
    } yield keyPair

  protected[crypto] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[SigningKeyGenerationError, SigningKeyPair] = keySpec match {
    case SigningKeySpec.EcCurve25519 =>
      for {
        rawKeyPair <- Either
          .catchOnly[GeneralSecurityException](Ed25519Sign.KeyPair.newKeyPair())
          .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError.apply)
        algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
        publicKey = new SubjectPublicKeyInfo(algoId, rawKeyPair.getPublicKey).getEncoded
        privateKey = new PrivateKeyInfo(
          algoId,
          new DEROctetString(rawKeyPair.getPrivateKey),
        ).getEncoded
        keyPair <- SigningKeyPair
          .create(
            publicFormat = CryptoKeyFormat.DerX509Spki,
            publicKeyBytes = ByteString.copyFrom(publicKey),
            privateFormat = CryptoKeyFormat.DerPkcs8Pki,
            privateKeyBytes = ByteString.copyFrom(privateKey),
            keySpec = keySpec,
            usage = usage,
          )
      } yield keyPair

    case SigningKeySpec.EcP256 =>
      generateEcDsaSigningKeyPair(EllipticCurves.CurveType.NIST_P256, keySpec, usage)

    case SigningKeySpec.EcP384 =>
      generateEcDsaSigningKeyPair(EllipticCurves.CurveType.NIST_P384, keySpec, usage)

    case SigningKeySpec.EcSecp256k1 =>
      for {
        javaKeyPair <-
          Either
            .catchOnly[GeneralSecurityException](
              {
                val kpGen =
                  KeyPairGenerator.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
                kpGen.initialize(new ECGenParameterSpec("secp256k1"))
                kpGen.generateKeyPair()
              }
            )
            .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError.apply)

        keyPair <- fromJavaSigningKeyPair(javaKeyPair, keySpec, usage)
      } yield keyPair

  }

  protected[crypto] def generateEncryptionKeypair(
      keySpec: EncryptionKeySpec
  ): Either[EncryptionKeyGenerationError, EncryptionKeyPair] = {

    def convertJavaKeyPair(
        javaKeyPair: JKeyPair
    ): EncryptionKeyPair = {
      val rawKeyPair = fromJavaKeyPair(javaKeyPair)
      EncryptionKeyPair.create(
        publicFormat = CryptoKeyFormat.DerX509Spki,
        publicKeyBytes = rawKeyPair.publicKey,
        privateFormat = CryptoKeyFormat.DerPkcs8Pki,
        privateKeyBytes = rawKeyPair.privateKey,
        keySpec = keySpec,
      )
    }

    for {
      javaKeyPair <- keySpec match {
        case EncryptionKeySpec.EcP256 =>
          Either
            .catchOnly[GeneralSecurityException](
              {
                val kpGen =
                  KeyPairGenerator.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
                kpGen.initialize(new ECGenParameterSpec("P-256"))
                kpGen.generateKeyPair()
              }
            )
            .leftMap[EncryptionKeyGenerationError](EncryptionKeyGenerationError.GeneralError.apply)
        case EncryptionKeySpec.Rsa2048 =>
          Either
            .catchOnly[GeneralSecurityException](
              {
                val kpGen =
                  KeyPairGenerator.getInstance("RSA", JceSecurityProvider.bouncyCastleProvider)
                kpGen.initialize(new RSAKeyGenParameterSpec(2048, RSAKeyGenParameterSpec.F4))
                kpGen.generateKeyPair()
              }
            )
            .leftMap[EncryptionKeyGenerationError](EncryptionKeyGenerationError.GeneralError.apply)
      }
    } yield convertJavaKeyPair(javaKeyPair)
  }
}
