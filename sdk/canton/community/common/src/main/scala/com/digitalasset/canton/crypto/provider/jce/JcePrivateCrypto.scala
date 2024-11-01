// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.tracing.TraceContext
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.crypto.tink.subtle.{Ed25519Sign, EllipticCurves}
import com.google.protobuf.ByteString

import java.security.spec.{ECGenParameterSpec, RSAKeyGenParameterSpec}
import java.security.{GeneralSecurityException, KeyPair as JKeyPair, KeyPairGenerator}
import scala.concurrent.ExecutionContext

class JcePrivateCrypto(
    pureCrypto: JcePureCrypto,
    override val defaultSigningAlgorithmSpec: SigningAlgorithmSpec,
    override val defaultSigningKeySpec: SigningKeySpec,
    override val defaultEncryptionKeySpec: EncryptionKeySpec,
    override protected val store: CryptoPrivateStoreExtended,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreApi {

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  // Internal case class to ensure we don't mix up the private and public key bytestrings
  private case class RawKeyPair(id: Fingerprint, publicKey: ByteString, privateKey: ByteString)

  private def fromJavaKeyPair(javaKeyPair: JKeyPair): RawKeyPair = {
    // Encode public key as X509 subject public key info in DER
    val publicKey = ByteString.copyFrom(javaKeyPair.getPublic.getEncoded)

    // Encode private key as PKCS8 in DER
    val privateKey = ByteString.copyFrom(javaKeyPair.getPrivate.getEncoded)

    val keyId = Fingerprint.create(publicKey)

    RawKeyPair(keyId, publicKey, privateKey)
  }

  private def fromJavaSigningKeyPair(
      javaKeyPair: JKeyPair,
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): SigningKeyPair = {
    val rawKeyPair = fromJavaKeyPair(javaKeyPair)
    SigningKeyPair.create(
      format = CryptoKeyFormat.Der,
      publicKeyBytes = rawKeyPair.publicKey,
      privateKeyBytes = rawKeyPair.privateKey,
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
    } yield fromJavaSigningKeyPair(javaKeyPair, keySpec, usage)

  override protected[crypto] def generateEncryptionKeypair(keySpec: EncryptionKeySpec)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionKeyPair] = {

    def convertJavaKeyPair(
        javaKeyPair: JKeyPair
    ): EncryptionKeyPair = {
      val rawKeyPair = fromJavaKeyPair(javaKeyPair)
      EncryptionKeyPair.create(
        format = CryptoKeyFormat.Der,
        publicKeyBytes = rawKeyPair.publicKey,
        privateKeyBytes = rawKeyPair.privateKey,
        keySpec = keySpec,
      )
    }

    EitherT.fromEither {
      (keySpec match {
        case EncryptionKeySpec.EcP256 =>
          Either
            .catchOnly[GeneralSecurityException](
              {
                val kpGen = KeyPairGenerator.getInstance("EC", "BC")
                kpGen.initialize(new ECGenParameterSpec("P-256"))
                kpGen.generateKeyPair()
              }
            )
            .leftMap[EncryptionKeyGenerationError](EncryptionKeyGenerationError.GeneralError.apply)
        case EncryptionKeySpec.Rsa2048 =>
          Either
            .catchOnly[GeneralSecurityException](
              {
                val kpGen = KeyPairGenerator.getInstance("RSA", "BC")
                kpGen.initialize(new RSAKeyGenParameterSpec(2048, RSAKeyGenParameterSpec.F4))
                kpGen.generateKeyPair()
              }
            )
            .leftMap[EncryptionKeyGenerationError](EncryptionKeyGenerationError.GeneralError.apply)
      }).map(convertJavaKeyPair)
    }
  }

  override protected[crypto] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningKeyPair] = keySpec match {
    case SigningKeySpec.EcCurve25519 =>
      for {
        rawKeyPair <- Either
          .catchOnly[GeneralSecurityException](Ed25519Sign.KeyPair.newKeyPair())
          .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError.apply)
          .toEitherT[FutureUnlessShutdown]
        publicKey = ByteString.copyFrom(rawKeyPair.getPublicKey)
        privateKey = ByteString.copyFrom(rawKeyPair.getPrivateKey)
        keyPair = SigningKeyPair
          .create(
            format = CryptoKeyFormat.Raw,
            publicKeyBytes = publicKey,
            privateKeyBytes = privateKey,
            keySpec = keySpec,
            usage = usage,
          )
      } yield keyPair

    case SigningKeySpec.EcP256 =>
      generateEcDsaSigningKeyPair(EllipticCurves.CurveType.NIST_P256, keySpec, usage).toEitherT

    case SigningKeySpec.EcP384 =>
      generateEcDsaSigningKeyPair(EllipticCurves.CurveType.NIST_P384, keySpec, usage).toEitherT

  }

  override def close(): Unit = ()
}
