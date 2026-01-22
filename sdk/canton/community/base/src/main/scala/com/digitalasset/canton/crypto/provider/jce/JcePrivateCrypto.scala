// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.data.EitherT
import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.config.ProcessingTimeout
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.CryptoKeyFormat.Symbolic
import com.digitalasset.canton.crypto.store.CryptoPrivateStoreExtended
import com.digitalasset.canton.health.ComponentHealthState
import com.digitalasset.canton.lifecycle.FutureUnlessShutdown
import com.digitalasset.canton.logging.{NamedLoggerFactory, NamedLogging}
import com.digitalasset.canton.tracing.TraceContext
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.DEROctetString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.bouncycastle.crypto.params.Ed25519PrivateKeyParameters
import org.bouncycastle.jce.ECNamedCurveTable
import org.bouncycastle.jce.spec.ECNamedCurveSpec

import java.security.interfaces.{ECPrivateKey, RSAPrivateKey}
import java.security.spec.{
  ECGenParameterSpec,
  ECPublicKeySpec,
  InvalidKeySpecException,
  RSAKeyGenParameterSpec,
  RSAPublicKeySpec,
}
import java.security.{
  GeneralSecurityException,
  KeyFactory,
  KeyPair as JKeyPair,
  KeyPairGenerator,
  spec,
}
import scala.concurrent.ExecutionContext

class JcePrivateCrypto(
    pureCrypto: JcePureCrypto,
    override val signingSchemes: SigningCryptoSchemes,
    override val encryptionSchemes: EncryptionCryptoSchemes,
    override protected val store: CryptoPrivateStoreExtended,
    override protected val timeouts: ProcessingTimeout,
    override protected val loggerFactory: NamedLoggerFactory,
)(override implicit val ec: ExecutionContext)
    extends CryptoPrivateStoreApi
    with NamedLogging {

  override private[crypto] def getInitialHealthState: ComponentHealthState = this.initialHealthState

  override protected val signingOps: SigningOps = pureCrypto
  override protected val encryptionOps: EncryptionOps = pureCrypto

  override protected[crypto] def generateEncryptionKeypair(keySpec: EncryptionKeySpec)(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, EncryptionKeyGenerationError, EncryptionKeyPair] =
    CryptoKeyValidation
      .ensureCryptoKeySpec(
        keySpec,
        encryptionSchemes.keySpecs.allowed,
        EncryptionKeyGenerationError.UnsupportedKeySpec.apply,
      )
      .flatMap(_ => JcePrivateCrypto.generateEncryptionKeypair(keySpec))
      .toEitherT

  override protected[crypto] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  )(implicit
      traceContext: TraceContext
  ): EitherT[FutureUnlessShutdown, SigningKeyGenerationError, SigningKeyPair] =
    CryptoKeyValidation
      .ensureCryptoKeySpec(
        keySpec,
        signingSchemes.keySpecs.allowed,
        SigningKeyGenerationError.UnsupportedKeySpec.apply,
      )
      .flatMap(_ => JcePrivateCrypto.generateSigningKeypair(keySpec, usage))
      .toEitherT

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
  ): Either[SigningKeyCreationError, SigningKeyPair] = {
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

  private def generateEcDsaJKeyPair(
      keySpec: EcKeySpec
  ): Either[GeneralSecurityException, JKeyPair] =
    Either
      .catchOnly[GeneralSecurityException] {
        val kpg = KeyPairGenerator.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
        kpg.initialize(new ECGenParameterSpec(keySpec.jcaCurveName))
        kpg.generateKeyPair()
      }

  private def generateEcDsaSigningKeyPair(
      ecKeySpec: EcKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[SigningKeyGenerationError, SigningKeyPair] =
    for {
      javaKeyPair <- generateEcDsaJKeyPair(ecKeySpec)
        .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError.apply)
      signingKeySpec <- ecKeySpec match {
        case SigningKeySpec.EcP256 => Right(SigningKeySpec.EcP256)
        case SigningKeySpec.EcSecp256k1 => Right(SigningKeySpec.EcSecp256k1)
        case SigningKeySpec.EcP384 => Right(SigningKeySpec.EcP384)
        case spec =>
          Left(
            SigningKeyGenerationError.GeneralError(
              new IllegalArgumentException(s"$spec is not an EC signing key spec")
            )
          )
      }
      keyPair <- fromJavaSigningKeyPair(javaKeyPair, signingKeySpec, usage)
        .leftMap(SigningKeyGenerationError.KeyCreationError.apply)
    } yield keyPair

  private[canton] def generateSigningKeypair(
      keySpec: SigningKeySpec,
      usage: NonEmpty[Set[SigningKeyUsage]],
  ): Either[SigningKeyGenerationError, SigningKeyPair] = keySpec match {
    case SigningKeySpec.EcCurve25519 =>
      for {
        rawPrivateKey <- Either
          .catchNonFatal {
            val priv = new Ed25519PrivateKeyParameters(JceSecureRandom.random.get())
            ByteString.copyFrom(priv.getEncoded)
          }
          .leftMap[SigningKeyGenerationError](SigningKeyGenerationError.GeneralError.apply)
        privateKeyDer <- JcePrivateCrypto
          .encodeEd25519PrivateKey(rawPrivateKey)
          .leftMap[SigningKeyGenerationError](err =>
            SigningKeyGenerationError.KeyCreationError(
              SigningKeyCreationError.CreatePrivateKeyError(err)
            )
          )
        publicKeyDer <- JcePrivateCrypto
          .deriveEd25519PublicKey(rawPrivateKey)
          .leftMap[SigningKeyGenerationError](err =>
            SigningKeyGenerationError.KeyCreationError(
              SigningKeyCreationError.DerivePublicKeyError(err)
            )
          )
        keyPair <- SigningKeyPair
          .create(
            publicFormat = CryptoKeyFormat.DerX509Spki,
            publicKeyBytes = publicKeyDer,
            privateFormat = CryptoKeyFormat.DerPkcs8Pki,
            privateKeyBytes = privateKeyDer,
            keySpec = keySpec,
            usage = usage,
          )
          .leftMap(SigningKeyGenerationError.KeyCreationError.apply)
      } yield keyPair

    case ecKeySpec: EcKeySpec =>
      generateEcDsaSigningKeyPair(ecKeySpec, usage)

  }

  private[crypto] def generateEncryptionKeypair(
      keySpec: EncryptionKeySpec
  ): Either[EncryptionKeyGenerationError, EncryptionKeyPair] = {

    def fromJavaSigningKeyPair(
        javaKeyPair: JKeyPair
    ): Either[EncryptionKeyCreationError, EncryptionKeyPair] = {
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
        case ecKeySpec: EcKeySpec =>
          generateEcDsaJKeyPair(ecKeySpec)
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
      keyPair <- fromJavaSigningKeyPair(javaKeyPair).leftMap(
        EncryptionKeyGenerationError.KeyCreationError.apply
      )
    } yield keyPair
  }

  /** Encodes an Ed25519 private key given as bytes into a minimal PKCS#8 DER-encoded
    * PrivateKeyInfo.
    *
    * @param rawPrivateKey
    *   Raw Ed25519 private key bytes
    * @return
    *   DER-encoded PKCS#8 private key
    */
  private[crypto] def encodeEd25519PrivateKey(
      rawPrivateKey: ByteString
  ): Either[String, ByteString] =
    Either
      .catchNonFatal {
        val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
        ByteString.copyFrom(
          new PrivateKeyInfo(
            algoId,
            new DEROctetString(rawPrivateKey.toByteArray),
          ).getEncoded
        )
      }
      .leftMap(_.toString)

  /** Derives the public key from a raw Ed25519 private key and encodes it as a DER-encoded X.509
    * SubjectPublicKeyInfo (SPKI).
    *
    * @param rawPrivateKey
    *   Raw Ed25519 private key bytes
    * @return
    *   DER-encoded X.509 SPKI public key
    */
  private[crypto] def deriveEd25519PublicKey(
      rawPrivateKey: ByteString
  ): Either[String, ByteString] =
    Either
      .catchNonFatal {
        // Derive the raw public key bytes
        val rawPublicKey = new Ed25519PrivateKeyParameters(rawPrivateKey.toByteArray, 0)
          .generatePublicKey()

        // Encode the raw public key bytes in SubjectPublicKeyInfo (DER X.509 SPKI)
        val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
        val spki = new SubjectPublicKeyInfo(algoId, rawPublicKey.getEncoded)
        ByteString.copyFrom(spki.getEncoded)
      }
      .leftMap(_.toString)

  /** Derives the public key from the given private key material. This is used when importing a key
    * pair via the `importKeyPair` console command to prevent a malicious admin from injecting
    * mismatched keys.
    */
  private[crypto] def derivePublicKey(
      privateKey: PrivateKey
  ): Either[String, PublicKey] = {

    // Derives the public key point (Q = d * G) from the EC private key.
    def deriveEcPublicKey(
        curveName: String,
        privateKey: PrivateKey,
    ): Either[String, ByteString] =
      for {
        jKey <- JceJavaKeyConverter.toJava(privateKey).leftMap(_.toString)
        ecPrivateKey <- jKey match {
          case ecPrivateKey: ECPrivateKey => Right(ecPrivateKey)
          case _ => Left("Invalid EC key")
        }

        params <- Option(ECNamedCurveTable.getParameterSpec(curveName))
          .toRight(s"Cannot get parameters from curve $curveName")
        d = ecPrivateKey.getS

        // Compute Q = d * G. Refer to https://www.secg.org/sec1-v2.pdf, section 3.2.1 Elliptic Curve Key Pair
        // Generation Primitive.
        publicPoint <-
          Either
            .catchOnly[ArithmeticException](params.getG.multiply(d).normalize())
            .leftMap(_.toString)

        // Convert BC ECPoint → java.security.spec.ECPoint
        affineX = publicPoint.getAffineXCoord.toBigInteger
        affineY = publicPoint.getAffineYCoord.toBigInteger
        javaPoint = new spec.ECPoint(affineX, affineY)

        // Convert BC curve spec → standard Java spec
        javaParams = new ECNamedCurveSpec(
          curveName,
          params.getCurve,
          params.getG,
          params.getN,
          params.getH,
          params.getSeed,
        )

        pubSpec = new ECPublicKeySpec(javaPoint, javaParams)
        keyFactory = KeyFactory.getInstance("EC", JceSecurityProvider.bouncyCastleProvider)
        derivedPublicKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePublic(pubSpec).getEncoded)
          .leftMap(err => s"Failed to derive EC public key: $err")
      } yield ByteString.copyFrom(derivedPublicKey)

    // Derives the public key from an RSA private key using the private key’s modulus and the standard exponent (e = 65537).
    def deriveRsaPublicKey(
        privateKey: PrivateKey
    ): Either[String, ByteString] =
      for {
        jKey <- JceJavaKeyConverter.toJava(privateKey).leftMap(_.toString)
        rsaPrivateKey <- jKey match {
          case rsaPrivateKey: RSAPrivateKey => Right(rsaPrivateKey)
          case _ => Left(s"Invalid RSA key [${privateKey.id}]")
        }
        modulus = rsaPrivateKey.getModulus
        pubSpec = new RSAPublicKeySpec(modulus, RSAKeyGenParameterSpec.F4)
        keyFactory = KeyFactory.getInstance("RSA", JceSecurityProvider.bouncyCastleProvider)
        derivedPublicKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePublic(pubSpec).getEncoded)
          .leftMap(err => s"Failed to derive RSA public key: $err")
      } yield ByteString.copyFrom(derivedPublicKey)

    (privateKey: @unchecked) match {
      case SigningPrivateKey(_, Symbolic, _, _, _) | EncryptionPrivateKey(_, Symbolic, _, _) =>
        // we cannot derive a public key when using symbolic keys
        Left(s"Unsupported key format: $Symbolic")
      case signingPrivateKey @ SigningPrivateKey(_, _, _, keySpec, usage) =>
        val signingPublicKeyE = keySpec match {
          case SigningKeySpec.EcCurve25519 =>
            for {
              rawPrivateKeyBytes <- CryptoKeyFormat
                .extractPrivateKeyFromPkcs8Pki(privateKey.key)
                .leftMap(_.toString)
              publicKey <- deriveEd25519PublicKey(ByteString.copyFrom(rawPrivateKeyBytes))
            } yield publicKey
          case SigningKeySpec.EcP256 =>
            deriveEcPublicKey(SigningKeySpec.EcP256.jcaCurveName, signingPrivateKey)
          case SigningKeySpec.EcP384 =>
            deriveEcPublicKey(SigningKeySpec.EcP384.jcaCurveName, signingPrivateKey)
          case SigningKeySpec.EcSecp256k1 =>
            deriveEcPublicKey(SigningKeySpec.EcSecp256k1.jcaCurveName, signingPrivateKey)
        }
        signingPublicKeyE.flatMap(derivedPublicKey =>
          SigningPublicKey
            .create(
              CryptoKeyFormat.DerX509Spki,
              derivedPublicKey,
              keySpec,
              usage,
            )
            .leftMap(_.toString)
        )
      case encryptionPrivateKey @ EncryptionPrivateKey(_, _, _, keySpec) =>
        val encryptionPublicKeyE = keySpec match {
          case EncryptionKeySpec.EcP256 =>
            deriveEcPublicKey(SigningKeySpec.EcP256.jcaCurveName, encryptionPrivateKey)
          case EncryptionKeySpec.Rsa2048 =>
            deriveRsaPublicKey(encryptionPrivateKey)
        }
        encryptionPublicKeyE.flatMap(derivedPublicKey =>
          EncryptionPublicKey
            .create(
              CryptoKeyFormat.DerX509Spki,
              derivedPublicKey,
              keySpec,
            )
            .leftMap(_.toString)
        )
    }
  }

}
