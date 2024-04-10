// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.google.crypto.tink.subtle.EllipticCurves
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers
import org.bouncycastle.asn1.sec.SECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers
import org.bouncycastle.crypto.params.Ed25519PublicKeyParameters

import java.io.IOException
import java.security.spec.{InvalidKeySpecException, X509EncodedKeySpec}
import java.security.{
  GeneralSecurityException,
  KeyFactory,
  NoSuchAlgorithmException,
  PublicKey as JPublicKey,
}

class JceJavaConverter(
    supportedSigningSchemes: NonEmpty[Set[SigningKeyScheme]],
    supportedEncryptionSchemes: NonEmpty[Set[EncryptionKeyScheme]],
) extends JavaKeyConverter {

  import com.digitalasset.canton.util.ShowUtil.*

  private def fromCurveType(curveType: CurveType): Either[String, AlgorithmIdentifier] =
    for {
      asnObjId <- curveType match {
        // CCF prefers the X9 identifiers (ecdsa-shaX) and not the SEC OIDs (secp384r1)
        case CurveType.NIST_P256 => Right(X9ObjectIdentifiers.ecdsa_with_SHA256)
        case CurveType.NIST_P384 => Right(X9ObjectIdentifiers.ecdsa_with_SHA384)
        case CurveType.NIST_P521 => Left("Elliptic curve NIST-P521 not supported right now")
      }
    } yield new AlgorithmIdentifier(asnObjId)

  private def toJavaEcDsa(
      publicKey: PublicKey,
      curveType: CurveType,
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] =
    for {
      _ <- CryptoKeyValidation.ensureFormat(
        publicKey.format,
        Set(CryptoKeyFormat.Der),
        _ => JavaKeyConversionError.UnsupportedKeyFormat(publicKey.format, CryptoKeyFormat.Der),
      )
      algoId <- fromCurveType(curveType).leftMap(JavaKeyConversionError.InvalidKey)
      ecPublicKey <- Either
        .catchOnly[GeneralSecurityException](
          EllipticCurves.getEcPublicKey(publicKey.key.toByteArray)
        )
        .leftMap(JavaKeyConversionError.GeneralError)
    } yield (algoId, ecPublicKey)

  override def toJava(
      publicKey: PublicKey
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] = {

    def convert(
        format: CryptoKeyFormat,
        x509PublicKey: Array[Byte],
        keyInstance: String,
    ): Either[JavaKeyConversionError, JPublicKey] =
      for {
        _ <- CryptoKeyValidation.ensureFormat(
          publicKey.format,
          Set(format),
          _ => JavaKeyConversionError.UnsupportedKeyFormat(publicKey.format, format),
        )
        x509KeySpec = new X509EncodedKeySpec(x509PublicKey)
        keyFactory <- Either
          .catchOnly[NoSuchAlgorithmException](
            KeyFactory.getInstance(keyInstance, JceSecurityProvider.bouncyCastleProvider)
          )
          .leftMap(JavaKeyConversionError.GeneralError)
        javaPublicKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePublic(x509KeySpec))
          .leftMap(err => JavaKeyConversionError.InvalidKey(show"$err"))
      } yield javaPublicKey

    (publicKey: @unchecked) match {
      case sigKey: SigningPublicKey =>
        sigKey.scheme match {
          case SigningKeyScheme.Ed25519 =>
            val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
            val x509PublicKey = new SubjectPublicKeyInfo(algoId, publicKey.key.toByteArray)
            convert(CryptoKeyFormat.Raw, x509PublicKey.getEncoded, "Ed25519").map(pk =>
              (algoId, pk)
            )
          case SigningKeyScheme.EcDsaP256 => toJavaEcDsa(publicKey, CurveType.NIST_P256)
          case SigningKeyScheme.EcDsaP384 => toJavaEcDsa(publicKey, CurveType.NIST_P384)
        }
      case encKey: EncryptionPublicKey =>
        encKey.scheme match {
          case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm =>
            toJavaEcDsa(publicKey, CurveType.NIST_P256)
          case EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc =>
            val algoId = new AlgorithmIdentifier(SECObjectIdentifiers.secp256r1)
            convert(CryptoKeyFormat.Der, publicKey.key.toByteArray, "EC").map(pk => (algoId, pk))
          case EncryptionKeyScheme.Rsa2048OaepSha256 =>
            val algoId = new AlgorithmIdentifier(PKCSObjectIdentifiers.id_RSAES_OAEP)
            convert(CryptoKeyFormat.Der, publicKey.key.toByteArray, "RSA").map(pk => (algoId, pk))
        }
    }
  }

  override def fromJavaSigningKey(
      javaPublicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
  ): Either[JavaKeyConversionError, SigningPublicKey] = {

    def ensureJceSupportedScheme(scheme: SigningKeyScheme): Either[JavaKeyConversionError, Unit] = {
      Either.cond(
        supportedSigningSchemes.contains(scheme),
        (),
        JavaKeyConversionError.UnsupportedSigningKeyScheme(scheme, supportedSigningSchemes),
      )
    }

    javaPublicKey.getAlgorithm match {
      case "EC" =>
        for {
          scheme <- JavaKeyConverter.toSigningKeyScheme(algorithmIdentifier)
          _ <- Either.cond(
            SigningKeyScheme.EcDsaSchemes.contains(scheme),
            (),
            JavaKeyConversionError.UnsupportedSigningKeyScheme(
              scheme,
              SigningKeyScheme.EcDsaSchemes,
            ),
          )
          _ <- ensureJceSupportedScheme(scheme)

          publicKeyBytes = ByteString.copyFrom(javaPublicKey.getEncoded)
          fingerprint = Fingerprint.create(publicKeyBytes)
          publicKey = new SigningPublicKey(
            id = fingerprint,
            format = CryptoKeyFormat.Der,
            key = publicKeyBytes,
            scheme = scheme,
          )
        } yield publicKey

      // With support for EdDSA (since Java 15) the more general 'EdDSA' algorithm identifier is also used
      // See https://bugs.openjdk.org/browse/JDK-8190219
      case "Ed25519" | "EdDSA" =>
        for {
          scheme <- JavaKeyConverter.toSigningKeyScheme(algorithmIdentifier)
          _ <- Either.cond(
            scheme == SigningKeyScheme.Ed25519,
            (),
            JavaKeyConversionError.UnsupportedSigningKeyScheme(
              scheme,
              NonEmpty.mk(Set, SigningKeyScheme.Ed25519),
            ),
          )
          _ <- ensureJceSupportedScheme(scheme)

          // The encoded public key has a prefix that we drop
          publicKeyEncoded = ByteString.copyFrom(
            javaPublicKey.getEncoded.takeRight(Ed25519PublicKeyParameters.KEY_SIZE)
          )
          publicKeyParams <- Either
            .catchOnly[IOException](new Ed25519PublicKeyParameters(publicKeyEncoded.newInput()))
            .leftMap(JavaKeyConversionError.GeneralError)
          publicKeyBytes = ByteString.copyFrom(publicKeyParams.getEncoded)
          fingerprint = Fingerprint.create(publicKeyBytes)
          publicKey = new SigningPublicKey(
            id = fingerprint,
            format = CryptoKeyFormat.Raw,
            key = publicKeyBytes,
            scheme = scheme,
          )
        } yield publicKey

      case unsupportedAlgo =>
        Left(
          JavaKeyConversionError.InvalidKey(
            s"Java public key of kind $unsupportedAlgo not supported."
          )
        )
    }
  }

  override def fromJavaEncryptionKey(
      javaPublicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
  ): Either[JavaKeyConversionError, EncryptionPublicKey] = {

    def ensureJceSupportedScheme(
        scheme: EncryptionKeyScheme
    ): Either[JavaKeyConversionError, Unit] = {
      Either.cond(
        supportedEncryptionSchemes.contains(scheme),
        (),
        JavaKeyConversionError.UnsupportedEncryptionKeyScheme(scheme, supportedEncryptionSchemes),
      )
    }

    javaPublicKey.getAlgorithm match {
      case "EC" | "RSA" =>
        for {
          scheme <- JavaKeyConverter.toEncryptionKeyScheme(algorithmIdentifier)
          _ <- ensureJceSupportedScheme(scheme)

          publicKeyBytes = ByteString.copyFrom(javaPublicKey.getEncoded)
          fingerprint = Fingerprint.create(publicKeyBytes)
          publicKey = new EncryptionPublicKey(
            id = fingerprint,
            format = CryptoKeyFormat.Der,
            key = publicKeyBytes,
            scheme = scheme,
          )
        } yield publicKey

      case unsupportedAlgo =>
        Left(
          JavaKeyConversionError.InvalidKey(
            s"Java public key of kind $unsupportedAlgo not supported."
          )
        )
    }
  }

}
