// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.crypto.provider.tink.TinkJavaConverter
import com.google.crypto.tink.subtle.EllipticCurves
import com.google.crypto.tink.subtle.EllipticCurves.CurveType
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers
import org.bouncycastle.asn1.sec.SECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
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

  private def ensureFormat(
      key: CryptoKey,
      format: CryptoKeyFormat,
  ): Either[JavaKeyConversionError, Unit] =
    Either.cond(
      key.format == format,
      (),
      JavaKeyConversionError.UnsupportedKeyFormat(key.format, format),
    )

  private def toJavaEcDsa(
      publicKey: PublicKey,
      curveType: CurveType,
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)] =
    for {
      _ <- ensureFormat(publicKey, CryptoKeyFormat.Der)
      // We are using the tink-subtle API here, thus using the TinkJavaConverter to have a consistent mapping of curve
      // type to algo id.
      algoId <- TinkJavaConverter
        .fromCurveType(curveType)
        .leftMap(JavaKeyConversionError.InvalidKey)
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
        _ <- ensureFormat(publicKey, format)
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
      fingerprint: Fingerprint,
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
      fingerprint: Fingerprint,
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
