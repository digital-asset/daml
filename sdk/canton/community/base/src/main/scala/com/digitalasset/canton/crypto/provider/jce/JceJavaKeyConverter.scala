// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import org.bouncycastle.asn1.DEROctetString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PrivateKeyInfo
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import sun.security.ec.ECPrivateKeyImpl

import java.security.spec.{InvalidKeySpecException, PKCS8EncodedKeySpec, X509EncodedKeySpec}
import java.security.{
  KeyFactory,
  NoSuchAlgorithmException,
  PrivateKey as JPrivateKey,
  PublicKey as JPublicKey,
}

/** Converter for Canton private and public keys into corresponding Java keys */
object JceJavaKeyConverter {

  import com.digitalasset.canton.util.ShowUtil.*

  private[crypto] def toJava(
      publicKey: PublicKey
  ): Either[JceJavaKeyConversionError, JPublicKey] = {

    def convert(
        format: CryptoKeyFormat,
        x509PublicKey: Array[Byte],
        keyInstance: String,
    ): Either[JceJavaKeyConversionError, JPublicKey] =
      for {
        _ <- CryptoKeyValidation.ensureFormat(
          publicKey.format,
          Set(format),
          _ => JceJavaKeyConversionError.UnsupportedKeyFormat(publicKey.format, format),
        )
        x509KeySpec = new X509EncodedKeySpec(x509PublicKey)
        keyFactory <- Either
          .catchOnly[NoSuchAlgorithmException](
            KeyFactory.getInstance(keyInstance, JceSecurityProvider.bouncyCastleProvider)
          )
          .leftMap(JceJavaKeyConversionError.GeneralError)
        javaPublicKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePublic(x509KeySpec))
          .leftMap(err => JceJavaKeyConversionError.InvalidKey(show"$err"))
      } yield javaPublicKey

    (publicKey: @unchecked) match {
      case sigKey: SigningPublicKey =>
        sigKey.scheme match {
          case SigningKeyScheme.Ed25519 =>
            val algoId = new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519)
            val x509PublicKey = new SubjectPublicKeyInfo(algoId, publicKey.key.toByteArray)
            convert(CryptoKeyFormat.Raw, x509PublicKey.getEncoded, "Ed25519")
          case SigningKeyScheme.EcDsaP256 | SigningKeyScheme.EcDsaP384 =>
            convert(CryptoKeyFormat.Der, publicKey.key.toByteArray, "EC")
        }
      case encKey: EncryptionPublicKey =>
        encKey.scheme match {
          case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm |
              EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc =>
            convert(CryptoKeyFormat.Der, publicKey.key.toByteArray, "EC")
          case EncryptionKeyScheme.Rsa2048OaepSha256 =>
            convert(CryptoKeyFormat.Der, publicKey.key.toByteArray, "RSA")
        }
    }
  }

  private[crypto] def toJava(
      privateKey: PrivateKey
  ): Either[JceJavaKeyConversionError, JPrivateKey] = {

    def convertFromPkcs8(
        pkcs8PrivateKey: Array[Byte],
        keyInstance: String,
    ): Either[JceJavaKeyConversionError, JPrivateKey] = {
      val pkcs8KeySpec = new PKCS8EncodedKeySpec(pkcs8PrivateKey)
      for {
        keyFactory <- Either
          .catchOnly[NoSuchAlgorithmException](KeyFactory.getInstance(keyInstance, "BC"))
          .leftMap(JceJavaKeyConversionError.GeneralError)
        javaPrivateKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePrivate(pkcs8KeySpec))
          .leftMap(err => JceJavaKeyConversionError.InvalidKey(show"$err"))
        _ =
          // There exists a race condition in ECPrivateKey before java 15
          if (Runtime.version.feature < 15)
            javaPrivateKey match {
              case pk: ECPrivateKeyImpl =>
                /* Force the initialization of the private key's internal array data structure.
                 * This prevents concurrency problems later on during decryption, while generating the shared secret,
                 * due to a race condition in getArrayS.
                 */
                pk.getArrayS.discard
              case _ => ()
            }
      } yield javaPrivateKey
    }

    (privateKey: @unchecked) match {
      case sigKey: SigningPrivateKey =>
        sigKey.scheme match {
          case SigningKeyScheme.Ed25519 if sigKey.format == CryptoKeyFormat.Raw =>
            val privateKeyInfo = new PrivateKeyInfo(
              new AlgorithmIdentifier(EdECObjectIdentifiers.id_Ed25519),
              new DEROctetString(privateKey.key.toByteArray),
            )
            convertFromPkcs8(privateKeyInfo.getEncoded, "Ed25519")
          case SigningKeyScheme.EcDsaP256 if sigKey.format == CryptoKeyFormat.Der =>
            convertFromPkcs8(privateKey.key.toByteArray, "EC")
          case SigningKeyScheme.EcDsaP384 if sigKey.format == CryptoKeyFormat.Der =>
            convertFromPkcs8(privateKey.key.toByteArray, "EC")
          case _ =>
            val expectedFormat = sigKey.scheme match {
              case SigningKeyScheme.Ed25519 => CryptoKeyFormat.Raw
              case SigningKeyScheme.EcDsaP256 => CryptoKeyFormat.Der
              case SigningKeyScheme.EcDsaP384 => CryptoKeyFormat.Der
            }
            Either.left[JceJavaKeyConversionError, JPrivateKey](
              JceJavaKeyConversionError.UnsupportedKeyFormat(sigKey.format, expectedFormat)
            )
        }
      case encKey: EncryptionPrivateKey =>
        encKey.scheme match {
          case EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm
              if encKey.format == CryptoKeyFormat.Der =>
            convertFromPkcs8(privateKey.key.toByteArray, "EC")
          case EncryptionKeyScheme.EciesP256HmacSha256Aes128Cbc
              if encKey.format == CryptoKeyFormat.Der =>
            convertFromPkcs8(privateKey.key.toByteArray, "EC")
          case EncryptionKeyScheme.Rsa2048OaepSha256 if encKey.format == CryptoKeyFormat.Der =>
            convertFromPkcs8(privateKey.key.toByteArray, "RSA")
          case _ =>
            Either.left[JceJavaKeyConversionError, JPrivateKey](
              JceJavaKeyConversionError.UnsupportedKeyFormat(encKey.format, CryptoKeyFormat.Der)
            )
        }
    }
  }

}

sealed trait JceJavaKeyConversionError extends Product with Serializable with PrettyPrinting

object JceJavaKeyConversionError {

  final case class GeneralError(error: Exception) extends JceJavaKeyConversionError {
    override def pretty: Pretty[GeneralError] =
      prettyOfClass(unnamedParam(_.error))
  }

  final case class UnsupportedKeyFormat(format: CryptoKeyFormat, expectedFormat: CryptoKeyFormat)
      extends JceJavaKeyConversionError {
    override def pretty: Pretty[UnsupportedKeyFormat] =
      prettyOfClass(param("format", _.format), param("expected format", _.expectedFormat))
  }

  final case class InvalidKey(error: String) extends JceJavaKeyConversionError {
    override def pretty: Pretty[InvalidKey] =
      prettyOfClass(unnamedParam(_.error.unquoted))
  }

}
