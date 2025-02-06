// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto.provider.jce

import cats.syntax.either.*
import com.digitalasset.canton.crypto.*
import com.digitalasset.canton.discard.Implicits.DiscardOps
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
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

    def convertFromX509Spki(
        x509PublicKey: Array[Byte],
        keyInstance: String,
    ): Either[JceJavaKeyConversionError, JPublicKey] = {
      val expectedFormat = CryptoKeyFormat.DerX509Spki

      for {
        _ <- CryptoKeyValidation.ensureFormat(
          publicKey.format,
          Set(expectedFormat),
          _ => JceJavaKeyConversionError.UnsupportedKeyFormat(publicKey.format, expectedFormat),
        )
        x509KeySpec = new X509EncodedKeySpec(x509PublicKey)
        keyFactory <- Either
          .catchOnly[NoSuchAlgorithmException](
            KeyFactory.getInstance(keyInstance, JceSecurityProvider.bouncyCastleProvider)
          )
          .leftMap(JceJavaKeyConversionError.GeneralError.apply)
        javaPublicKey <- Either
          .catchOnly[InvalidKeySpecException](keyFactory.generatePublic(x509KeySpec))
          .leftMap(err => JceJavaKeyConversionError.InvalidKey(show"$err"))
      } yield javaPublicKey
    }

    (publicKey: @unchecked) match {
      case sigKey: SigningPublicKey =>
        sigKey.keySpec match {
          case SigningKeySpec.EcCurve25519 =>
            convertFromX509Spki(publicKey.key.toByteArray, "Ed25519")
          case SigningKeySpec.EcP256 | SigningKeySpec.EcP384 | SigningKeySpec.EcSecp256k1 =>
            convertFromX509Spki(publicKey.key.toByteArray, "EC")
        }
      case encKey: EncryptionPublicKey =>
        encKey.keySpec match {
          case EncryptionKeySpec.EcP256 =>
            convertFromX509Spki(publicKey.key.toByteArray, "EC")
          case EncryptionKeySpec.Rsa2048 =>
            convertFromX509Spki(publicKey.key.toByteArray, "RSA")
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
      val expectedFormat = CryptoKeyFormat.DerPkcs8Pki

      for {
        _ <- CryptoKeyValidation.ensureFormat(
          privateKey.format,
          Set(expectedFormat),
          _ => JceJavaKeyConversionError.UnsupportedKeyFormat(privateKey.format, expectedFormat),
        )
        pkcs8KeySpec = new PKCS8EncodedKeySpec(pkcs8PrivateKey)
        keyFactory <- Either
          .catchOnly[NoSuchAlgorithmException](
            KeyFactory.getInstance(keyInstance, JceSecurityProvider.bouncyCastleProvider)
          )
          .leftMap(JceJavaKeyConversionError.GeneralError.apply)
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
        sigKey.keySpec match {
          case SigningKeySpec.EcCurve25519 =>
            convertFromPkcs8(privateKey.key.toByteArray, "Ed25519")
          case SigningKeySpec.EcP256 | SigningKeySpec.EcP384 | SigningKeySpec.EcSecp256k1 =>
            convertFromPkcs8(privateKey.key.toByteArray, "EC")
        }
      case encKey: EncryptionPrivateKey =>
        encKey.keySpec match {
          // EcP384 is not a supported encryption key because the current accepted encryption algorithms do not support it
          case EncryptionKeySpec.EcP256 =>
            convertFromPkcs8(privateKey.key.toByteArray, "EC")
          case EncryptionKeySpec.Rsa2048 =>
            convertFromPkcs8(privateKey.key.toByteArray, "RSA")
        }
    }
  }

}

sealed trait JceJavaKeyConversionError extends Product with Serializable with PrettyPrinting

object JceJavaKeyConversionError {

  final case class GeneralError(error: Exception) extends JceJavaKeyConversionError {
    override protected def pretty: Pretty[GeneralError] =
      prettyOfClass(unnamedParam(_.error))
  }

  final case class UnsupportedKeyFormat(format: CryptoKeyFormat, expectedFormat: CryptoKeyFormat)
      extends JceJavaKeyConversionError {
    override protected def pretty: Pretty[UnsupportedKeyFormat] =
      prettyOfClass(param("format", _.format), param("expected format", _.expectedFormat))
  }

  final case class InvalidKey(error: String) extends JceJavaKeyConversionError {
    override protected def pretty: Pretty[InvalidKey] =
      prettyOfClass(unnamedParam(_.error.unquoted))
  }

}
