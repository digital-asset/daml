// Copyright (c) 2024 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.crypto

import com.daml.nonempty.NonEmpty
import com.digitalasset.canton.logging.pretty.{Pretty, PrettyPrinting}
import com.digitalasset.canton.util.ErrorUtil
import com.google.protobuf.ByteString
import org.bouncycastle.asn1.edec.EdECObjectIdentifiers
import org.bouncycastle.asn1.pkcs.PKCSObjectIdentifiers
import org.bouncycastle.asn1.sec.SECObjectIdentifiers
import org.bouncycastle.asn1.x509.{AlgorithmIdentifier, SubjectPublicKeyInfo}
import org.bouncycastle.asn1.x9.X9ObjectIdentifiers
import org.bouncycastle.openssl.PEMParser

import java.io.{IOException, StringReader}
import java.security.PublicKey as JPublicKey

trait JavaKeyConverter {

  /** Convert to Java public key */
  def toJava(
      publicKey: PublicKey
  ): Either[JavaKeyConversionError, (AlgorithmIdentifier, JPublicKey)]

  /** Convert a Java public key into a Canton signing public key. */
  def fromJavaSigningKey(
      publicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
  ): Either[JavaKeyConversionError, SigningPublicKey]

  def fromJavaEncryptionKey(
      publicKey: JPublicKey,
      algorithmIdentifier: AlgorithmIdentifier,
  ): Either[JavaKeyConversionError, EncryptionPublicKey]
}

object JavaKeyConverter {

  def toSigningKeyScheme(
      algoId: AlgorithmIdentifier
  ): Either[JavaKeyConversionError, SigningKeyScheme] =
    algoId.getAlgorithm match {
      case X9ObjectIdentifiers.ecdsa_with_SHA256 | SECObjectIdentifiers.secp256r1 =>
        Right(SigningKeyScheme.EcDsaP256)
      case X9ObjectIdentifiers.ecdsa_with_SHA384 | SECObjectIdentifiers.secp384r1 =>
        Right(SigningKeyScheme.EcDsaP384)
      case EdECObjectIdentifiers.id_Ed25519 => Right(SigningKeyScheme.Ed25519)
      case unsupportedIdentifier =>
        Left(JavaKeyConversionError.UnsupportedAlgorithm(algoId))
    }

  @SuppressWarnings(Array("org.wartremover.warts.AsInstanceOf"))
  def convertPublicKeyFromPemToDer(pubKeyPEM: String): Either[String, ByteString] = {
    val pemParser: PEMParser = new PEMParser(new StringReader(pubKeyPEM))
    try {
      Option(pemParser.readObject) match {
        case Some(spki: SubjectPublicKeyInfo) =>
          Right(ByteString.copyFrom(spki.getEncoded))
        case Some(_) =>
          Left("unexpected type conversion")
        case None =>
          Left("could not parse public key info from PEM format")
      }
    } catch {
      case e: IOException =>
        Left(
          s"failed to convert public key from PEM to DER format: ${ErrorUtil.messageWithStacktrace(e)}"
        )
    } finally {
      pemParser.close()
    }
  }

  def toEncryptionKeyScheme(
      algoId: AlgorithmIdentifier
  ): Either[JavaKeyConversionError, EncryptionKeyScheme] =
    algoId.getAlgorithm match {
      case X9ObjectIdentifiers.ecdsa_with_SHA256 | SECObjectIdentifiers.secp256r1 =>
        // TODO(i12757): It maps to two ECIES schemes right now, need to disentangle key scheme from algorithm
        Right(EncryptionKeyScheme.EciesP256HkdfHmacSha256Aes128Gcm)
      case PKCSObjectIdentifiers.id_RSAES_OAEP =>
        Right(EncryptionKeyScheme.Rsa2048OaepSha256)
      case unsupportedIdentifier =>
        Left(JavaKeyConversionError.UnsupportedAlgorithm(algoId))
    }

}

sealed trait JavaKeyConversionError extends Product with Serializable with PrettyPrinting

object JavaKeyConversionError {

  final case class GeneralError(error: Exception) extends JavaKeyConversionError {
    override def pretty: Pretty[GeneralError] =
      prettyOfClass(unnamedParam(_.error))
  }

  final case class UnsupportedAlgorithm(algorithmIdentifier: AlgorithmIdentifier)
      extends JavaKeyConversionError {
    override def pretty: Pretty[UnsupportedAlgorithm] =
      prettyOfClass(unnamedParam(_.algorithmIdentifier.toString.unquoted))
  }

  final case class UnsupportedKeyFormat(format: CryptoKeyFormat, expectedFormat: CryptoKeyFormat)
      extends JavaKeyConversionError {
    override def pretty: Pretty[UnsupportedKeyFormat] =
      prettyOfClass(param("format", _.format), param("expected format", _.expectedFormat))
  }

  final case class UnsupportedSigningKeyScheme(
      scheme: SigningKeyScheme,
      supportedSchemes: NonEmpty[Set[SigningKeyScheme]],
  ) extends JavaKeyConversionError {
    override def pretty: Pretty[UnsupportedSigningKeyScheme] =
      prettyOfClass(
        param("scheme", _.scheme),
        param("supported schemes", _.supportedSchemes),
      )
  }

  final case class UnsupportedEncryptionKeyScheme(
      scheme: EncryptionKeyScheme,
      supportedSchemes: NonEmpty[Set[EncryptionKeyScheme]],
  ) extends JavaKeyConversionError {
    override def pretty: Pretty[UnsupportedEncryptionKeyScheme] =
      prettyOfClass(
        param("scheme", _.scheme),
        param("supported schemes", _.supportedSchemes),
      )
  }

  final case class KeyStoreError(error: String) extends JavaKeyConversionError {
    override def pretty: Pretty[KeyStoreError] =
      prettyOfClass(unnamedParam(_.error.unquoted))
  }

  final case class InvalidKey(error: String) extends JavaKeyConversionError {
    override def pretty: Pretty[InvalidKey] =
      prettyOfClass(unnamedParam(_.error.unquoted))
  }

}
