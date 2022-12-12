// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.io.{File, FileInputStream}
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.security.cert.CertificateFactory
import java.security.interfaces.{ECPublicKey, RSAPrivateKey, RSAPublicKey}
import java.security.spec.PKCS8EncodedKeySpec
import java.security.KeyFactory

import scalaz.Show
import scalaz.syntax.show._

import scala.util.{Try, Using}

object KeyUtils {
  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"KeyUtils.Error: ${e.what}, ${e.message}")
  }

  private val mimeCharSet = StandardCharsets.ISO_8859_1

  /** Reads an RSA public key from a X509 encoded file.
    * These usually have the .crt file extension.
    */
  def readRSAPublicKeyFromCrt(file: File): Try[RSAPublicKey] =
    Using(new FileInputStream(file))(
      CertificateFactory
        .getInstance("X.509")
        .generateCertificate(_)
        .getPublicKey
        .asInstanceOf[RSAPublicKey]
    )

  /** Reads an EC public key from a X509 encoded file.
    * These usually have the .crt file extension.
    */
  def readECPublicKeyFromCrt(file: File): Try[ECPublicKey] =
    Using(new FileInputStream(file))(
      CertificateFactory
        .getInstance("X.509")
        .generateCertificate(_)
        .getPublicKey
        .asInstanceOf[ECPublicKey]
    )

  /** Reads a RSA private key from a PEM/PKCS#8 file.
    * These usually have the .pem file extension.
    */
  def readRSAPrivateKeyFromPem(file: File): Try[RSAPrivateKey] =
    for {
      fileContent <- Try(Files.readAllBytes(file.toPath))

      // Remove PEM container header and footer
      pemContent <- Try(
        new String(fileContent, mimeCharSet)
          .replaceFirst("-----BEGIN ([A-Z ])*-----\n", "")
          .replaceFirst("\n-----END ([A-Z ])*-----\n", "")
          .replace("\r", "")
          .replace("\n", "")
      )

      // Base64-decode the PEM container content
      decoded <- Base64
        .decode(pemContent)
        .leftMap(e => new RuntimeException(e.shows))
        .toEither
        .toTry

      // Interpret the container content as PKCS#8
      key <- Try {
        val kf = KeyFactory.getInstance("RSA")
        val keySpec = new PKCS8EncodedKeySpec(decoded.getBytes)
        kf.generatePrivate(keySpec).asInstanceOf[RSAPrivateKey]
      }
    } yield key

  /** Reads a RSA private key from a binary file (PKCS#8, DER).
    * To generate this file from a .pem file, use the following command:
    * openssl pkcs8 -topk8 -inform PEM -outform DER -in private-key.pem -nocrypt > private-key.der
    */
  def readRSAPrivateKeyFromDer(file: File): Try[RSAPrivateKey] =
    for {
      fileContent <- Try(Files.readAllBytes(file.toPath))

      // Interpret the container content as PKCS#8
      key <- Try {
        val kf = KeyFactory.getInstance("RSA")
        val keySpec = new PKCS8EncodedKeySpec(fileContent)
        kf.generatePrivate(keySpec).asInstanceOf[RSAPrivateKey]
      }
    } yield key

  /** Generates a JWKS JSON object for the given map of KeyID->Key
    *
    * Note: this uses the same format as Google OAuth, see https://www.googleapis.com/oauth2/v3/certs
    */
  def generateJwks(keys: Map[String, RSAPublicKey]): String = {
    def generateKeyEntry(keyId: String, key: RSAPublicKey): String =
      s"""    {
         |      "kid": "$keyId",
         |      "kty": "RSA",
         |      "alg": "RS256",
         |      "use": "sig",
         |      "e": "${java.util.Base64.getUrlEncoder
          .encodeToString(key.getPublicExponent.toByteArray)}",
         |      "n": "${java.util.Base64.getUrlEncoder.encodeToString(key.getModulus.toByteArray)}"
         |    }""".stripMargin

    s"""
       |{
       |  "keys": [
       |${keys.toList.map { case (keyId, key) => generateKeyEntry(keyId, key) }.mkString(",\n")}
       |  ]
       |}
    """.stripMargin
  }
}
