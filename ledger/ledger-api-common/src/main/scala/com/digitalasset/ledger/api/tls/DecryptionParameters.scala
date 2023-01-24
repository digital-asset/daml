// Copyright (c) 2022 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import org.slf4j.LoggerFactory
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.Base64
import javax.crypto.Cipher
import javax.crypto.spec.{GCMParameterSpec, SecretKeySpec}
import scala.util.{Try, Using}

final class PrivateKeyDecryptionException(cause: Throwable) extends Exception(cause)

/** @param transformation: "<algorithm>/<mode>/<padding>", for example: "AES/CBC/PKCS5Padding"
  * @param keyInHex: Hex encoded bytes of key.
  * @param initializationVectorInHex: Hex encoded bytes of IV.
  *
  * Decrypts a file encrypted by a transformation using AES algorithm.
  * See also: https://docs.oracle.com/javase/8/docs/technotes/guides/security/crypto/CryptoSpec.html
  */
case class DecryptionParameters(
    transformation: String,
    keyInHex: String,
    initializationVectorInHex: String,
    tagLength: Int,
) {
  import DecryptionParameters._

  def decrypt(encrypted: File): Array[Byte] = {
    if (transformation != AES_GCM_NOPADDING) {
      throw new IllegalArgumentException(
        s"Only $AES_GCM_NOPADDING is supported, but ${transformation} was provided."
      )
    }
    val bytes = Files.readAllBytes(encrypted.toPath)
    decrypt(bytes)
  }

  private[tls] def algorithm: String = {
    transformation.split("/")(0)
  }

  private[tls] def decrypt(encrypted: Array[Byte]): Array[Byte] = {
    val key: Array[Byte] = Hex.decodeHex(keyInHex)
    val secretKey = new SecretKeySpec(key, algorithm)
    val iv: Array[Byte] = Hex.decodeHex(initializationVectorInHex)
    val cipher = Cipher.getInstance(transformation)
    val ivParameterSpec = new GCMParameterSpec(tagLength, iv)
    cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParameterSpec)
    val binary = decodeBase64OrGetVerbatim(encrypted)
    cipher.doFinal(binary)
  }
}

object DecryptionParametersJsonProtocol extends DefaultJsonProtocol {
  implicit val decryptionParams: RootJsonFormat[DecryptionParameters] = jsonFormat(
    DecryptionParameters.apply,
    "algorithm",
    "key",
    "iv",
    "tag_length",
  )
}

object DecryptionParameters {

  val AES_GCM_NOPADDING = "AES/GCM/NoPadding"

  /** Creates an instance of [[DecryptionParameters]] by fetching necessary information from an URL
    */
  def fromSecretsServer(url: SecretsUrl): DecryptionParameters = {
    val body = fetchPayload(url)
    parsePayload(body)
  }

  private[tls] def fetchPayload(url: SecretsUrl): String =
    Using.resource(url.openStream()) { stream =>
      IOUtils.toString(stream, StandardCharsets.UTF_8.name())
    }

  private[tls] def parsePayload(payload: String): DecryptionParameters = {
    import DecryptionParametersJsonProtocol._
    import spray.json._
    val jsonAst: JsValue = payload.parseJson
    jsonAst.convertTo[DecryptionParameters]
  }

  private val logger = LoggerFactory.getLogger(getClass)

  // According to MIME's section of java.util.Base64 javadoc "All line separators or other
  // characters not found in the base64 alphabet table are ignored in decoding operation."
  // For this reason a buffer needs to be screened whether it contains only the allowed
  // Base64 characters before attempting to decode it.
  private[tls] def decodeBase64OrGetVerbatim(encrypted: Array[Byte]): Array[Byte] = {
    val allowedBase64Char =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz/+=\n\r".getBytes(
        StandardCharsets.UTF_8
      )
    encrypted.find(!allowedBase64Char.contains(_)) match {
      case None =>
        logger.debug(s"Encrypted key contains only MIME Base64 characters. Attempting to decode")
        Try(Base64.getMimeDecoder.decode(encrypted)).getOrElse(encrypted)
      case _ =>
        logger.debug(s"Encrypted key contains non MIME Base64 characters. Using it verbatim")
        encrypted
    }
  }

}
