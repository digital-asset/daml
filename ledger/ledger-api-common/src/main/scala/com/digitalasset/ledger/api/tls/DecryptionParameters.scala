// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import java.io.File
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.util.Base64
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
) {

  def decrypt(encrypted: File): Array[Byte] = {
    val bytes = Files.readAllBytes(encrypted.toPath)
    decrypt(bytes)
  }

  private[tls] def algorithm: String = {
    transformation.split("/")(0)
  }

  private def decodeBase64OrGetVerbatim(encrypted: Array[Byte]): Array[Byte] = {
    val potentialBase64Char =
      "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz/+=\n\r".getBytes()
    encrypted.find(!potentialBase64Char.contains(_)) match {
      case None => Try(Base64.getMimeDecoder.decode(encrypted)).getOrElse(encrypted)
      case _ => encrypted
    }
  }

  private[tls] def decrypt(encrypted: Array[Byte]): Array[Byte] = {
    val key: Array[Byte] = Hex.decodeHex(keyInHex)
    val secretKey = new SecretKeySpec(key, algorithm)
    val iv: Array[Byte] = Hex.decodeHex(initializationVectorInHex)
    val cipher = Cipher.getInstance(transformation)
    val ivParameterSpec = new IvParameterSpec(iv)
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
  )
}

object DecryptionParameters {

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

}
