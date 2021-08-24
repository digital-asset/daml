// Copyright (c) 2021 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.ledger.api.tls

import org.apache.commons.codec.binary.Hex
import org.apache.commons.io.IOUtils
import spray.json.{DefaultJsonProtocol, RootJsonFormat}

import java.io.File
import java.net.URL
import java.nio.charset.StandardCharsets
import java.nio.file.Files
import javax.crypto.Cipher
import javax.crypto.spec.{IvParameterSpec, SecretKeySpec}
import scala.util.Using

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

  private[tls] def decrypt(encrypted: Array[Byte]): Array[Byte] = {
    val key: Array[Byte] = Hex.decodeHex(keyInHex)
    val secretKey =
      new SecretKeySpec(key, "AES") // TODO PBATKO hardcoded. Is it good enough for now?
    val iv: Array[Byte] = Hex.decodeHex(initializationVectorInHex)
    val cipher = Cipher.getInstance(transformation)
    val ivParameterSpec = new IvParameterSpec(iv)
    cipher.init(Cipher.DECRYPT_MODE, secretKey, ivParameterSpec)
    cipher.doFinal(encrypted)
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
  def fromSecretsServer(url: URL): DecryptionParameters = {
    val text = fetchPayload(url)
    parsePayload(text)
  }

  private[tls] def fetchPayload(url: URL): String = {
    val text = Using.resource(url.openStream()) { stream =>
      IOUtils.toString(stream, StandardCharsets.UTF_8.name())
    }
    text
  }

  private[tls] def parsePayload(payload: String): DecryptionParameters = {
    import DecryptionParametersJsonProtocol._
    import spray.json._
    val jsonAst: JsValue = payload.parseJson
    jsonAst.convertTo[DecryptionParameters]
  }

}
