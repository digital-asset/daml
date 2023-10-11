// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.canton.platform.apiserver.meteringreport

import com.daml.crypto.MacPrototype
import spray.json.DefaultJsonProtocol.{jsonFormat3, *}
import spray.json.{JsValue, JsonFormat, RootJsonFormat, deserializationError}

import java.util.Base64
import javax.crypto.KeyGenerator
import javax.crypto.spec.SecretKeySpec
import scala.util.Try

object HmacSha256 {

  def toBase64(bytes: Array[Byte]): String = Base64.getUrlEncoder.encodeToString(bytes)

  def fromBase64(base64: String): Either[Throwable, Array[Byte]] = Try(
    Base64.getUrlDecoder.decode(base64)
  ).toEither

  final case class Bytes(bytes: Array[Byte]) {
    override def equals(obj: Any): Boolean = obj match {
      case Bytes(other) => other.sameElements(bytes)
      case _ => false
    }
    override def toString: String = toBase64
    def toBase64: String = HmacSha256.toBase64(bytes)
  }

  implicit val BytesFormat: RootJsonFormat[Bytes] = new RootJsonFormat[Bytes] {
    private[this] val base = implicitly[JsonFormat[String]]
    override def write(obj: Bytes): JsValue = base.write(obj.toBase64)
    override def read(json: JsValue): Bytes = fromBase64(base.read(json))
      .map(Bytes)
      .fold(deserializationError(s"Failed to deserialize $json", _), identity)
  }

  /** @param scheme  - a long lived name that be associated with, and only with, this key
    * @param encoded - the encoded bytes of a HmacSha256 secret key
    * @param algorithm - the key algorithm
    */
  final case class Key(scheme: String, encoded: Bytes, algorithm: String)

  implicit val KeyFormat: RootJsonFormat[Key] = jsonFormat3(Key.apply)

  // The key used for both mac and key generation as defined in
  // https://docs.oracle.com/javase/9/docs/specs/security/standard-names.html
  val algorithm = "HmacSHA256"
  private val macPrototype = new MacPrototype(algorithm)

  def compute(key: Key, message: Array[Byte]): Either[Throwable, Array[Byte]] = {
    Try {
      val mac = macPrototype.newMac
      val secretKey = new SecretKeySpec(key.encoded.bytes, key.algorithm)
      mac.init(secretKey)
      mac.doFinal(message)
    }.toEither
  }

  def generateKey(scheme: String): Key = {
    val generator = KeyGenerator.getInstance(algorithm)
    val key = generator.generateKey()
    Key(scheme, Bytes(key.getEncoded), key.getAlgorithm)
  }

}
