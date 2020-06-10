// Copyright (c) 2020 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.lf.engine.trigger

import com.daml.lf.data.Ref.Party
import akka.http.scaladsl.model.headers.{Authorization, BasicHttpCredentials}
import akka.http.scaladsl.model.HttpRequest
import java.nio.charset.StandardCharsets
import java.util
import java.security.MessageDigest
import javax.crypto.Cipher
import javax.crypto.spec.SecretKeySpec

case class Unauthorized(message: String) extends Error(message)
case class EncryptedToken(token: String)
case class UnencryptedToken(token: String)

object TokenManagement {

  // TL;DR You can store the SALT in plaintext without any form of
  // obfuscation or encryption, but don't just give it out to anyone
  // that wants it.
  private val SALT = "jMhKlOuJnM34G6NHkqo9V010GhLAqOpF0BePojHgh1HgNg8^72k"

  // Given 'key', use 'SALT' to produce an AES (Advanced Encryption
  // Standard) secret key specification. This utility is called from
  // the 'encrypt' and 'decrypt' functions.
  private def keyToSpec(key: String): SecretKeySpec = {
    var keyBytes: Array[Byte] = (SALT + key).getBytes("UTF-8")
    val sha: MessageDigest = MessageDigest.getInstance("SHA-1")
    keyBytes = sha.digest(keyBytes)
    keyBytes = util.Arrays.copyOf(keyBytes, 16)
    new SecretKeySpec(keyBytes, "AES")
  }

  // AES encrypt 'value' given 'key'. Proceed by first encrypting the
  // value and then base64 encode the result (the resulting string
  // consists of characters strictly in the set [a-z], [A-Z], [0-9] +
  // and /.
  private def encrypt(key: String, value: UnencryptedToken): EncryptedToken = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5Padding")
    cipher.init(Cipher.ENCRYPT_MODE, keyToSpec(key))
    val bytes = java.util.Base64.getEncoder
      .encode(cipher.doFinal(value.token.getBytes("UTF-8")))
    EncryptedToken(new String(bytes, StandardCharsets.UTF_8))
  }

  // AES decrypt 'value' given 'key'. Proceed by first decoding from
  // base64 then decrypt the result.
  private def decrypt(key: String, value: EncryptedToken): UnencryptedToken = {
    val cipher: Cipher = Cipher.getInstance("AES/ECB/PKCS5PADDING")
    cipher.init(Cipher.DECRYPT_MODE, keyToSpec(key))
    UnencryptedToken(
      new String(
        cipher.doFinal(java.util.Base64.getDecoder.decode(value.token)),
        StandardCharsets.UTF_8))
  }

  // Utility to get the username and password out of a basic auth
  // token. By construction we ensure that there will always be two
  // components and that the first component is a syntactically valid
  // party identifier (see 'findCredentials').
  def decodeCredentials(
      key: String,
      credentials: UserCredentials): (com.daml.ledger.api.refinements.ApiTypes.Party, String) = {
    val components = decrypt(key, credentials.token).token.split(":")
    (com.daml.ledger.api.refinements.ApiTypes.Party(components(0)), components(1))
  }

  // Parse the user credentials out of a request's headers.
  def findCredentials(key: String, req: HttpRequest): Either[String, UserCredentials] = {
    req.headers
      .collectFirst {
        case Authorization(c @ BasicHttpCredentials(username, password)) => {
          val token = c.token()
          val bytes = java.util.Base64.getDecoder.decode(token.getBytes())
          UserCredentials(encrypt(key, UnencryptedToken(new String(bytes, StandardCharsets.UTF_8))))
        }
      } match {
      // Check the given username conforms to the syntactic
      // requirements of a party identifier.
      case Some(credentials) =>
        decodeCredentials(key, credentials) match {
          case (party, _) =>
            val ident = party.toString()
            if (Party.fromString(ident).isRight) {
              Right(credentials)
            } else {
              Left("invalid party identifier '" + ident + "'")
            }
        }
      case None => Left("missing Authorization header with Basic Token")
    }
  }
}
