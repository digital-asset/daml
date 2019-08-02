// Copyright (c) 2019 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.digitalasset.jwt

import java.nio.charset.Charset

import com.auth0.jwt.algorithms.Algorithm
import scalaz.syntax.traverse._
import scalaz.{Show, \/}

object JwtSigner {

  private val charset = Charset.forName("ASCII")

  private val base64encoder = java.util.Base64.getUrlEncoder.withoutPadding()

  object HMAC256 {
    def sign(jwt: domain.DecodedJwt[String], secret: String): Error \/ domain.Jwt =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- \/.fromTryCatchNonFatal(Algorithm.HMAC256(secret))
          .leftMap(e => Error(Symbol("HMAC256.sign"), e.getMessage))

        signature <- \/.fromTryCatchNonFatal(algorithm.sign(base64Jwt.header, base64Jwt.payload))
          .leftMap(e => Error(Symbol("HMAC256.sign"), e.getMessage))

        base64Signature <- base64Encode(signature)

      } yield
        domain.Jwt(
          s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}")
  }

  private def str(bs: Array[Byte]) = new String(bs, charset)

  @SuppressWarnings(Array("org.wartremover.warts.Any"))
  private def base64Encode(a: domain.DecodedJwt[String]): Error \/ domain.DecodedJwt[Array[Byte]] =
    a.traverse(base64Encode)

  private def base64Encode(str: String): Error \/ Array[Byte] =
    \/.fromTryCatchNonFatal(base64encoder.encode(str.getBytes))
      .leftMap(e => Error('base64Encode, "Base64 encoding failed. Cause: " + e.getMessage))

  private def base64Encode(bs: Array[Byte]): Error \/ Array[Byte] =
    \/.fromTryCatchNonFatal(base64encoder.encode(bs))
      .leftMap(e => Error('base64Encode, "Base64 encoding failed. Cause: " + e.getMessage))

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwtValidator.Error: ${e.what}, ${e.message}")
  }
}
