// Copyright (c) 2023 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import java.nio.charset.Charset
import java.security.interfaces.{ECPrivateKey, RSAPrivateKey}

import com.auth0.jwt.algorithms.Algorithm
import scalaz.syntax.traverse._
import scalaz.{Show, \/}
import scalaz.syntax.show._

object JwtSigner {

  private val charset = Charset.forName("ASCII")

  object HMAC256 {
    def sign(jwt: domain.DecodedJwt[String], secret: String): Error \/ domain.Jwt =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- \/.attempt(Algorithm.HMAC256(secret))(e =>
          Error(Symbol("HMAC256.sign"), e.getMessage)
        )

        signature <- \/.attempt(algorithm.sign(base64Jwt.header, base64Jwt.payload))(e =>
          Error(Symbol("HMAC256.sign"), e.getMessage)
        )

        base64Signature <- base64Encode(signature)

      } yield domain.Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  object RSA256 {
    def sign(jwt: domain.DecodedJwt[String], privateKey: RSAPrivateKey): Error \/ domain.Jwt =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- \/.attempt(Algorithm.RSA256(null, privateKey))(e =>
          Error(Symbol("RSA256.sign"), e.getMessage)
        )

        signature <- \/.attempt(algorithm.sign(base64Jwt.header, base64Jwt.payload))(e =>
          Error(Symbol("RSA256.sign"), e.getMessage)
        )

        base64Signature <- base64Encode(signature)

      } yield domain.Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  object ECDSA {
    def sign(
        jwt: domain.DecodedJwt[String],
        privateKey: ECPrivateKey,
        algorithm: ECPrivateKey => Algorithm,
    ): Error \/ domain.Jwt =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- \/.attempt(algorithm(privateKey))(e =>
          Error(Symbol(algorithm.getClass.getTypeName), e.getMessage)
        )

        signature <- \/.attempt(algorithm.sign(base64Jwt.header, base64Jwt.payload))(e =>
          Error(Symbol(algorithm.getClass.getTypeName), e.getMessage)
        )

        base64Signature <- base64Encode(signature)

      } yield domain.Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  private def str(bs: Array[Byte]) = new String(bs, charset)

  private def base64Encode(a: domain.DecodedJwt[String]): Error \/ domain.DecodedJwt[Array[Byte]] =
    a.traverse(base64Encode)

  private def base64Encode(str: String): Error \/ Array[Byte] =
    base64Encode(str.getBytes)

  private def base64Encode(bs: Array[Byte]): Error \/ Array[Byte] =
    Base64
      .encodeWithoutPadding(bs)
      .leftMap(e => Error(Symbol("base64Encode"), e.shows))

  final case class Error(what: Symbol, message: String)

  object Error {
    implicit val showInstance: Show[Error] =
      Show.shows(e => s"JwtSigner.Error: ${e.what}, ${e.message}")
  }
}
