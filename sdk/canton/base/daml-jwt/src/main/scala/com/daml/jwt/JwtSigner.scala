// Copyright (c) 2026 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwt.algorithms.Algorithm

import java.nio.charset.Charset
import java.security.interfaces.{ECPrivateKey, RSAPrivateKey}

object JwtSigner extends WithExecuteUnsafe {

  private val charset = Charset.forName("ASCII")

  object HMAC256 {
    def sign(jwt: DecodedJwt[String], secret: String): Either[Error, Jwt] =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- executeUnsafe(Algorithm.HMAC256(secret), Symbol("HMAC256.sign"))

        signature <- executeUnsafe(
          algorithm.sign(base64Jwt.header, base64Jwt.payload),
          Symbol("HMAC256.sign"),
        )

        base64Signature <- base64Encode(signature)

      } yield Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  @SuppressWarnings(Array("org.wartremover.warts.Null"))
  object RSA256 {
    def sign(jwt: DecodedJwt[String], privateKey: RSAPrivateKey): Either[Error, Jwt] =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- executeUnsafe(Algorithm.RSA256(null, privateKey), Symbol("RSA256.sign"))

        signature <- executeUnsafe(
          algorithm.sign(base64Jwt.header, base64Jwt.payload),
          Symbol("RSA256.sign"),
        )

        base64Signature <- base64Encode(signature)

      } yield Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  object ECDSA {
    def sign(
        jwt: DecodedJwt[String],
        privateKey: ECPrivateKey,
        algorithm: ECPrivateKey => Algorithm,
    ): Either[Error, Jwt] =
      for {
        base64Jwt <- base64Encode(jwt)

        algorithm <- executeUnsafe(algorithm(privateKey), Symbol(algorithm.getClass.getTypeName))

        signature <- executeUnsafe(
          algorithm.sign(base64Jwt.header, base64Jwt.payload),
          Symbol(algorithm.getClass.getTypeName),
        )

        base64Signature <- base64Encode(signature)

      } yield Jwt(
        s"${str(base64Jwt.header): String}.${str(base64Jwt.payload)}.${str(base64Signature): String}"
      )
  }

  private def str(bs: Array[Byte]) = new String(bs, charset)

  private def base64Encode(a: DecodedJwt[String]): Either[Error, DecodedJwt[Array[Byte]]] =
    a.transform(base64Encode)

  private def base64Encode(str: String): Either[Error, Array[Byte]] =
    base64Encode(str.getBytes)

  private def base64Encode(bs: Array[Byte]): Either[Error, Array[Byte]] =
    Base64
      .encodeWithoutPadding(bs)
      .left
      .map(_.within(Symbol("JwtSigner.base64Encode")))
}
