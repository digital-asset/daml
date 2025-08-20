// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.RSAKeyProvider

import java.io.File
import java.security.interfaces.{ECPublicKey, RSAPublicKey}

abstract class JwtVerifierBase {
  def verify(jwt: Jwt): Either[Error, DecodedJwt[String]]
}

class JwtVerifier(val verifier: com.auth0.jwt.interfaces.JWTVerifier)
    extends JwtVerifierBase
    with WithExecuteUnsafe {

  def verify(jwt: Jwt): Either[Error, DecodedJwt[String]] =
    // The auth0 library verification already fails if the token has expired,
    // but we still need to do manual expiration checks in ongoing streams
    executeUnsafe(verifier.verify(jwt.value), Symbol("JwtVerifier.verify"))
      // TODO (i26199)- possible place to add expiration time check
      .map(a => DecodedJwt(header = a.getHeader, payload = a.getPayload))
      .flatMap(base64Decode)

  private def base64Decode(jwt: DecodedJwt[String]): Either[Error, DecodedJwt[String]] =
    jwt.transform(Base64.decode).left.map(_.within(Symbol("JwtVerifier.base64Decode")))

}

// HMAC256 validator factory
object HMAC256Verifier extends Leeway with WithExecuteUnsafe {
  def apply(
      secret: String,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {
        val algorithm = Algorithm.HMAC256(secret)
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier)
      },
      Symbol("HMAC256"),
    )
}

// ECDSA validator factory
object ECDSAVerifier extends Leeway with WithExecuteUnsafe {
  def apply(
      algorithm: Algorithm,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier)
      },
      Symbol(algorithm.getName),
    )

  def fromCrtFile(
      path: String,
      algorithmPublicKey: ECPublicKey => Algorithm,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  ): Either[Error, JwtVerifier] =
    for {
      key <- KeyUtils
        .readECPublicKeyFromCrt(new File(path))
        .toEither
        .left
        .map(e => Error(Symbol("ECDSAVerifier.fromCrtFile"), e.getMessage))
      verifier <- ECDSAVerifier(algorithmPublicKey(key), jwtTimestampLeeway)
    } yield verifier
}

// RSA256 validator factory
object RSA256Verifier extends Leeway with WithExecuteUnsafe {
  def apply(
      publicKey: RSAPublicKey,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {
        val algorithm = Algorithm.RSA256(publicKey, null)
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier)
      },
      Symbol("RSA256"),
    )

  def apply(keyProvider: RSAKeyProvider): Either[Error, JwtVerifier] =
    executeUnsafe(
      {

        val algorithm = Algorithm.RSA256(keyProvider)
        val verifier = getVerifier(algorithm)
        new JwtVerifier(verifier)
      },
      (Symbol("RSA256")),
    )

  def apply(
      keyProvider: RSAKeyProvider,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {

        val algorithm = Algorithm.RSA256(keyProvider)
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier)
      },
      Symbol("RSA256"),
    )

  /** Create a RSA256 validator with the key loaded from the given file. The file is assumed to be a
    * X509 encoded certificate. These typically have the .crt file extension.
    */
  def fromCrtFile(
      path: String,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
  ): Either[Error, JwtVerifier] =
    for {
      rsaKey <- KeyUtils
        .readRSAPublicKeyFromCrt(new File(path))
        .toEither
        .left
        .map(e => Error(Symbol("RSA256Verifier.fromCrtFile"), e.getMessage))
      verifier <- RSA256Verifier.apply(rsaKey, jwtTimestampLeeway)
    } yield verifier
}
