// Copyright (c) 2025 Digital Asset (Switzerland) GmbH and/or its affiliates. All rights reserved.
// SPDX-License-Identifier: Apache-2.0

package com.daml.jwt

import com.auth0.jwt.algorithms.Algorithm
import com.auth0.jwt.interfaces.RSAKeyProvider

import java.io.File
import java.security.interfaces.{ECPublicKey, RSAPublicKey}
import java.time.{Duration, Instant}
import scala.math.Ordered.orderingToOrdered

abstract class JwtVerifierBase {
  def verify(jwt: Jwt): Either[Error, DecodedJwt[String]]
}

class JwtVerifier(
    val verifier: com.auth0.jwt.interfaces.JWTVerifier,
    val maxTokenLife: Option[Long],
) extends JwtVerifierBase
    with WithExecuteUnsafe {

  def verify(jwt: Jwt): Either[Error, DecodedJwt[String]] =
    // The auth0 library verification already fails if the token has expired,
    // but we still need to do manual expiration checks in ongoing streams
    executeUnsafe(verifier.verify(jwt.value), Symbol("JwtVerifier.verify"))
      .map(a =>
        (
          DecodedJwt(
            header = a.getHeader,
            payload = a.getPayload,
          ),
          Option(a.getExpiresAtAsInstant()),
        )
      )
      .flatMap { case (jwt, expirationOption) =>
        checkTokenLifeTime(jwt, expirationOption)
      }
      .flatMap(base64Decode)

  // Check if the expiration time is not too long and if it exists
  private def checkTokenLifeTime(
      jwt: DecodedJwt[String],
      expirationOption: Option[Instant],
  ) = {
    // TODO (i27262) use TimeProvider to get current time
    val currentTime = Instant.now()
    expirationOption
      .map { expiresAt =>
        val duration = Duration.ofMillis(expiresAt.toEpochMilli - currentTime.toEpochMilli)
        val maxTokenLifeDuration = maxTokenLife.getOrElse(Long.MaxValue)
        // We do not check for negative durations (expired token), as the JWT library already ensures that (with a leeway)
        if (duration > Duration.ofMillis(maxTokenLifeDuration)) {
          Left(Error(Symbol("JwtVerifier.verify"), s"token lifetime ($expiresAt) too long"))
        } else {
          Right(jwt)
        }
      }
      .getOrElse(
        maxTokenLife
          .map(_ => Left(Error(Symbol("JwtVerifier.verify"), "token has no expiration time")))
          .getOrElse(Right(jwt))
      )
  }

  private def base64Decode(jwt: DecodedJwt[String]): Either[Error, DecodedJwt[String]] =
    jwt.transform(Base64.decode).left.map(_.within(Symbol("JwtVerifier.base64Decode")))

}

// HMAC256 validator factory
object HMAC256Verifier extends Leeway with WithExecuteUnsafe {
  def apply(
      secret: String,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      maxTokenLife: Option[Long] = None,
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {
        val algorithm = Algorithm.HMAC256(secret)
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier, maxTokenLife)
      },
      Symbol("HMAC256"),
    )
}

// ECDSA validator factory
object ECDSAVerifier extends Leeway with WithExecuteUnsafe {
  def apply(
      algorithm: Algorithm,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      maxTokenLife: Option[Long] = None,
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier, maxTokenLife)
      },
      Symbol(algorithm.getName),
    )

  def fromCrtFile(
      path: String,
      algorithmPublicKey: ECPublicKey => Algorithm,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      maxTokenLife: Option[Long] = None,
  ): Either[Error, JwtVerifier] =
    for {
      key <- KeyUtils
        .readECPublicKeyFromCrt(new File(path))
        .toEither
        .left
        .map(e => Error(Symbol("ECDSAVerifier.fromCrtFile"), e.getMessage))
      verifier <- ECDSAVerifier(algorithmPublicKey(key), jwtTimestampLeeway, maxTokenLife)
    } yield verifier
}

// RSA256 validator factory
object RSA256Verifier extends Leeway with WithExecuteUnsafe {
  def apply(
      publicKey: RSAPublicKey,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      maxTokenLife: Option[Long] = None,
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {
        val algorithm = Algorithm.RSA256(publicKey, null)
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier, maxTokenLife)
      },
      Symbol("RSA256"),
    )

  def apply(keyProvider: RSAKeyProvider, maxTokenLife: Option[Long]): Either[Error, JwtVerifier] =
    executeUnsafe(
      {

        val algorithm = Algorithm.RSA256(keyProvider)
        val verifier = getVerifier(algorithm)
        new JwtVerifier(verifier, maxTokenLife)
      },
      (Symbol("RSA256")),
    )

  def apply(
      keyProvider: RSAKeyProvider,
      jwtTimestampLeeway: Option[JwtTimestampLeeway],
      maxTokenLife: Option[Long],
  ): Either[Error, JwtVerifier] =
    executeUnsafe(
      {

        val algorithm = Algorithm.RSA256(keyProvider)
        val verifier = getVerifier(algorithm, jwtTimestampLeeway)
        new JwtVerifier(verifier, maxTokenLife)
      },
      Symbol("RSA256"),
    )

  /** Create a RSA256 validator with the key loaded from the given file. The file is assumed to be a
    * X509 encoded certificate. These typically have the .crt file extension.
    */
  def fromCrtFile(
      path: String,
      jwtTimestampLeeway: Option[JwtTimestampLeeway] = None,
      maxTokenLife: Option[Long] = None,
  ): Either[Error, JwtVerifier] =
    for {
      rsaKey <- KeyUtils
        .readRSAPublicKeyFromCrt(new File(path))
        .toEither
        .left
        .map(e => Error(Symbol("RSA256Verifier.fromCrtFile"), e.getMessage))
      verifier <- RSA256Verifier.apply(rsaKey, jwtTimestampLeeway, maxTokenLife)
    } yield verifier
}
